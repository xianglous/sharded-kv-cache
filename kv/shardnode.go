package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/final/kv/proto"
	"cs426.yale.edu/final/labrpc"
	"cs426.yale.edu/final/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

const NotLeader codes.Code = 105

type KvCommand struct {
	Op  string
	Key string
	Obj *KvObject
}

type KvObject struct {
	value       string
	stored_time time.Time
	ttlMs       int64
}

type KvShardNode struct {
	proto.UnimplementedKvServer
	data    map[string]*KvObject
	mu      sync.RWMutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	shutCh  chan bool
}

func MakeKvShardNode(nodePool NodePool, peers []*labrpc.ClientEnd, me int,
	persister *raft.Persister) *KvShardNode {
	applyCh := make(chan raft.ApplyMsg, 1000)
	node := KvShardNode{
		data:    make(map[string]*KvObject),
		shutCh:  make(chan bool),
		applyCh: applyCh,
		rf:      raft.Make(peers, me, persister, applyCh)}
	go node.ttlMonitor()
	return &node
}

func (node *KvShardNode) GetRaft() *raft.Raft {
	return node.rf
}

func (node *KvShardNode) cleanup() {
	node.mu.Lock()
	defer node.mu.Unlock()
	var expired []string
	for key, obj := range node.data {
		if hasExpired(obj) {
			expired = append(expired, key)
		}
	}
	for _, key := range expired {
		delete(node.data, key)
	}
}

func (node *KvShardNode) ttlMonitor() {
	for {
		select {
		case <-node.shutCh:
			node.rf.Kill()
			return
		case msg := <-node.applyCh:
			logrus.Println(msg)
			if msg.CommandValid {
				cmd, ok := msg.Command.(KvCommand)
				// logrus.Println(cmd.Op)
				if ok {
					node.execute(cmd.Op, cmd.Key, cmd.Obj)
				}
			}
		case <-time.After(time.Second * 1):
			node.cleanup()
		}
	}
}

func hasExpired(obj *KvObject) bool {
	return time.Since(obj.stored_time).Milliseconds() >= obj.ttlMs
}

func (node *KvShardNode) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	node.mu.RLock()
	defer node.mu.RUnlock()

	if !node.rf.IsLeader() {
		return &proto.GetResponse{}, status.Error(NotLeader, "node is not leader")
	}

	if request.Key == "" {
		return &proto.GetResponse{}, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	obj, ok := node.data[request.Key]
	if !ok {
		return &proto.GetResponse{
			Value:    "",
			WasFound: false,
		}, nil
	} else if hasExpired(obj) {
		return &proto.GetResponse{}, status.Error(codes.DeadlineExceeded, "key expired")
	}
	return &proto.GetResponse{
		Value:    obj.value,
		WasFound: true,
	}, nil
}

func (node *KvShardNode) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if request.Key == "" {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "key cannot be empty")
	}
	// logrus.Println(request.Key, request.Value)
	_, _, isLeader := node.rf.Start(&KvCommand{
		Op:  "Set",
		Key: request.Key,
		Obj: &KvObject{
			value:       request.Value,
			stored_time: time.Now(),
			ttlMs:       request.TtlMs,
		},
	})
	if !isLeader {
		return &proto.SetResponse{}, status.Error(NotLeader, "node is not leader")
	}
	return &proto.SetResponse{}, nil
}

func (node *KvShardNode) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if request.Key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	_, _, isLeader := node.rf.Start(&KvCommand{
		Op:  "Delete",
		Key: request.Key,
		Obj: nil,
	})
	if !isLeader {
		return &proto.DeleteResponse{}, status.Error(NotLeader, "node is not leader")
	}
	return &proto.DeleteResponse{}, nil
}

func (node *KvShardNode) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if !node.rf.IsLeader() {
		return &proto.GetShardContentsResponse{}, status.Error(NotLeader, "node is not leader")
	}

	res := &proto.GetShardContentsResponse{
		Values: make([]*proto.GetShardValue, 0),
	}

	for key, obj := range node.data {
		if !hasExpired(obj) {
			res.Values = append(res.Values, &proto.GetShardValue{
				Key:            key,
				Value:          obj.value,
				TtlMsRemaining: obj.ttlMs - time.Since(obj.stored_time).Milliseconds(),
			})
		}
	}
	return res, nil
}

func (node *KvShardNode) execute(op string, key string, obj *KvObject) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if op == "Set" {
		node.data[key] = obj
	} else {
		delete(node.data, key)
	}
}

func (node *KvShardNode) Kill() {
	node.shutCh <- true
}

func (node *KvShardNode) IsLeader() bool {
	return node.rf.IsLeader()
}
