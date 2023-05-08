package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/final/kv/proto"
	"cs426.yale.edu/final/labrpc"
	"cs426.yale.edu/final/raft"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvCommand struct {
	Op  string
	Key string
	Obj *kvObject
}

type kvObject struct {
	value       string
	stored_time time.Time
	ttlMs       int64
}

type KvShardNode struct {
	data    map[string]*kvObject
	mu      sync.RWMutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	shutCh  chan bool
}

func MakeKvShardNode(peers []*labrpc.ClientEnd, me int,
	persister *raft.Persister) *KvShardNode {
	applyCh := make(chan raft.ApplyMsg, 1000)
	node := KvShardNode{
		data:    make(map[string]*kvObject),
		shutCh:  make(chan bool),
		applyCh: applyCh,
		rf:      raft.Make(peers, me, persister, applyCh)}
	go node.ttlMonitor()
	return &node
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
			if msg.CommandValid {
				node.execute(msg.Command.Op, msg.Command.Key, msg.Command.Obj)
			}
		case <-time.After(time.Second * 1):
			node.cleanup()
		}
	}
}

func hasExpired(obj *kvObject) bool {
	return time.Since(obj.stored_time).Milliseconds() >= obj.ttlMs
}

func (node *KvShardNode) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	node.mu.RLock()
	defer node.mu.RUnlock()

	if !node.rf.IsLeader() {
		return nil, status.Error(codes.PermissionDenied, "node is not leader")
	}

	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	obj, ok := node.data[request.Key]
	if !ok {
		return &proto.GetResponse{
			Value:    "",
			WasFound: false,
		}, nil
	} else if hasExpired(obj) {
		return nil, status.Error(codes.DeadlineExceeded, "key expired")
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
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	_, _, isLeader := node.rf.Start(&kvCommand{
		Op:  "Set",
		Key: request.Key,
		Obj: &kvObject{
			value:       request.Value,
			stored_time: time.Now(),
			ttlMs:       request.TtlMs,
		},
	})
	if !isLeader {
		return nil, status.Error(codes.PermissionDenied, "node is not leader")
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
		return nil, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	_, _, isLeader := node.rf.Start(&kvCommand{
		Op:  "Delete",
		Key: key,
		Obj: nil,
	})
	if !isLeader {
		return nil, status.Error(codes.PermissionDenied, "node is not leader")
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
		return nil, status.Error(codes.PermissionDenied, "node is not leader")
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

func (node *KvShardNode) execute(op string, key string, obj *kvObject) {
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

func (node *KvShardNode) IsLeader() {
	return node.rf.IsLeader()
}
