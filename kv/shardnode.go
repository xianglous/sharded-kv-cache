package kv

import (
	"sync"
	"time"

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

type kvShardNode struct {
	data    map[string]*kvObject
	mu      sync.RWMutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	shutCh  chan bool
}

func MakekvShardNode(peers []*labrpc.ClientEnd, me int,
	persister *raft.Persister) *kvShardNode {
	applyCh := make(chan raft.ApplyMsg)
	node := kvShardNode{
		data:    make(map[string]*kvObject),
		shutCh:  make(chan bool),
		applyCh: applyCh,
		rf:      raft.Make(peers, me, persister, applyCh)}
	go node.ttlMonitor()
	return &node
}

func (node *kvShardNode) cleanup() {
	node.mu.Lock()
	defer node.mu.Unlock()
	var expired []string
	for key, obj := range node.data {
		if HasExpired(obj) {
			expired = append(expired, key)
		}
	}
	for _, key := range expired {
		delete(node.data, key)
	}
}

func (node *kvShardNode) ttlMonitor() {
	for {
		select {
		case <-node.shutCh:
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

func (node *kvShardNode) Get(key string) (*kvObject, error) {
	node.mu.RLock()
	defer node.mu.RUnlock()
	if !node.rf.IsLeader() {
		return nil, status.Error(codes.PermissionDenied, "node is not leader")
	}
	obj, ok := node.data[key]
	if !ok || HasExpired(obj) {
		return nil, status.Error(codes.DeadlineExceeded, "key expired")
	}
	return obj, nil
}

func (node *kvShardNode) Set(key string, value string, ttlMs int64) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	_, _, isLeader := node.rf.Start(&kvCommand{
		Op:  "Set",
		Key: key,
		Obj: &kvObject{
			value:       value,
			stored_time: time.Now(),
			ttlMs:       ttlMs,
		},
	})
	if !isLeader {
		return status.Error(codes.PermissionDenied, "node is not leader")
	}
	return nil
}

func (node *kvShardNode) execute(op string, key string, obj *kvObject) {
	node.mu.Lock()
	defer node.mu.Unlock()
	if op == "Set" {
		node.data[key] = obj
	} else {
		delete(node.data, key)
	}
}

func (node *kvShardNode) Delete(key string) error {
	node.mu.Lock()
	defer node.mu.Unlock()
	_, _, isLeader := node.rf.Start(&kvCommand{
		Op:  "Delete",
		Key: key,
		Obj: nil,
	})
	if !isLeader {
		return status.Error(codes.PermissionDenied, "node is not leader")
	}
	return nil
}

func (node *kvShardNode) Data() map[string]*kvObject {
	node.mu.RLock()
	defer node.mu.RUnlock()
	return node.data
}
