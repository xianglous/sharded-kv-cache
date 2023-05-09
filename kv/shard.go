package kv

import (
	"context"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sync/atomic"

	"cs426.yale.edu/final/kv/proto"
	"google.golang.org/grpc/status"
)

type ShardState struct {
	Id    int32      `json:"id"`
	Nodes []NodeInfo `json:"nodes"`
}

func Read(dirname string, shardId int) (*ShardState, error) {
	filename := filepath.Join(dirname, fmt.Sprintf("shard_%d.json", shardId))
	data, err := os.ReadFile(filename)
	if err != nil {
		return nil, err
	}
	shardState := &ShardState{}
	err = json.Unmarshal(data, shardState)
	if err != nil {
		return nil, err
	}
	return shardState, nil
}

func Write(dirname string, shardState *ShardState) error {
	data, err := json.Marshal(shardState)
	if err != nil {
		return err
	}
	filename := filepath.Join(dirname, fmt.Sprintf("shard_%d.json", shardState.Id))
	err = os.WriteFile(filename, data, fs.ModePerm)
	if err != nil {
		return err
	}
	return nil
}

type kvShard struct {
	// data      map[string]*kvObject
	// mu sync.RWMutex
	// shutCh   chan bool
	nodePool NodePool
	leader   int64
}

func MakeKvShard(nodesInfo []NodeInfo) *kvShard {
	nodePool := MakeNodePool(nodesInfo)
	shard := kvShard{
		// data:      make(map[string]*kvObject),
		// shutCh:   make(chan bool),
		nodePool: &nodePool,
		leader:   0}
	// go shard.monitor()
	return &shard
}

func (shard *kvShard) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (res *proto.GetResponse, err error) {
	leader := atomic.LoadInt64(&shard.leader)
	for i := 0; i < shard.nodePool.Size(); i++ {
		idx := (leader + int64(i)) % int64(shard.nodePool.Size())
		client, err := shard.nodePool.GetClient(int(idx))
		if err == nil {
			res, err = client.Get(ctx, request)
			if err == nil {
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return res, nil
			} else if e, ok := status.FromError(err); ok && e.Code() != NotLeader { // is leader
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return &proto.GetResponse{}, err
			}
		}
	}
	return &proto.GetResponse{}, err
}

func (shard *kvShard) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (res *proto.SetResponse, err error) {
	leader := atomic.LoadInt64(&shard.leader)
	for i := 0; i < shard.nodePool.Size(); i++ {
		idx := (leader + int64(i)) % int64(shard.nodePool.Size())
		client, err := shard.nodePool.GetClient(int(idx))
		if err == nil {
			res, err = client.Set(ctx, request)
			if err == nil {
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				// logrus.Println("Leader: ", leader)
				return res, nil
			} else if e, ok := status.FromError(err); ok && e.Code() != NotLeader { // is leader
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return &proto.SetResponse{}, err
			}
		}
	}
	return &proto.SetResponse{}, err
}

func (shard *kvShard) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (res *proto.DeleteResponse, err error) {
	leader := atomic.LoadInt64(&shard.leader)
	for i := 0; i < shard.nodePool.Size(); i++ {
		idx := (leader + int64(i)) % int64(shard.nodePool.Size())
		client, err := shard.nodePool.GetClient(int(idx))
		if err == nil {
			res, err = client.Delete(ctx, request)
			if err == nil {
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return res, nil
			} else if e, ok := status.FromError(err); ok && e.Code() != NotLeader { // is leader
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return &proto.DeleteResponse{}, err
			}
		}
	}
	return &proto.DeleteResponse{}, err
}

func (shard *kvShard) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (res *proto.GetShardContentsResponse, err error) {
	leader := atomic.LoadInt64(&shard.leader)
	for i := 0; i < shard.nodePool.Size(); i++ {
		idx := (leader + int64(i)) % int64(shard.nodePool.Size())
		client, err := shard.nodePool.GetClient(int(idx))
		if err == nil {
			res, err = client.GetShardContents(ctx, request)
			if err == nil {
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return res, nil
			} else if e, ok := status.FromError(err); ok && e.Code() != NotLeader { // is leader
				atomic.CompareAndSwapInt64(&shard.leader, leader, idx)
				return &proto.GetShardContentsResponse{}, err
			}
		}
	}
	return &proto.GetShardContentsResponse{}, err
}
