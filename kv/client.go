package kv

import (
	"context"
	"math/rand"
	"sync"
	"time"

	kvpb "cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool

	// Add any client-side state you want here
	mu sync.Mutex
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) get(ctx context.Context, key string, node string) (string, bool, error) {
	// defer atomic.AddUint32(&kv.LBCounter, 1)
	kvClient, err := kv.clientPool.GetClient(node)
	if err != nil {
		return "", false, err
	}
	res, err := kvClient.Get(ctx, &kvpb.GetRequest{Key: key})
	if err != nil {
		return "", false, err
	}
	return res.GetValue(), res.GetWasFound(), nil
}

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)
	if len(nodes) == 0 {
		return "", false, status.Error(codes.NotFound, "no node found")
	}
	var err error
	index := rand.Intn(len(nodes))
	for i := 0; i < len(nodes); i++ {
		value, wasFound, e := kv.get(ctx, key, nodes[(index+i)%len(nodes)])
		if e == nil {
			return value, wasFound, nil
		}
		err = e
	}
	return "", false, err
}

func (kv *Kv) set(ctx context.Context, key string, value string, ttl time.Duration, node string) error {
	kvClient, err := kv.clientPool.GetClient(node)
	if err != nil {
		return err
	}
	_, err = kvClient.Set(ctx, &kvpb.SetRequest{Key: key, Value: value, TtlMs: ttl.Milliseconds()})
	return err
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "no node found")
	}

	err_ch := make(chan error, len(nodes))

	for i := 0; i < len(nodes); i++ {
		go func(node string) {
			err_ch <- kv.set(ctx, key, value, ttl, node)
		}(nodes[i])
	}

	var err error
	for i := 0; i < len(nodes); i++ {
		if e := <-err_ch; e != nil {
			err = e
		}
	}
	close(err_ch)
	return err
}

func (kv *Kv) delete(ctx context.Context, key string, node string) error {
	kvClient, err := kv.clientPool.GetClient(node)
	if err != nil {
		return err
	}
	_, err = kvClient.Delete(ctx, &kvpb.DeleteRequest{Key: key})
	return err
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)

	if len(nodes) == 0 {
		return status.Error(codes.NotFound, "no node found")
	}

	err_ch := make(chan error, len(nodes))

	for i := 0; i < len(nodes); i++ {
		go func(node string) {
			err_ch <- kv.delete(ctx, key, node)
		}(nodes[i])
	}

	var err error
	for i := 0; i < len(nodes); i++ {
		if e := <-err_ch; e != nil {
			err = e
		}
	}
	close(err_ch)
	return err
}
