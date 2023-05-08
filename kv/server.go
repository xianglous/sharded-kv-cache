package kv

import (
	"context"
	"log"
	"math/rand"
	"sync"
	"time"

	"cs426.yale.edu/final/kv/proto"
	"cs426.yale.edu/final/labrpc"
	"cs426.yale.edu/final/raft"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type kvShard struct {
	// data      map[string]*kvObject
	mu        sync.RWMutex
	shutCh    chan bool
	persister raft.Persister
	nodes     []*KvShardNode
	leader    int
}

func MakeKvShard(clusterSize int) *kvShard {
	shard := kvShard{
		// data:      make(map[string]*kvObject),
		shutCh:    make(chan bool),
		persister: nil,
		nodes:     make([]*KvShardNode, 0),
		leader:    0}
	ends := make([]*labrpc.ClientEnd, clusterSize)
	for i := 0; i < clusterSize; i++ {
		if shard.persister == nil {
			shard.persister = raft.MakePersister()
		} else {
			shard.persister = shard.persister.copy()
		}
		shard.nodes = append(shard.nodes, MakeKvShardNode(ends, i, shard.persister))
	}
	go shard.monitor()
	return &shard
}

func (shard *kvShard) updateLeader() {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	index := rand.Intn(len(shard.nodes))
	for i := 0; i < len(shard.nodes); i++ {
		idx := (i + index) % len(shard.nodes)
		if shard.nodes[idx].IsLeader() {
			shard.leader = idx
			return
		}
	}
	shard.leader = index
	return
}

func (shard *kvShard) monitor() {
	for {
		select {
		case <-shard.shutCh:
			for node := range shard.nodes {
				node.Kill()
			}
			return
		case <-time.After(time.Second * 10):
			shard.updateLeader()
		}
	}
}

func (shard *kvShard) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	res, err := shard.nodes[shard.leader].Get(ctx, request)
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.PermissionDenied: // not leader
			shard.updateLeader()
			return shard.nodes[shard.leader].Get(ctx, request)
		}
	}
	return nil, err
}

func (shard *kvShard) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	res, err := shard.nodes[shard.leader].Set(ctx, request)
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.PermissionDenied: // not leader
			shard.updateLeader()
			return shard.nodes[shard.leader].Set(ctx, request)
		}
	}
	return nil, err
}

func (shard *kvShard) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	res, err := shard.nodes[shard.leader].Delete(ctx, request)
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.PermissionDenied: // not leader
			shard.updateLeader()
			return shard.nodes[shard.leader].Delete(ctx, request)
		}
	}
	return nil, err
}

func (shard *kvShard) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	res, err := shard.nodes[shard.leader].GetShardContents(ctx, request)
	if e, ok := status.FromError(err); ok {
		switch e.Code() {
		case codes.PermissionDenied: // not leader
			shard.updateLeader()
			return shard.nodes[shard.leader].GetShardContents(ctx, request)
		}
	}
	return nil, err
}

func (shard *kvShard) Shutdown() {
	shard.shutCh <- true
}

type KvServerImpl struct {
	mu       sync.RWMutex
	stopCh   chan struct{}
	nodeName string

	// TODO: deal with the case when there are way too many logs
	clientPool ClientPool
	shardMap   *ShardMap
	listener   *ShardMapListener
	shards     map[int32]*kvShard
	shutdown   chan struct{}
}

func (server *KvServerImpl) isShardHosted(shard int) bool {
	_, exists := server.shards[int32(shard)]
	return exists
}

func (server *KvServerImpl) fetchShards() map[int32]bool {
	shards := make(map[int32]bool)
	for _, shard := range server.shardMap.ShardsForNode(server.nodeName) {
		shards[int32(shard)] = true
	}
	return shards
}

func (server *KvServerImpl) fetchNodeShardContent(node string, shard int32) (*proto.GetShardContentsResponse, error) {
	client, err := server.clientPool.GetClient(node)
	if err != nil {
		return nil, err
	}
	res, err := client.GetShardContents(context.Background(), &proto.GetShardContentsRequest{
		Shard: shard,
	})
	if err != nil {
		return nil, err
	}
	return res, nil
}

func (server *KvServerImpl) fetchShardContent(nodes []string, shard int32) (*proto.GetShardContentsResponse, error) {
	var err error = nil
	index := rand.Intn(len(nodes))
	for i := 0; i < len(nodes); i++ {
		node := nodes[(index+i)%len(nodes)]
		if node == server.nodeName {
			continue
		}
		res, e := server.fetchNodeShardContent(node, shard)
		if e == nil {
			return res, nil
		}
		err = e
	}
	log.Println("shard not available")
	return &proto.GetShardContentsResponse{}, err
}

func (server *KvServerImpl) handleShardMapUpdate() {
	server.mu.Lock()
	defer server.mu.Unlock()

	shards := server.fetchShards()
	added := make(map[int32]bool)   // new shards
	removed := make(map[int32]bool) // shards to be removed

	for shard := range shards {
		if _, exists := server.shards[shard]; !exists {
			added[shard] = true
		}
	}

	for shard := range server.shards {
		if _, exists := shards[shard]; !exists {
			removed[shard] = true
		}
	}

	for shard := range removed {
		server.shards[shard].Shutdown()
		delete(server.shards, shard)
	}

	if len(added) == 0 {
		return
	}
	resCh := make(chan *proto.GetShardContentsResponse, len(added))
	for shard := range added {
		server.shards[shard] = MakeKvShard()
		go func(shard int32, nodes []string) {
			res, _ := server.fetchShardContent(nodes, shard)
			resCh <- res
		}(shard, server.shardMap.NodesForShard(int(shard)))
	}
	server.mu.Unlock()
	results := make([]*proto.GetShardContentsResponse, 0)
	for i := 0; i < len(added); i++ {
		results = append(results, <-resCh)
	}
	close(resCh)
	server.mu.Lock()
	for _, res := range results {
		for _, shardVal := range res.Values {
			shard := GetShardForKey(shardVal.Key, server.shardMap.NumShards())
			server.shards[int32(shard)].Set(shardVal.Key, shardVal.Value, shardVal.TtlMsRemaining)
		}
	}
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		shards:     make(map[int32]*kvShard),
	}
	// server.shards = server.fetchShards()
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	// go server.monitor()
	return &server
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.listener.Close()
	for _, shard := range server.shards {
		shard.Shutdown()
	}
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	// Trace-level logging for node receiving this request (enable by running with -log-level=trace),
	// feel free to use Trace() or Debug() logging in your code to help debug tests later without
	// cluttering logs by default. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Get() request")

	server.mu.RLock()
	defer server.mu.RUnlock()

	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	if !server.isShardHosted(shard) {
		return &proto.GetResponse{}, status.Error(codes.NotFound, "shard not hosted")
	}

	if request.Key == "" {
		return &proto.GetResponse{}, status.Error(codes.InvalidArgument, "key cannot be empty")
	}
	return server.shards[int32(shard)].Get(ctx, request)
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Set() request")

	server.mu.Lock()
	defer server.mu.Unlock()
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	if !server.isShardHosted(shard) {
		return &proto.SetResponse{}, status.Error(codes.NotFound, "shard not hosted")
	}

	if request.Key == "" {
		return &proto.SetResponse{}, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	return server.shards[int32(shard)].Set(ctx, request)
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("node received Delete() request")

	server.mu.Lock()
	defer server.mu.Unlock()
	shard := GetShardForKey(request.Key, server.shardMap.NumShards())
	if !server.isShardHosted(shard) {
		return &proto.DeleteResponse{}, status.Error(codes.NotFound, "shard not hosted")
	}

	if request.Key == "" {
		return &proto.DeleteResponse{}, status.Error(codes.InvalidArgument, "key cannot be empty")
	}

	return server.shards[int32(shard)].Delete(ctx, request)
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	server.mu.RLock()
	defer server.mu.RUnlock()
	if !server.isShardHosted(int(request.Shard)) {
		return &proto.GetShardContentsResponse{}, status.Error(codes.NotFound, "shard not hosted")
	}
	return server.shards[int32(request.Shard)].GetShardContents(ctx, request)
}
