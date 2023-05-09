package kv

import (
	"fmt"
	"sync"

	"github.com/sirupsen/logrus"

	"cs426.yale.edu/final/kv/proto"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

/*
 * NodePool is the main interface for server nodes to conduct gRPC with raft nodes
 */
type NodePool interface {
	/*
	 * Returns a KvClient for a given node if one can be created. Returns (nil, err)
	 * otherwise. Errors are not cached, so subsequent calls may return a valid KvClient.
	 */
	GetClient(nodeId int) (proto.KvClient, error)
	Size() int
}

type GrpcNodePool struct {
	mutex   sync.RWMutex
	nodes   []NodeInfo
	clients map[int]proto.KvClient
}

func MakeNodePool(nodes []NodeInfo) GrpcNodePool {
	return GrpcNodePool{nodes: nodes, clients: make(map[int]proto.KvClient)}
}

func (pool *GrpcNodePool) GetClient(nodeId int) (proto.KvClient, error) {
	// Optimistic read -- most cases we will have already cached the client, so
	// only take a read lock to maximize concurrency here
	if nodeId < 0 || nodeId >= len(pool.nodes) {
		return nil, status.Error(codes.InvalidArgument, "Invalid node id")
	}
	pool.mutex.RLock()
	client, ok := pool.clients[nodeId]
	pool.mutex.RUnlock()
	if ok {
		return client, nil
	}

	pool.mutex.Lock()
	defer pool.mutex.Unlock()
	// We may have lost a race and someone already created a client, try again
	// while holding the exclusive lock
	client, ok = pool.clients[nodeId]
	if ok {
		return client, nil
	}

	nodeInfo := pool.nodes[nodeId]

	// Otherwise create the client: gRPC expects an address of the form "ip:port"
	address := fmt.Sprintf("%s:%d", nodeInfo.Address, nodeInfo.Port)
	client, err := MakeConnection(address)
	if err != nil {
		logrus.WithField("node", nodeId).Debugf("failed to connect to shard node %s (%s): %q", nodeId, address, err)
		return nil, err
	}
	pool.clients[nodeId] = client
	return client, nil
}

func (pool *GrpcNodePool) Size() int {
	return len(pool.nodes)
}
