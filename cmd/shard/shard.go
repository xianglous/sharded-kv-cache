package main

import (
	crand "crypto/rand"
	"encoding/base64"
	"flag"
	"fmt"
	"net"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/sirupsen/logrus"

	"cs426.yale.edu/final/kv"
	"cs426.yale.edu/final/kv/proto"
	"cs426.yale.edu/final/labrpc"
	"cs426.yale.edu/final/logging"
	"cs426.yale.edu/final/raft"
	"google.golang.org/grpc"
)

// Main entry-point for actually running your KV implementation as a single node.
//
// Takes in the shard map as a JSON file (see shardmaps/ for examples), and a nodeName.
//
// Listens on the port given for the node in the shardmap file automatically.
// You may want to start multiple nodes concurrently on your machine using different
// ports to test the cluster functionality. See scripts/run-cluster.sh for running
// a bunch of "nodes" as separate processes locally on your machine.

var (
	shardId  = flag.Int("shard_id", -1, "ShardID")
	numNodes = flag.Int("num_nodes", 1, "Number of nodes in the cluster")
	// port      = flag.Int("port", 9000, "Port for the node")
	configDir = flag.String("config_dir", "", "Config directory for shard Raft cluster")
)

// type Config struct {
// 	kv.ShardStates []kv.ShardState `json:"shards"`
// }

// type ShardNodeOptions struct {
// 	NodeAddr string
// }

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func startRaft(shardState *kv.ShardState, dirname string) {
	persister := raft.MakePersister()
	endnames := make([][]string, len(shardState.Nodes))
	network := labrpc.MakeNetwork()
	var wg1 sync.WaitGroup
	var wg2 sync.WaitGroup
	fmt.Println(len(shardState.Nodes))
	wg1.Add(len(shardState.Nodes))
	for i := 0; i < len(shardState.Nodes); i++ {
		go func(i int) {
			server := grpc.NewServer()
			lis, err := net.Listen("tcp", fmt.Sprintf(":%d", shardState.Nodes[i].Port))
			if err != nil {
				logrus.Fatalf("Node: %d failed to listen: %v\nRetry another port...", err)
				lis, err = net.Listen("tcp", fmt.Sprintf(":%d", 0))
				if err != nil {
					logrus.Fatalf("Node: %d failed to listen: %v", err)
					wg1.Done()
					return
				}
			}
			shardState.Nodes[i].Port = int32(lis.Addr().(*net.TCPAddr).Port)
			endnames[i] = make([]string, len(shardState.Nodes))
			for j := 0; j < len(shardState.Nodes); j++ {
				endnames[i][j] = randstring(20)
			}
			ends := make([]*labrpc.ClientEnd, len(shardState.Nodes))
			for j := 0; j < len(shardState.Nodes); j++ {
				ends[j] = network.MakeEnd(endnames[i][j])
				network.Connect(endnames[i][j], j)
			}
			nodePool := kv.MakeNodePool(shardState.Nodes)
			node := kv.MakeKvShardNode(&nodePool, ends, i, persister.Copy())

			proto.RegisterKvServer(
				server,
				node,
			)

			svc := labrpc.MakeService(node.GetRaft())
			srv := labrpc.MakeServer()
			srv.AddService(svc)
			network.AddServer(i, srv)
			wg2.Add(1)
			wg1.Done()
			wg1.Wait()

			for j := 0; j < len(shardState.Nodes); j++ {
				network.Enable(endnames[i][j], true)
				network.Enable(endnames[j][i], true)
			}
			logrus.Printf("Node: %d listening at Port: %d", i, shardState.Nodes[i].Port)
			wg2.Done()
			if err := server.Serve(lis); err != nil {
				logrus.Fatalf("failed to serve: %v", err)
			}
		}(i)
	}
	wg1.Wait()
	wg2.Wait()
	err := kv.Write(dirname, shardState)
	if err != nil {
		logrus.Fatalf("Cannot write to directory %s, %v", dirname, err)
	}
	logrus.Printf("Finished writing to file %s", filepath.Join(dirname, fmt.Sprintf("shard_%d.json", shardState.Id)))
	for true {
		time.Sleep(1 * time.Second)
	}
}

func main() {
	flag.Parse()
	logging.InitLogging()

	if *shardId < 0 {
		logrus.Fatal("--shard_id must be a positive integer")
	}

	if *numNodes == 0 || *numNodes%2 == 0 {
		logrus.Fatal("--num_nodes must be a positive odd number")
	}

	// if *port < 0 || *port > 65536 {
	// 	logrus.Fatal("--port invalid")
	// }

	var shardState *kv.ShardState
	var err error
	if _, err = os.Stat(*configDir); len(*configDir) == 0 || os.IsNotExist(err) {
		logrus.Fatal("--config_dir must be a valid directory")
	}
	if shardState, err = kv.Read(*configDir, *shardId); err != nil || len(shardState.Nodes) != *numNodes {
		shardState = &kv.ShardState{
			Id:    int32(*shardId),
			Nodes: make([]kv.NodeInfo, 0),
		}
		fmt.Println(*shardId)
		for i := 0; i < *numNodes; i++ {
			shardState.Nodes = append(shardState.Nodes, kv.NodeInfo{
				Address: "127.0.0.1",
				Port:    0,
			})
		}
	}
	startRaft(shardState, *configDir)
}
