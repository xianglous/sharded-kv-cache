package kv

import (
	"sync"
	"cs426.yale.edu/final/raft"
	"time"
	"log"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type kvObject struct {
	value       string
	stored_time time.Time
	ttlMs       int64
}

type kvShard struct {
	data   map[string]*kvObject
	mu     sync.RWMutex
	shutCh chan bool
}

func MakeKvShard() *kvShard {
	shard := kvShard{data: make(map[string]*kvObject), shutCh: make(chan bool)}
	go shard.ttlMonitor()
	return &shard
}


func (shard *kvShard) ttlMonitor() {
	for {
		select {
		case <-shard.shutCh:
			return
		case <-time.After(time.Second * 1):
			shard.cleanup()
		}
	}
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	// TODO: deal with the case when there are way too many logs
	shardMap   *ShardMap
	storage     map[string]*kvObject
	ttlShutdown chan bool
	shards map[int32]*kvShard

	persister      *raft.Persister
	lastApplyIndex int
	lastApplyTerm  int

	lockStart time.Time // debug 找出长时间 lock
	lockEnd   time.Time
	lockName  string
}

