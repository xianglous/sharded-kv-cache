package kv

import (
	"sync"
	"time"

	"cs426.yale.edu/final/raft"
)

type kvCommand struct {
	Op    string
	Key   string
	Value string
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

func (shard *kvShard) Get(key string) (*kvObject, bool) {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	obj, ok := shard.data[key]
	if !ok || HasExpired(obj) {
		return nil, false
	}
	return obj, ok
}

func (shard *kvShard) Set(key string, value string, ttlMs int64) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	shard.data[key] = &kvObject{
		value:       value,
		stored_time: time.Now(),
		ttlMs:       ttlMs,
	}
}

func (shard *kvShard) Delete(key string) {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	delete(shard.data, key)
}

func (shard *kvShard) Data() map[string]*kvObject {
	shard.mu.RLock()
	defer shard.mu.RUnlock()
	return shard.data
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	stopCh  chan struct{}

	// TODO: deal with the case when there are way too many logs
	shardMap    *ShardMap
	storage     map[string]*kvObject
	ttlShutdown chan bool
	shards      map[int32]*kvShard

	persister      *raft.Persister
	lastApplyIndex int
	lastApplyTerm  int

	lockStart time.Time // debug 找出长时间 lock
	lockEnd   time.Time
	lockName  string
}
