package kv

import (
	"time"
)

func hasExpired(obj *kvObject) bool {
	return time.Since(obj.stored_time).Milliseconds() >= obj.ttlMs
}

func (shard *kvShard) Shutdown() {
	shard.shutCh <- true
}

func (shard *kvShard) cleanup() {
	shard.mu.Lock()
	defer shard.mu.Unlock()
	var expired []string
	for key, obj := range shard.data {
		if hasExpired(obj) {
			expired = append(expired, key)
		}
	}
	for _, key := range expired {
		delete(shard.data, key)
	}
}