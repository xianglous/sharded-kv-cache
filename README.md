# sharded-kv-cache

## TODO (option1): 
- [ ] Decide on new Get, Set, Delete grpc APIs. 
- [ ] Write new shardkv server, which is a wrapper of lab3 raft server.
- [ ] The manipulations of shards are done through rf.applyLogs() ? 
- [ ] how to deal with TTLs when a server goes down and come back up after ttl is expired =>  self-clean? 
- [ ] how to implement "copy data for a live shard to another node"? Communication between nodes should be lab3 RPC, not grpc. 
- [ ] worry about shardmaplistener later

## TODO (option2):
- [ ] Write new shardkv server, which is a wrapper of lab3 raft server.
- [ ] Write new shardkv client, which talks with shardkv server in lab3 rpc. 
- [ ] Extend delete, ttl, shardmaplistener, clientpool??? 