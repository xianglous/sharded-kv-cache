# sharded-kv-cache

## Group Contribution
Xianglong Li: Designed the structure for the service (RAFT at shard level). Implemented ShardNode to comply with RAFT consensus and replicate log and execute Get, Set, and Delete operations. Implemented NodePool to conduct gRPC communication between Kv and Shard.

Junyan Zhang: Implemented new Get Set and Delete for the server. Wrote execution scripts for starting shard and server. Debugged code with custom shard configurations.

## Project Structure
The project directory has three major components:

### Kv
The `kv` package defines the key-value store service. In addition to the old code from Lab4, the folder has implementation of the shard (`shardnode.go` and `shard.go`), server (`server.go`), nodepool (`nodepool.go`) and (`client.go`). 

### Raft
`raft.go` contains the implementation of RAFT consensus as described in Lab3. We did not add too many new implementation to it as we want to use it as a versatile library instead of dedicating it to this project.

### Cmd
The `cmd` package contains executable scripts to start the shard cluster (`cmd/shard/shard.go`), kv server (`cmd/server/server.go`) and make request to the service (`cmd/client/client.go`).

To start the service, first run the following command
```
go run cmd/shard/shard.go --config_dir="config/5-shard-3-node/" --num_nodes 3 --shard_id 1
```
This command start a shard cluster with shard_id 1 and 3 nodes in the Raft consensus. Its configuration file is stored in the folder config/5-shard-3-node/. You can start multiple shards using the similar command.

After setting up the shards, run the following command
```
go run cmd/server/server.go --shardmap="shardmaps/single-node-single-shard.json" --config_dir="config/5-shard-3-node" --node="n1"
```
Which start a KV node with name "n1" and having shard map from shardmaps/single-node-single-shard.json. The node will get the shard information (address, port, ids) from the shard directory config/5-shard-3-node

Then you can run the following commands to test the service
```
go run cmd/client/client.go --shardmap="shardmaps/single-node-single-shard.json" set abc 234 5000
go run cmd/client/client.go --shardmap="shardmaps/single-node-single-shard.json" get abc
go run cmd/client/client.go --shardmap="shardmaps/single-node-single-shard.json" delete abc
go run cmd/client/client.go --shardmap="shardmaps/single-node-single-shard.json" get abc
```

You can also run the same stress checker as in Lab4

## Results
We tested our implementation on single node single shard and single raft node and the same with triple raft nodes. The results are following:

Single Raft Node:
```
Stress test completed!
Get requests: 6020/6020 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 5890/6018 = 97.873048%
Total requests: 7840 = 130.663446 QPS
``` 

3 Raft Nodes:
```
Stress test completed!
Get requests: 3226/6020 succeeded = 53.588040% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 3225/3225 = 100.000000%
Total requests: 7840 = 130.659757 QPS
```

We can see that the performance is not desirable. The problem may be caused by leader being front-run by another new leader, and the new leader receives a Set operation before the old leader return the Get operation to the user. Such problem was discussed in the Raft paper. Also, election can make the service totally unavailable because no leader can reply to the user.

Another problem is not being able to run gRPC for Raft is another drawback for our implementation. We used the similar labrpc from lab3 for Raft internel communication because we did not have enough time to implement another gRPC protocol.

[Demo](https://drive.google.com/file/d/1qprDM6bBY6_jHGAGTEahu4dbIvXNUXhB/view?usp=sharing)