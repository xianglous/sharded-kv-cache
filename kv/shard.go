package kv

import (
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
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
