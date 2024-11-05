package kv

import (
	"context"
	"cs426.yale.edu/lab4/kv/proto"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool
	mu         sync.Mutex

	// Add any client-side state you want here
}

func MakeKv(shardMap *ShardMap, clientPool ClientPool) *Kv {
	kv := &Kv{
		shardMap:   shardMap,
		clientPool: clientPool,
	}
	// Add any initialization logic
	return kv
}

func (kv *Kv) getNode(shard int) (string, error) {
	nodes := kv.shardMap.NodesForShard(shard)
	if len(nodes) == 0 {
		return "", errors.New(fmt.Sprintf("no node hosts shard %v", shard))
	}

	return nodes[0], nil
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
	node, err := kv.getNode(shard)
	if err != nil {
		return "", false, err
	}

	kvClient, err := kv.clientPool.GetClient(node)
	if err != nil {
		return "", false, err
	}

	response, err := kvClient.Get(ctx, &proto.GetRequest{
		Key: key,
	})
	if err != nil {
		return "", false, err
	}

	return response.Value, response.WasFound, nil
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	panic("TODO: Part B")
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	panic("TODO: Part B")
}
