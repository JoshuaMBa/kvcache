package kv

import (
	"context"
	"cs426.yale.edu/lab4/kv/proto"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/sirupsen/logrus"
)

type Kv struct {
	shardMap   *ShardMap
	clientPool ClientPool
	smu        sync.Mutex
	dmu        sync.Mutex

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

func (kv *Kv) Get(ctx context.Context, key string) (string, bool, error) {
	// Trace-level logging -- you can remove or use to help debug in your tests
	// with `-log-level=trace`. See the logging section of the spec.
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Get() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)
	if len(nodes) == 0 {
		return "", false, errors.New(fmt.Sprintf("Get: no node hosts shard %v", shard))
	}

	var err error
	var response *proto.GetResponse
	start := rand.Intn(len(nodes))
	for i := 0; i < len(nodes); i++ {
		var kvClient proto.KvClient
		kvClient, err = kv.clientPool.GetClient(nodes[(start+i)%len(nodes)])
		if err != nil {
			continue
		}

		response, err = kvClient.Get(ctx, &proto.GetRequest{
			Key: key,
		})
		if err == nil {
			return response.Value, response.WasFound, nil
		}
	}

	return "", false, err
}

func (kv *Kv) Set(ctx context.Context, key string, value string, ttl time.Duration) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Set() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)
	if len(nodes) == 0 {
		return errors.New(fmt.Sprintf("Set: no node hosts shard %v", shard))
	}

	var err error
	var wg sync.WaitGroup

	for _, node := range nodes {
		kvClient, status := kv.clientPool.GetClient(node)
		if status != nil {
			err = status
			continue
		}

		wg.Add(1)
		go func() {
			_, set := kvClient.Set(ctx, &proto.SetRequest{
				Key:   key,
				Value: value,
				TtlMs: int64(ttl),
			})

			if set != nil {
				kv.smu.Lock()
				err = set
				kv.smu.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return err
}

func (kv *Kv) Delete(ctx context.Context, key string) error {
	logrus.WithFields(
		logrus.Fields{"key": key},
	).Trace("client sending Delete() request")

	shard := GetShardForKey(key, kv.shardMap.NumShards())
	nodes := kv.shardMap.NodesForShard(shard)
	if len(nodes) == 0 {
		return errors.New(fmt.Sprintf("Delete: no node hosts shard %v", shard))
	}

	var err error
	var wg sync.WaitGroup

	for _, node := range nodes {
		kvClient, status := kv.clientPool.GetClient(node)
		if status != nil {
			err = status
			continue
		}

		wg.Add(1)
		go func() {
			_, deleted := kvClient.Delete(ctx, &proto.DeleteRequest{
				Key: key,
			})

			if deleted != nil {
				kv.dmu.Lock()
				err = deleted
				kv.dmu.Unlock()
			}
			wg.Done()
		}()
	}

	wg.Wait()

	return err
}
