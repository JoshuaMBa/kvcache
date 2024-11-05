package kv

import (
	"context"
	"sync"
	"time"

	"cs426.yale.edu/lab4/kv/proto"
	"github.com/sirupsen/logrus"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type ValueEntry struct {
	value     string
	expiresAt time.Time
}

type ShardData struct {
	hosted bool
	data   map[string]*ValueEntry
	lock   sync.RWMutex
}

type KvServerImpl struct {
	proto.UnimplementedKvServer
	nodeName string

	shardMap   *ShardMap
	listener   *ShardMapListener
	clientPool ClientPool
	shutdown   chan struct{}

	numShards    int
	shards       map[int]*ShardData
	hostedShards map[int]bool
}

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:     nodeName,
		shardMap:     shardMap,
		listener:     &listener,
		clientPool:   clientPool,
		shutdown:     make(chan struct{}),
		numShards:    shardMap.NumShards(),
		shards:       make(map[int]*ShardData),
		hostedShards: make(map[int]bool),
	}

	for i := 0; i < shardMap.NumShards(); i++ {
		server.shards[i] = &ShardData{
			data:   make(map[string]*ValueEntry),
			lock:   sync.RWMutex{},
			hosted: false,
		}
	}

	go server.ttlCleanupLoop()
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	return &server
}

func contains(arr []int, val int) bool {
	for _, id := range arr {
		if id == val {
			return true
		}
	}
	return false
}

func (server *KvServerImpl) handleShardMapUpdate() {
	shardsForMe := server.shardMap.ShardsForNode(server.nodeName)

	toAdd := make(map[int]bool)
	toRemove := make(map[int]bool)

	for _, shardID := range shardsForMe {
		if !server.hostedShards[shardID-1] {
			toAdd[shardID] = true
		}
	}

	for shardID, hosted := range server.hostedShards {
		if hosted && !contains(shardsForMe, shardID+1) {
			toRemove[shardID+1] = true
		}
	}

	for shardID := range toAdd {
		err := server.copyShardData(shardID)
		if err != nil {
			logrus.WithFields(logrus.Fields{"shard": shardID}).Debug("Shard copy failed, initializing empty")
			server.shards[shardID-1].lock.Lock()
			server.shards[shardID-1].hosted = true
			server.shards[shardID-1].data = make(map[string]*ValueEntry)
			server.shards[shardID-1].lock.Unlock()
		}
		server.hostedShards[shardID-1] = true
	}

	for shardID := range toRemove {
		shard := server.shards[shardID-1]
		shard.lock.Lock()
		server.shards[shardID-1].hosted = false
		server.shards[shardID-1].data = make(map[string]*ValueEntry)
		shard.lock.Unlock()
		delete(server.hostedShards, shardID-1)
		logrus.WithFields(logrus.Fields{"shard": shardID}).Debug("Successfully removed shard")
	}
}

func (server *KvServerImpl) shardMapListenLoop() {
	listener := server.listener.UpdateChannel()
	for {
		select {
		case <-server.shutdown:
			return
		case <-listener:
			server.handleShardMapUpdate()
		}
	}
}

func (server *KvServerImpl) ttlCleanupLoop() {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-server.shutdown:
			return
		case <-ticker.C:
			server.cleanupExpiredEntries()
		}
	}
}

func (server *KvServerImpl) cleanupExpiredEntries() {
	now := time.Now()

	for _, shard := range server.shards {
		shard.lock.Lock()

		for key, entry := range shard.data {
			if entry.expiresAt.Before(now) {
				delete(shard.data, key)
			}
		}

		shard.lock.Unlock()
	}
}

func (server *KvServerImpl) Shutdown() {
	server.shutdown <- struct{}{}
	server.listener.Close()
}

func (server *KvServerImpl) Get(
	ctx context.Context,
	request *proto.GetRequest,
) (*proto.GetResponse, error) {
	if request.Key == "" {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.InvalidArgument, "Key cannot be empty")
	}

	shardID := GetShardForKey(request.Key, server.numShards)
	shard, exists := server.shards[shardID-1]

	if !exists || shard == nil || !shard.hosted {
		return &proto.GetResponse{Value: "", WasFound: false}, status.Error(codes.NotFound, "Shard not hosted")
	}

	shard.lock.RLock()
	defer shard.lock.RUnlock()

	entry, exists := shard.data[request.Key]
	if !exists || time.Now().After(entry.expiresAt) {
		return &proto.GetResponse{Value: "", WasFound: false}, nil
	}

	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("Get key found")

	return &proto.GetResponse{Value: entry.value, WasFound: true}, nil
}

func (server *KvServerImpl) Set(
	ctx context.Context,
	request *proto.SetRequest,
) (*proto.SetResponse, error) {
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key cannot be empty")
	}

	shardID := GetShardForKey(request.Key, server.numShards)
	shard, exists := server.shards[shardID-1]

	if !exists || shard == nil || !shard.hosted {
		return nil, status.Error(codes.NotFound, "Shard not hosted")
	}

	shard.lock.Lock()
	defer shard.lock.Unlock()

	expiry := time.Now().Add(time.Duration(request.TtlMs) * time.Millisecond)
	shard.data[request.Key] = &ValueEntry{
		value:     request.Value,
		expiresAt: expiry,
	}

	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key, "ttl_ms": request.TtlMs},
	).Trace("Set key with TTL")

	return &proto.SetResponse{}, nil
}

func (server *KvServerImpl) Delete(
	ctx context.Context,
	request *proto.DeleteRequest,
) (*proto.DeleteResponse, error) {
	if request.Key == "" {
		return nil, status.Error(codes.InvalidArgument, "Key cannot be empty")
	}

	shardID := GetShardForKey(request.Key, server.numShards)
	shard, exists := server.shards[shardID-1]

	if !exists || shard == nil || !shard.hosted {
		return nil, status.Error(codes.NotFound, "Shard not hosted")
	}

	shard.lock.Lock()
	defer shard.lock.Unlock()

	delete(shard.data, request.Key)

	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Trace("Delete key")

	return &proto.DeleteResponse{}, nil
}

func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	shardID := int(request.Shard)
	shard, exists := server.shards[shardID-1]

	if !exists || !shard.hosted {
		return nil, status.Error(codes.NotFound, "Shard not hosted on this server")
	}

	shard.lock.RLock()
	defer shard.lock.RUnlock()

	var values []*proto.GetShardValue
	now := time.Now()
	for key, entry := range shard.data {
		if entry.expiresAt.After(now) {
			values = append(values, &proto.GetShardValue{
				Key:            key,
				Value:          entry.value,
				TtlMsRemaining: int64(entry.expiresAt.Sub(now).Milliseconds()),
			})
		}
	}

	return &proto.GetShardContentsResponse{Values: values}, nil
}

func (server *KvServerImpl) copyShardData(shardID int) error {
	peers := server.shardMap.NodesForShard(shardID)
	for _, peer := range peers {
		if peer == server.nodeName {
			continue
		}

		client, err := server.clientPool.GetClient(peer)
		if err != nil {
			logrus.WithFields(logrus.Fields{"shard": shardID, "peer": peer}).Debug("Failed to get client")
			continue
		}

		response, err := client.GetShardContents(context.Background(), &proto.GetShardContentsRequest{Shard: int32(shardID)})
		if err != nil {
			logrus.WithFields(logrus.Fields{"shard": shardID, "peer": peer}).Debug("Failed to get shard contents")
			continue
		}

		shard := server.shards[shardID-1]
		shard.lock.Lock()
		defer shard.lock.Unlock()

		now := time.Now()
		for _, entry := range response.Values {
			shard.data[entry.Key] = &ValueEntry{
				value:     entry.Value,
				expiresAt: now.Add(time.Duration(entry.TtlMsRemaining) * time.Millisecond),
			}
		}

		shard.hosted = true
		return nil
	}

	logrus.WithFields(logrus.Fields{"shard": shardID}).Debug("Failed to copy shard data from all peers")
	return status.Error(codes.Unavailable, "Failed to copy shard data from all peers")
}
