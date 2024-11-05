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

	numShards  int
	shards     map[int]*ShardData
}

func (server *KvServerImpl) handleShardMapUpdate() {
	// TODO: Part C
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

func MakeKvServer(nodeName string, shardMap *ShardMap, clientPool ClientPool) *KvServerImpl {
	listener := shardMap.MakeListener()
	server := KvServerImpl{
		nodeName:   nodeName,
		shardMap:   shardMap,
		listener:   &listener,
		clientPool: clientPool,
		shutdown:   make(chan struct{}),
		numShards:  shardMap.NumShards(),
		shards:     make(map[int]*ShardData),
	}

	for i := 0; i < shardMap.NumShards(); i++ {
		server.shards[i] = &ShardData{
			data: make(map[string]*ValueEntry),
			lock: sync.RWMutex{},
		}
	}

	shardsForMe := shardMap.ShardsForNode(nodeName)
	for i := 0; i < len(shardsForMe); i++ {
		server.shards[shardsForMe[i] - 1].hosted = true
	}

	go server.ttlCleanupLoop()
	go server.shardMapListenLoop()
	server.handleShardMapUpdate()
	return &server
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
	shard := server.shards[shardID-1]

	if !shard.hosted {
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
	shard := server.shards[shardID-1]

	if !shard.hosted {
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
	).Debug("Set key with TTL")

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
	shard := server.shards[shardID-1]

	if !shard.hosted {
		return nil, status.Error(codes.NotFound, "Shard not hosted")
	}

	shard.lock.Lock()
	defer shard.lock.Unlock()

	delete(shard.data, request.Key)

	logrus.WithFields(
		logrus.Fields{"node": server.nodeName, "key": request.Key},
	).Debug("Delete key")

	return &proto.DeleteResponse{}, nil
}


func (server *KvServerImpl) GetShardContents(
	ctx context.Context,
	request *proto.GetShardContentsRequest,
) (*proto.GetShardContentsResponse, error) {
	panic("TODO: Part C")
}
