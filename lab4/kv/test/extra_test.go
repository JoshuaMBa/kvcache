package kvtest

import (
	"fmt"
	"testing"
	"time"

	"cs426.yale.edu/lab4/kv"
	"github.com/stretchr/testify/assert"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// Like previous labs, you must write some tests on your own.
// Add your test cases in this file and submit them as extra_test.go.
// You must add at least 5 test cases, though you can add as many as you like.
//
// You can use any type of test already used in this lab: server
// tests, client tests, or integration tests.
//
// You can also write unit tests of any utility functions you have in utils.go
//
// Tests are run from an external package, so you are testing the public API
// only. You can make methods public (e.g. utils) by making them Capitalized.

func TestIntegrationClientTtl(t *testing.T) {
	setup := MakeTestSetup(MakeBasicOneShard())

	err := setup.Set("abc", "123", 500*time.Millisecond)
	assert.Nil(t, err)
	assert.Equal(t, 1, setup.clientPool.GetRequestsSent("n1"))

	_, wasFound, _ := setup.Get("abc")
	assert.True(t, wasFound)

	time.Sleep(800 * time.Millisecond)
	assert.Equal(t, 2, setup.clientPool.GetRequestsSent("n1"))

	_, wasFound, _ = setup.Get("abc")
	assert.False(t, wasFound)
	assert.Equal(t, 3, setup.clientPool.GetRequestsSent("n1"))

	setup.Shutdown()
}

func TestGetShardContentsNoSelf(t *testing.T) {
	setup := MakeTestSetup(kv.ShardMapState{
		NumShards: 2,
		Nodes:     makeNodeInfos(2),
		ShardsToNodes: map[int][]string{
			1: {"n1"},
			2: {"n2"},
		},
	})

	// doesn't call GetShardContents on self ...
	setup.UpdateShardMapping(map[int][]string{1: {"n1"}, 2: {"n1", "n2"}})
	assert.Equal(t, 0, setup.clientPool.GetRequestsSent("n1"))
	assert.Equal(t, 1, setup.clientPool.GetRequestsSent("n2"))

	setup.Shutdown()
}

// func TestGetShardContentsDistribution(t *testing.T) {
// 	const numNodes = 8
// 	const shard1Nodes = 4
// 	const shard2Nodes = 4
// 	const numRepetitions = 100

// 	// Create node names n1, n2, ..., n20
// 	nodeNames := make([]string, numNodes)
// 	for i := 0; i < numNodes; i++ {
// 		nodeNames[i] = fmt.Sprintf("n%d", i+1)
// 	}

// 	// Initial mapping: first 10 nodes have shard 1, next 10 have shard 2
// 	initialShardToNodes := map[int][]string{
// 		1: nodeNames[:shard1Nodes],
// 		2: nodeNames[shard1Nodes : shard1Nodes+shard2Nodes],
// 	}

// 	setup := MakeTestSetup(kv.ShardMapState{
// 		NumShards:     2,
// 		Nodes:         makeNodeInfos(numNodes),
// 		ShardsToNodes: initialShardToNodes,
// 	})

// 	// Running total of requests for each of the original shard 1 nodes
// 	requestCounts := make(map[string]int)
// 	for _, nodeName := range nodeNames[:shard1Nodes] {
// 		requestCounts[nodeName] = 0
// 	}

// 	// Repeat the test procedure 100 times
// 	for i := 0; i < numRepetitions; i++ {
// 		// Step 1: Update shard mapping so shard 2 nodes also have shard 1
// 		newShardToNodes := map[int][]string{
// 			1: nodeNames[:shard1Nodes+shard2Nodes],              // all nodes have shard 1
// 			2: nodeNames[shard1Nodes : shard1Nodes+shard2Nodes], // nothing changes for shard 2
// 		}
// 		setup.UpdateShardMapping(newShardToNodes)

// 		// Step 2: Count `GetRequestsSent` for each of the original shard 1 nodes
// 		for _, nodeName := range nodeNames[:shard1Nodes] {
// 			requestCounts[nodeName] += setup.clientPool.GetRequestsSent(nodeName)
// 		}

// 		// Step 3: Reset to original shard map
// 		setup.UpdateShardMapping(initialShardToNodes)

// 		// Step 4: Clear the requests for shard 1 nodes
// 		for _, nodeName := range nodeNames[:shard1Nodes] {
// 			setup.clientPool.ClearRequestsSent(nodeName)
// 		}
// 	}

// 	// Step 5: Verify even distribution across original shard 1 nodes
// 	// Calculate the average number of requests and set a tolerance
// 	averageRequests := requestCounts[nodeNames[0]] / numRepetitions
// 	tolerance := averageRequests / 10 // 10% tolerance

// 	// Step 6: Ensure no two request counts differ by more than the tolerance
// 	for i := 0; i < shard1Nodes; i++ {
// 		for j := i + 1; j < shard1Nodes; j++ {
// 			diff := abs(requestCounts[nodeNames[i]] - requestCounts[nodeNames[j]])
// 			if diff > tolerance {
// 				t.Errorf("Request count difference between nodes %s (%d) and %s (%d) is %d, which exceeds the tolerance of %d",
// 					nodeNames[i], requestCounts[nodeNames[i]], nodeNames[j], requestCounts[nodeNames[j]], diff, tolerance)
// 			}
// 		}
// 	}

// 	setup.Shutdown()
// }

func TestRetryOnGetShardContentsFailure(t *testing.T) {
	const numNodes = 5

	// Create node names n1, n2, ..., n5
	nodeNames := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeNames[i] = fmt.Sprintf("n%d", i+1)
	}

	// Initial mapping: first 4 nodes have shard 1, last node has shard 2
	initialShardToNodes := map[int][]string{
		1: nodeNames[:numNodes-1], // n1, n2, n3, n4 have shard 1
		2: nodeNames[numNodes-1:], // n5 has shard 2
	}

	setup := MakeTestSetup(kv.ShardMapState{
		NumShards:     2,
		Nodes:         makeNodeInfos(numNodes),
		ShardsToNodes: initialShardToNodes,
	})

	// Simulate failure for `GetShardContents` on the first 3 nodes for shard 1
	for i, nodeName := range nodeNames[:numNodes-1] {
		if i < numNodes-3 {
			setup.clientPool.OverrideRpcError(nodeName, status.Errorf(codes.Aborted, "set to fail"))
		}
	}

	// New mapping: replicate shard 1 across all nodes
	newShardToNodes := map[int][]string{
		1: nodeNames, // All nodes now have shard 1
		2: nodeNames[numNodes-1:],
	}

	// Call `UpdateShardMapping` to trigger `GetShardContents` retries on all nodes for shard 1
	setup.UpdateShardMapping(newShardToNodes)

	// Verify each failing node received a request
	for i, nodeName := range nodeNames {
		expectedRequests := 1
		if i < numNodes-3 {
			assert.Equal(t, expectedRequests, setup.clientPool.GetRequestsSent(nodeName), "Expected request sent to node %s", nodeName)
		}
	}
	assert.Equal(t, 0, setup.clientPool.GetRequestsSent("n4"), "Expected no request sent to node %s", "n4")

	setup.Shutdown()
}

func TestGetShardContentsInitializeEmpty(t *testing.T) {
	const numNodes = 5

	// Create node names n1, n2, ..., n5
	nodeNames := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeNames[i] = fmt.Sprintf("n%d", i+1)
	}

	// Initial mapping: shard 1 on nodes n1 to n4, shard 2 on node n5
	initialShardToNodes := map[int][]string{
		1: nodeNames[:4], // shard 1 on n1, n2, n3, n4
		2: nodeNames[4:], // shard 2 on n5
	}

	setup := MakeTestSetup(kv.ShardMapState{
		NumShards:     2,
		Nodes:         makeNodeInfos(numNodes),
		ShardsToNodes: initialShardToNodes,
	})

	// Use the Set function to add data to shard 1
	err := setup.Set("testKey", "testValue", 6000)
	if err != nil {
		t.Fatalf("Failed to set data in shard 1: %v", err)
	}

	// Simulate failure for `GetShardContents` on all nodes
	for _, nodeName := range nodeNames {
		setup.clientPool.OverrideRpcError(nodeName, status.Errorf(codes.Unavailable, "no available peer"))
	}
	setup.clientPool.ClearRpcOverrides("n5")

	// New mapping: both shard 1 and shard 2 are only on node n5
	newShardToNodes := map[int][]string{
		1: nodeNames[4:], // shard 1 only on n5
		2: nodeNames[4:], // shard 2 only on n5
	}

	// Call `UpdateShardMapping` to trigger `GetShardContents` with the new mapping
	setup.UpdateShardMapping(newShardToNodes)

	// Check that shard 1 is empty by attempting to retrieve the key
	value, found, err := setup.Get("testKey")
	if err != nil {
		t.Fatalf("Error occurred during Get operation: %v", err)
	}
	if found || value != "" {
		t.Errorf("Expected shard 1 to be empty, but found key with value: %v", value)
	}

	setup.Shutdown()
}

func TestGetShardContentsWaitComplete(t *testing.T) {
	const numNodes = 5

	// Create node names n1, n2, ..., n5
	nodeNames := make([]string, numNodes)
	for i := 0; i < numNodes; i++ {
		nodeNames[i] = fmt.Sprintf("n%d", i+1)
	}

	// Initial mapping: shard 1 on nodes n1 to n4, shard 2 on node n5
	initialShardToNodes := map[int][]string{
		1: nodeNames[:4], // shard 1 on n1, n2, n3, n4
		2: nodeNames[4:], // shard 2 on n5
	}

	setup := MakeTestSetup(kv.ShardMapState{
		NumShards:     2,
		Nodes:         makeNodeInfos(numNodes),
		ShardsToNodes: initialShardToNodes,
	})

	// Use the Set function to add data to shard 1 on nodes n1-n4
	err := setup.Set("testKey", "123", 10*time.Second)
	assert.Nil(t, err)

	// Verify that `Get` works before changing the shard mapping
	val, wasFound, err := setup.Get("testKey")
	assert.Nil(t, err)
	assert.True(t, wasFound)
	assert.Equal(t, "123", val)

	// New mapping: both shard 1 and shard 2 are only on node n5
	newShardToNodes := map[int][]string{
		1: nodeNames[4:], // shard 1 only on n5
		2: nodeNames[4:], // shard 2 only on n5
	}

	// Add latency to nodes n1 to n4 to simulate shard copy taking time
	for _, nodeName := range nodeNames[:4] {
		setup.clientPool.AddLatencyInjection(nodeName, 100*time.Millisecond)
	}

	// Call `UpdateShardMapping` to start the shard copy in progress
	setup.UpdateShardMapping(newShardToNodes)

	// Attempt to access shard 1 while the shard copy is in progress
	val, wasFound, err = setup.Get("testKey")
	assert.False(t, wasFound)
	assert.Empty(t, val)

	// Wait for the shard copy (latency) to complete
	time.Sleep(6000 * time.Millisecond)

	setup.Shutdown()
}

// Helper function to calculate absolute value of an integer
func abs(x int) int {
	if x < 0 {
		return -x
	}
	return x
}
