**A4**

I chose to have a loop that does the TTL cleanup every 5 seconds, with complexity O(N), where N is the 
number of keys.. I chose this since most TTLs will probably be done in one or two cycles. Since a TTL 
requires a set to get reset, a more write intensive server could do it whenever a get request is received. 
Another option I considered but did not opt for was spawning goroutines to clean each specific entry after 
the TTL (if it was not reset). I didn't go for this as it seemed like it might spawn too many routines in a 
scaled up server and probably wouldn't be as great.

**B2** 

I chose to implement a randomized load balancer where the node used to service the GET request is selected at 
random. One issue with randomized load balancing is that there is no awareness of node load or capacity. While
the randomized load balancing should get close to a uniform distribution of requests across the servers, it 
doesn't account for the fact that server configurations and capacities can differ, so even with a uniform 
distribution of requests, it's not necessarily true that each server is eqally equipped to handle the same
number of requests. 

A better solution would be to use some sort of weighted load balancing where we assign weights to nodes 
based on their capacity or current load, so nodes with higher capacity receive a proportionally larger 
share of requests. This requires dynamic tracking of node load or using a predefined weighting based on 
node specifications. Another alternative could be to introduce dynamic health checks during load balancing 
so that if one server is being overloaded, the load balancer could reject the randomized choice and pick 
a new server to handle the request.

**B3**

There are several issues with this approach. Firstly, sequential attempts can lead to increased latency because 
if multiple nodes are unavailable or fail to respond quickly, the client will have to wait for each failure 
before attempting the next node. The try-until-success approach also generates extra traffic in scenarios 
where all nodes are unavailable, as it attempts to reach every node before failing. This unnecessary network 
overhead can lead to slower recovery for the entire cluster, especially under heavy load. Furthermore, there is 
no scheme for how we actually iterate over the alternative nodes, so while our randomized load balancing may 
help slightly in this scenario, our approach for choosing an alternative node has no guarantees for load 
balancing in general. Specifically, this strategy doesn't distinguish between nodes with higher or lower 
recent availability. If a node has a history of being down or slow, attempting to use it before more reliable 
nodes may be inefficient, as the request will likely fail again.

In order to help in these scenarios, caching healthy/successful nodes in some way would allow us to have 
a more informed scheme for selecting alternative nodes. More generally, an adaptive retry mechanism would 
allow us to avoid sending requests to many servers. For example, if a particular node has failed multiple 
times recently, we can introduce a retry delay and increase the retry delay for that node. Another 
alternative is to proactively check node health at regular intervals instead of only on failure. By using a 
separate health-checking mechanism, the client can mark nodes as "unavailable" if they’re unresponsive or 
slow and avoid them in normal requests, improving response reliability. 

**B4** 

The main issue would be data inconsistency across nodes. If some nodes succeed in processing the SET request 
while others fail, different nodes may hold different values for the same key within the shard. This 
inconsistency can lead to unpredictable results when clients perform multiple GET operations, as they 
may retrieve different values depending on which node is used to respons to their request. There will be 
divergent reads where clients may read stale values if they query a node that didn’t receive the update. 
This divergence can lead to situations where different clients see different data, potentially breaking 
application logic that relies on the latest data being available across the cluster. One anomaly that a 
client would observe would be that they if they try to read data that they just wrote, the read may fail 
because the client could read from a node that didn't receive the update.

**D2**

sudo go run cmd/stress/tester.go --shardmap shardmaps/single-node.json --get-qps=250 --set-qps=250 --num-keys=1 -log-level=error
Stress test completed!
Get requests: 15020/15020 succeeded = 100.000000% success rate
Set requests: 15020/15020 succeeded = 100.000000% success rate
Correct responses: 1103/1103 = 100.000000%
Total requests: 30040 = 500.656509 QPS

I wanted to test how much the server can handle since shards are currently locked together, meaning that it would be very possible that it fails often. Indeed, it looks like the responses it could actually provide was incredibly low (1.1k/15k). This makes sense. Without num-keys=1, it was at 15000/15000 correct responses, which is much better, and makes sense for a more spread out load (5 shards vs everything hitting just 1).

cat shardmaps/test-3-node-start.json > shardmaps/test-3-node-file.json ; sudo go run cmd/stress/tester.go --shardmap shardmaps/test-3-node-file.json -log-level=error & (sleep 30 ; cat shardmaps/test-3-node-change.json > shardmaps/test-3-node-file.json)
Stress test completed!
Get requests: 6020/6020 succeeded = 100.000000% success rate
Set requests: 1820/1820 succeeded = 100.000000% success rate
Correct responses: 6018/6018 = 100.000000%
Total requests: 7840 = 130.659850 QPS

This test starts with the shardmap in shardmaps/test-3-node-start.json and changes to shardmaps/test-3-node-change.json after 30 seconds. I wanted to test how the servers do with the change in the mapping in the middle, and it works as expected. Here are the shardmaps (the start is the same as the given test-3-node.json, but the map it changes to is my own).

test-3-node-start.json

"shards": {
    "1": ["n1", "n3"],
    "2": ["n1"],
    "3": ["n2", "n3"],
    "4": ["n1", "n2", "n3"],
    "5": ["n2"]
}

test-3-node-change.json

"shards": {
    "1": ["n2", "n3"],
    "2": ["n1", "n2"],
    "3": ["n2", "n3"],
    "4": ["n1", "n3"],
    "5": ["n2", "n1"]
}

**Group Work**

Ayush handled implementing the server side. He did parts A (except additional tests) and C.

Josh handled implementing the client side. He did part B.

Michael handled edge casing/stress testing. He did part D and the extra tests.
