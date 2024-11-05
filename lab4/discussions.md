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



