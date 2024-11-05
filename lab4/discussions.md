**A4**

I chose to have a loop that does the TTL cleanup every 5 seconds, with complexity O(N), where N is the number of keys.. I chose this since most TTLs will probably be done in one or two cycles. Since a TTL requires a set to get reset, a more write intensive server could do it whenever a get request is received. Another option I considered but did not opt for was spawning goroutines to clean each specific entry after the TTL (if it was not reset). I didn't go for this as it seemed like it might spawn too many routines in a scaled up server and probably wouldn't be as great.

