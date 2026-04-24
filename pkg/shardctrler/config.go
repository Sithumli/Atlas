package shardctrler

// NShards is the fixed number of logical shards the keyspace is partitioned
// into. A value of 10 is a sensible default for development; production
// deployments typically use 64--256.
const NShards = 10

// Config is an authoritative assignment of shards to replica groups at a
// point in time. Configurations are totally ordered by Num.
type Config struct {
	Num    int                // configuration number; monotonically increasing
	Shards [NShards]int       // Shards[i] = group id owning shard i; 0 = unassigned
	Groups map[int][]string   // group id -> list of server addresses
}

// Key2Shard maps a key to its shard id using a deterministic hash.
// This must match the hash used by all clients and servers in the cluster.
func Key2Shard(key string) int {
	var h uint32
	for i := 0; i < len(key); i++ {
		h = h*31 + uint32(key[i])
	}
	return int(h) % NShards
}
