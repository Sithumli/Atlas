package shardkv

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"github.com/Sithumli/atlas/pkg/raft"
	"github.com/Sithumli/atlas/pkg/shardctrler"
)

// Clerk is a sharded KV client.
type Clerk struct {
	mu       sync.Mutex
	mck      *shardctrler.Clerk
	makeEnd  func(addr string) raft.Peer
	config   shardctrler.Config
	leaders  map[int]int
	clientID int64
	reqID    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	x, _ := rand.Int(rand.Reader, max)
	return x.Int64()
}

// MakeClerk constructs a sharded KV client.
func MakeClerk(ctrlers []raft.Peer, makeEnd func(addr string) raft.Peer) *Clerk {
	return &Clerk{
		mck:      shardctrler.MakeClerk(ctrlers),
		makeEnd:  makeEnd,
		leaders:  make(map[int]int),
		clientID: nrand(),
	}
}

func (c *Clerk) nextReqID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqID++
	return c.reqID
}

// Get returns the value at key.
func (c *Clerk) Get(key string) string {
	args := &GetArgs{Key: key, ClientID: c.clientID, RequestID: c.nextReqID()}
	for {
		shard := shardctrler.Key2Shard(key)
		c.mu.Lock()
		gid := c.config.Shards[shard]
		servers := append([]string(nil), c.config.Groups[gid]...)
		args.ConfigNum = c.config.Num
		leader := c.leaders[gid]
		c.mu.Unlock()
		if gid != 0 && len(servers) > 0 {
			for i := 0; i < len(servers); i++ {
				idx := (leader + i) % len(servers)
				peer := c.makeEnd(servers[idx])
				reply := &Reply{}
				ok := peer.Call("ShardKV.Get", args, reply)
				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
					c.mu.Lock()
					c.leaders[gid] = idx
					c.mu.Unlock()
					if reply.Err == ErrNoKey {
						return ""
					}
					return reply.Value
				}
				if ok && reply.Err == WrongGroup {
					break
				}
			}
		}
		time.Sleep(80 * time.Millisecond)
		c.refreshConfig()
	}
}

// Put writes value at key.
func (c *Clerk) Put(key, value string) { c.putAppend(key, value, "Put") }

// Append atomically appends value at key.
func (c *Clerk) Append(key, value string) { c.putAppend(key, value, "Append") }

func (c *Clerk) putAppend(key, value, op string) {
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: c.clientID, RequestID: c.nextReqID()}
	for {
		shard := shardctrler.Key2Shard(key)
		c.mu.Lock()
		gid := c.config.Shards[shard]
		servers := append([]string(nil), c.config.Groups[gid]...)
		args.ConfigNum = c.config.Num
		leader := c.leaders[gid]
		c.mu.Unlock()
		if gid != 0 && len(servers) > 0 {
			for i := 0; i < len(servers); i++ {
				idx := (leader + i) % len(servers)
				peer := c.makeEnd(servers[idx])
				reply := &Reply{}
				ok := peer.Call("ShardKV.PutAppend", args, reply)
				if ok && reply.Err == OK {
					c.mu.Lock()
					c.leaders[gid] = idx
					c.mu.Unlock()
					return
				}
				if ok && reply.Err == WrongGroup {
					break
				}
			}
		}
		time.Sleep(80 * time.Millisecond)
		c.refreshConfig()
	}
}

func (c *Clerk) refreshConfig() {
	cfg := c.mck.Query(-1)
	c.mu.Lock()
	c.config = cfg
	c.mu.Unlock()
}
