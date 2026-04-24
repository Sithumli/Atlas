package kvraft

import (
	"crypto/rand"
	"math/big"
	"sync"

	"github.com/Sithumli/atlas/pkg/raft"
)

// Clerk is a client of a single kvraft replica group.
type Clerk struct {
	mu       sync.Mutex
	servers  []raft.Peer
	leader   int
	clientID int64
	reqID    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	x, _ := rand.Int(rand.Reader, max)
	return x.Int64()
}

// MakeClerk constructs a new Clerk.
func MakeClerk(servers []raft.Peer) *Clerk {
	return &Clerk{
		servers:  servers,
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
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		reply := &Reply{}
		ok := c.servers[leader].Call("KVServer.Get", args, reply)
		if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
			if reply.Err == ErrNoKey {
				return ""
			}
			return reply.Value
		}
		c.mu.Lock()
		c.leader = (c.leader + 1) % len(c.servers)
		c.mu.Unlock()
	}
}

// Put writes value at key.
func (c *Clerk) Put(key, value string) { c.putAppend(key, value, "Put") }

// Append atomically appends value to key.
func (c *Clerk) Append(key, value string) { c.putAppend(key, value, "Append") }

func (c *Clerk) putAppend(key, value, op string) {
	args := &PutAppendArgs{Key: key, Value: value, Op: op, ClientID: c.clientID, RequestID: c.nextReqID()}
	for {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		reply := &Reply{}
		ok := c.servers[leader].Call("KVServer.PutAppend", args, reply)
		if ok && reply.Err == OK {
			return
		}
		c.mu.Lock()
		c.leader = (c.leader + 1) % len(c.servers)
		c.mu.Unlock()
	}
}
