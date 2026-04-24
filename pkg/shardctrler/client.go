package shardctrler

import (
	"crypto/rand"
	"math/big"
	"sync"
	"time"

	"github.com/Sithumli/atlas/pkg/raft"
)

// Clerk is a client of the configuration service.
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

// MakeClerk constructs a new shardctrler Clerk.
func MakeClerk(servers []raft.Peer) *Clerk {
	return &Clerk{servers: servers, clientID: nrand()}
}

func (c *Clerk) nextReqID() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.reqID++
	return c.reqID
}

// Query returns configuration num. -1 returns the latest.
func (c *Clerk) Query(num int) Config {
	args := &QueryArgs{Num: num}
	for {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		reply := &Reply{}
		ok := c.servers[leader].Call("ShardCtrler.Query", args, reply)
		if ok && reply.Err == OK {
			return reply.Config
		}
		c.mu.Lock()
		c.leader = (c.leader + 1) % len(c.servers)
		c.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// Join adds replica groups.
func (c *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{Servers: servers, ClientID: c.clientID, RequestID: c.nextReqID()}
	for {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		reply := &Reply{}
		ok := c.servers[leader].Call("ShardCtrler.Join", args, reply)
		if ok && reply.Err == OK {
			return
		}
		c.mu.Lock()
		c.leader = (c.leader + 1) % len(c.servers)
		c.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// Leave removes replica groups.
func (c *Clerk) Leave(gids []int) {
	args := &LeaveArgs{GIDs: gids, ClientID: c.clientID, RequestID: c.nextReqID()}
	for {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		reply := &Reply{}
		ok := c.servers[leader].Call("ShardCtrler.Leave", args, reply)
		if ok && reply.Err == OK {
			return
		}
		c.mu.Lock()
		c.leader = (c.leader + 1) % len(c.servers)
		c.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

// Move force-assigns a shard to a group.
func (c *Clerk) Move(shard, gid int) {
	args := &MoveArgs{Shard: shard, GID: gid, ClientID: c.clientID, RequestID: c.nextReqID()}
	for {
		c.mu.Lock()
		leader := c.leader
		c.mu.Unlock()
		reply := &Reply{}
		ok := c.servers[leader].Call("ShardCtrler.Move", args, reply)
		if ok && reply.Err == OK {
			return
		}
		c.mu.Lock()
		c.leader = (c.leader + 1) % len(c.servers)
		c.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}
