package shardctrler

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Sithumli/atlas/internal/storage"
	"github.com/Sithumli/atlas/pkg/raft"
	"github.com/Sithumli/atlas/pkg/rpc"
)

type rpcAdapter struct{ end *rpc.LabClientEnd }

func (r *rpcAdapter) Call(method string, args any, reply any) bool {
	return r.end.Call(method, args, reply)
}

var nameSeq int64
var nameMu sync.Mutex

func name() string {
	nameMu.Lock()
	nameSeq++
	v := nameSeq
	nameMu.Unlock()
	return "sc-" + strconv.FormatInt(v, 36) + "-" + time.Now().Format("150405.000000000")
}

type scCluster struct {
	t        *testing.T
	n        int
	servers  []*ShardCtrler
	persists []*storage.InMemoryPersister
	endNames [][]string
	srv      []*rpc.LabServer
	net      *rpc.LabNetwork
}

func newSCCluster(t *testing.T, n int) *scCluster {
	c := &scCluster{
		t:        t,
		n:        n,
		servers:  make([]*ShardCtrler, n),
		persists: make([]*storage.InMemoryPersister, n),
		endNames: make([][]string, n),
		srv:      make([]*rpc.LabServer, n),
		net:      rpc.NewLabNetwork(),
	}
	for i := 0; i < n; i++ {
		c.persists[i] = storage.NewInMemoryPersister()
		c.endNames[i] = make([]string, n)
		for j := 0; j < n; j++ {
			c.endNames[i][j] = name()
		}
	}
	for i := 0; i < n; i++ {
		peers := make([]raft.Peer, n)
		for j := 0; j < n; j++ {
			end := c.net.MakeEnd(c.endNames[i][j])
			peers[j] = &rpcAdapter{end: end}
		}
		sc := StartServer(peers, i, c.persists[i])
		c.servers[i] = sc
		s := rpc.NewLabServer()
		s.AddService(rpc.NewLabService(sc))
		s.AddService(rpc.NewLabService(sc.Raft()))
		c.srv[i] = s
		c.net.AddServer(i, s)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			c.net.Enable(c.endNames[i][j], true)
			c.net.Connect(c.endNames[i][j], j)
		}
	}
	return c
}

func (c *scCluster) makeClerk() *Clerk {
	peers := make([]raft.Peer, c.n)
	for i := 0; i < c.n; i++ {
		nm := name()
		end := c.net.MakeEnd(nm)
		peers[i] = &rpcAdapter{end: end}
		c.net.Enable(nm, true)
		c.net.Connect(nm, i)
	}
	return MakeClerk(peers)
}

func (c *scCluster) cleanup() {
	for _, s := range c.servers {
		if s != nil {
			s.Kill()
		}
	}
	c.net.Cleanup()
}

func TestSCInitial(t *testing.T) {
	c := newSCCluster(t, 3)
	defer c.cleanup()
	clerk := c.makeClerk()
	cfg := clerk.Query(-1)
	if cfg.Num != 0 {
		t.Fatalf("expected initial Num=0, got %d", cfg.Num)
	}
}

func TestSCJoinLeave(t *testing.T) {
	c := newSCCluster(t, 3)
	defer c.cleanup()
	clerk := c.makeClerk()

	clerk.Join(map[int][]string{1: {"a", "b"}})
	cfg := clerk.Query(-1)
	if cfg.Num != 1 {
		t.Fatalf("after Join expected Num=1 got %d", cfg.Num)
	}
	for _, gid := range cfg.Shards {
		if gid != 1 {
			t.Fatalf("after single-group Join expected all shards on 1, got %v", cfg.Shards)
		}
	}

	clerk.Join(map[int][]string{2: {"c"}, 3: {"d"}})
	cfg = clerk.Query(-1)
	counts := map[int]int{}
	for _, gid := range cfg.Shards {
		counts[gid]++
	}
	if len(counts) != 3 {
		t.Fatalf("expected 3 groups serving shards, got %v", counts)
	}
	for gid, c2 := range counts {
		if c2 < NShards/3-1 || c2 > NShards/3+2 {
			t.Fatalf("group %d carries %d shards (target ~%d)", gid, c2, NShards/3)
		}
	}

	clerk.Leave([]int{2, 3})
	cfg = clerk.Query(-1)
	for _, gid := range cfg.Shards {
		if gid != 1 {
			t.Fatalf("after Leave expected all shards on 1, got %v", cfg.Shards)
		}
	}
}

func TestSCMove(t *testing.T) {
	c := newSCCluster(t, 3)
	defer c.cleanup()
	clerk := c.makeClerk()
	clerk.Join(map[int][]string{1: {"a"}, 2: {"b"}})
	clerk.Move(0, 2)
	cfg := clerk.Query(-1)
	if cfg.Shards[0] != 2 {
		t.Fatalf("Move did not stick: %v", cfg.Shards)
	}
}

func TestSCMinimumMovement(t *testing.T) {
	c := newSCCluster(t, 3)
	defer c.cleanup()
	clerk := c.makeClerk()
	clerk.Join(map[int][]string{1: {"a"}, 2: {"b"}, 3: {"c"}})
	before := clerk.Query(-1)
	clerk.Join(map[int][]string{4: {"d"}})
	after := clerk.Query(-1)

	moved := 0
	for i := 0; i < NShards; i++ {
		if before.Shards[i] != after.Shards[i] {
			moved++
		}
	}
	if moved > NShards/3+1 {
		t.Fatalf("rebalance moved too many shards: %d", moved)
	}
}
