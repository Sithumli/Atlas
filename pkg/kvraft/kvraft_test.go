package kvraft

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

type kvCluster struct {
	t        *testing.T
	n        int
	servers  []*KVServer
	persists []*storage.InMemoryPersister
	endNames [][]string
	srv      []*rpc.LabServer
	net      *rpc.LabNetwork
	mu       sync.Mutex
	connected []bool
}

var nameSeq int64
var nameMu sync.Mutex

func name() string {
	nameMu.Lock()
	nameSeq++
	v := nameSeq
	nameMu.Unlock()
	return "kv-" + strconv.FormatInt(v, 36) + "-" + time.Now().Format("150405.000000000")
}

func newKVCluster(t *testing.T, n int, maxRaft int) *kvCluster {
	c := &kvCluster{
		t:         t,
		n:         n,
		servers:   make([]*KVServer, n),
		persists:  make([]*storage.InMemoryPersister, n),
		endNames:  make([][]string, n),
		srv:       make([]*rpc.LabServer, n),
		net:       rpc.NewLabNetwork(),
		connected: make([]bool, n),
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
		kv := StartKVServer(peers, i, c.persists[i], maxRaft)
		c.servers[i] = kv
		s := rpc.NewLabServer()
		s.AddService(rpc.NewLabService(kv))
		s.AddService(rpc.NewLabService(kv.Raft()))
		c.srv[i] = s
		c.net.AddServer(i, s)
	}
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	return c
}

func (c *kvCluster) connect(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[i] = true
	for j := 0; j < c.n; j++ {
		if c.connected[j] {
			c.net.Enable(c.endNames[i][j], true)
			c.net.Enable(c.endNames[j][i], true)
			c.net.Connect(c.endNames[i][j], j)
			c.net.Connect(c.endNames[j][i], i)
		}
	}
}

func (c *kvCluster) disconnect(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[i] = false
	for j := 0; j < c.n; j++ {
		c.net.Enable(c.endNames[i][j], false)
		c.net.Enable(c.endNames[j][i], false)
	}
}

func (c *kvCluster) makeClerkEnds() []raft.Peer {
	clerkNames := make([]string, c.n)
	peers := make([]raft.Peer, c.n)
	for i := 0; i < c.n; i++ {
		clerkNames[i] = name()
		end := c.net.MakeEnd(clerkNames[i])
		peers[i] = &rpcAdapter{end: end}
		c.net.Connect(clerkNames[i], i)
		c.net.Enable(clerkNames[i], true)
	}
	return peers
}

func (c *kvCluster) cleanup() {
	for _, s := range c.servers {
		if s != nil {
			s.Kill()
		}
	}
	c.net.Cleanup()
}

func TestKVBasic(t *testing.T) {
	c := newKVCluster(t, 3, -1)
	defer c.cleanup()
	clerk := MakeClerk(c.makeClerkEnds())
	clerk.Put("k", "v")
	if v := clerk.Get("k"); v != "v" {
		t.Fatalf("Get k=%q", v)
	}
	clerk.Append("k", "v2")
	if v := clerk.Get("k"); v != "vv2" {
		t.Fatalf("Append got %q", v)
	}
}

func TestKVConcurrent(t *testing.T) {
	c := newKVCluster(t, 3, -1)
	defer c.cleanup()

	const nClerks = 5
	const nOps = 20
	var wg sync.WaitGroup
	for k := 0; k < nClerks; k++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			clerk := MakeClerk(c.makeClerkEnds())
			for i := 0; i < nOps; i++ {
				key := "k" + strconv.Itoa(id) + "-" + strconv.Itoa(i%3)
				clerk.Append(key, strconv.Itoa(i)+",")
			}
		}(k)
	}
	wg.Wait()

	clerk := MakeClerk(c.makeClerkEnds())
	for k := 0; k < nClerks; k++ {
		for s := 0; s < 3; s++ {
			key := "k" + strconv.Itoa(k) + "-" + strconv.Itoa(s)
			v := clerk.Get(key)
			if v == "" {
				t.Fatalf("expected non-empty value for %s", key)
			}
		}
	}
}

func TestKVPartition(t *testing.T) {
	c := newKVCluster(t, 5, -1)
	defer c.cleanup()
	clerk := MakeClerk(c.makeClerkEnds())
	clerk.Put("a", "1")

	c.disconnect(0)
	c.disconnect(1)
	clerk.Put("a", "2")
	if v := clerk.Get("a"); v != "2" {
		t.Fatalf("expected 2 got %q", v)
	}
	c.connect(0)
	c.connect(1)
	time.Sleep(1 * time.Second)
	if v := clerk.Get("a"); v != "2" {
		t.Fatalf("expected 2 after rejoin got %q", v)
	}
}
