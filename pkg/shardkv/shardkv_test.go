package shardkv

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Sithumli/atlas/internal/storage"
	"github.com/Sithumli/atlas/pkg/raft"
	"github.com/Sithumli/atlas/pkg/rpc"
	"github.com/Sithumli/atlas/pkg/shardctrler"
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
	return "skv-" + strconv.FormatInt(v, 36) + "-" + time.Now().Format("150405.000000000")
}

type harness struct {
	t            *testing.T
	net          *rpc.LabNetwork
	mu           sync.Mutex
	ctrlServers  []*shardctrler.ShardCtrler
	ctrlNames    [][]string
	ctrlAddrs    []string
	ctrlSrv      []*rpc.LabServer

	groups       map[int]*group
	clerkClerk   *shardctrler.Clerk
}

type group struct {
	gid     int
	servers []*ShardKV
	addrs   []string
	srvs    []*rpc.LabServer
}

func newHarness(t *testing.T, nCtrl int) *harness {
	h := &harness{
		t:           t,
		net:         rpc.NewLabNetwork(),
		ctrlServers: make([]*shardctrler.ShardCtrler, nCtrl),
		ctrlNames:   make([][]string, nCtrl),
		ctrlAddrs:   make([]string, nCtrl),
		ctrlSrv:     make([]*rpc.LabServer, nCtrl),
		groups:      make(map[int]*group),
	}
	for i := 0; i < nCtrl; i++ {
		h.ctrlAddrs[i] = "ctrl-" + strconv.Itoa(i) + "-" + name()
		h.ctrlNames[i] = make([]string, nCtrl)
		for j := 0; j < nCtrl; j++ {
			h.ctrlNames[i][j] = name()
		}
	}
	for i := 0; i < nCtrl; i++ {
		peers := make([]raft.Peer, nCtrl)
		for j := 0; j < nCtrl; j++ {
			end := h.net.MakeEnd(h.ctrlNames[i][j])
			peers[j] = &rpcAdapter{end: end}
		}
		sc := shardctrler.StartServer(peers, i, storage.NewInMemoryPersister())
		h.ctrlServers[i] = sc
		s := rpc.NewLabServer()
		s.AddService(rpc.NewLabService(sc))
		s.AddService(rpc.NewLabService(sc.Raft()))
		h.ctrlSrv[i] = s
		h.net.AddServer(h.ctrlAddrs[i], s)
	}
	for i := 0; i < nCtrl; i++ {
		for j := 0; j < nCtrl; j++ {
			h.net.Enable(h.ctrlNames[i][j], true)
			h.net.Connect(h.ctrlNames[i][j], h.ctrlAddrs[j])
		}
	}
	clerkPeers := make([]raft.Peer, nCtrl)
	for i := 0; i < nCtrl; i++ {
		n := name()
		end := h.net.MakeEnd(n)
		clerkPeers[i] = &rpcAdapter{end: end}
		h.net.Connect(n, h.ctrlAddrs[i])
		h.net.Enable(n, true)
	}
	h.clerkClerk = shardctrler.MakeClerk(clerkPeers)
	return h
}

func (h *harness) addGroup(gid int, n int) {
	g := &group{gid: gid, servers: make([]*ShardKV, n), addrs: make([]string, n), srvs: make([]*rpc.LabServer, n)}
	for i := 0; i < n; i++ {
		g.addrs[i] = "g" + strconv.Itoa(gid) + "-" + strconv.Itoa(i) + "-" + name()
	}
	endNames := make([][]string, n)
	for i := 0; i < n; i++ {
		endNames[i] = make([]string, n)
		for j := 0; j < n; j++ {
			endNames[i][j] = name()
		}
	}
	ctrlPeers := func() []raft.Peer {
		peers := make([]raft.Peer, len(h.ctrlAddrs))
		for k, addr := range h.ctrlAddrs {
			n := name()
			end := h.net.MakeEnd(n)
			h.net.Connect(n, addr)
			h.net.Enable(n, true)
			peers[k] = &rpcAdapter{end: end}
			_ = k
		}
		return peers
	}

	makeEnd := func(addr string) raft.Peer {
		n := name()
		end := h.net.MakeEnd(n)
		h.net.Connect(n, addr)
		h.net.Enable(n, true)
		return &rpcAdapter{end: end}
	}

	for i := 0; i < n; i++ {
		peers := make([]raft.Peer, n)
		for j := 0; j < n; j++ {
			end := h.net.MakeEnd(endNames[i][j])
			peers[j] = &rpcAdapter{end: end}
		}
		kv := StartServer(peers, i, storage.NewInMemoryPersister(), gid, ctrlPeers(), makeEnd, -1)
		g.servers[i] = kv
		s := rpc.NewLabServer()
		s.AddService(rpc.NewLabService(kv))
		s.AddService(rpc.NewLabService(kv.Raft()))
		g.srvs[i] = s
		h.net.AddServer(g.addrs[i], s)
	}
	for i := 0; i < n; i++ {
		for j := 0; j < n; j++ {
			h.net.Enable(endNames[i][j], true)
			h.net.Connect(endNames[i][j], g.addrs[j])
		}
	}
	h.mu.Lock()
	h.groups[gid] = g
	h.mu.Unlock()
}

func (h *harness) joinGroup(gid int) {
	g := h.groups[gid]
	servers := append([]string(nil), g.addrs...)
	h.clerkClerk.Join(map[int][]string{gid: servers})
}

func (h *harness) leaveGroup(gid int) {
	h.clerkClerk.Leave([]int{gid})
}

func (h *harness) makeKVClerk() *Clerk {
	clerkPeers := make([]raft.Peer, len(h.ctrlAddrs))
	for i, addr := range h.ctrlAddrs {
		n := name()
		end := h.net.MakeEnd(n)
		h.net.Connect(n, addr)
		h.net.Enable(n, true)
		clerkPeers[i] = &rpcAdapter{end: end}
	}
	makeEnd := func(addr string) raft.Peer {
		n := name()
		end := h.net.MakeEnd(n)
		h.net.Connect(n, addr)
		h.net.Enable(n, true)
		return &rpcAdapter{end: end}
	}
	return MakeClerk(clerkPeers, makeEnd)
}

func (h *harness) cleanup() {
	for _, sc := range h.ctrlServers {
		if sc != nil {
			sc.Kill()
		}
	}
	for _, g := range h.groups {
		for _, kv := range g.servers {
			if kv != nil {
				kv.Kill()
			}
		}
	}
	h.net.Cleanup()
}

func TestShardKVStaticOneGroup(t *testing.T) {
	h := newHarness(t, 3)
	defer h.cleanup()
	h.addGroup(1, 3)
	h.joinGroup(1)

	clerk := h.makeKVClerk()
	clerk.Put("alpha", "1")
	clerk.Put("beta", "2")
	if v := clerk.Get("alpha"); v != "1" {
		t.Fatalf("alpha=%q", v)
	}
	if v := clerk.Get("beta"); v != "2" {
		t.Fatalf("beta=%q", v)
	}
}

func TestShardKVAddGroupMigration(t *testing.T) {
	h := newHarness(t, 3)
	defer h.cleanup()
	h.addGroup(1, 3)
	h.joinGroup(1)
	clerk := h.makeKVClerk()
	for i := 0; i < 20; i++ {
		clerk.Put("k"+strconv.Itoa(i), strconv.Itoa(i))
	}

	h.addGroup(2, 3)
	h.joinGroup(2)
	time.Sleep(2 * time.Second)

	for i := 0; i < 20; i++ {
		if v := clerk.Get("k" + strconv.Itoa(i)); v != strconv.Itoa(i) {
			t.Fatalf("k%d=%q after migration", i, v)
		}
	}
}

func TestShardKVLeaveGroup(t *testing.T) {
	h := newHarness(t, 3)
	defer h.cleanup()
	h.addGroup(1, 3)
	h.addGroup(2, 3)
	h.joinGroup(1)
	h.joinGroup(2)
	clerk := h.makeKVClerk()
	for i := 0; i < 20; i++ {
		clerk.Put("k"+strconv.Itoa(i), strconv.Itoa(i*7))
	}

	h.leaveGroup(2)
	time.Sleep(2 * time.Second)
	for i := 0; i < 20; i++ {
		want := strconv.Itoa(i * 7)
		if v := clerk.Get("k" + strconv.Itoa(i)); v != want {
			t.Fatalf("k%d=%q expected %q", i, v, want)
		}
	}
}
