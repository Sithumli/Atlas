package chaos

import (
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/Sithumli/atlas/internal/storage"
	"github.com/Sithumli/atlas/pkg/raft"
	"github.com/Sithumli/atlas/pkg/rpc"
	"github.com/Sithumli/atlas/pkg/shardctrler"
	"github.com/Sithumli/atlas/pkg/shardkv"
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
	return "ch-" + strconv.FormatInt(v, 36) + "-" + time.Now().Format("150405.000000000")
}

func TestChaosUnreliableNetwork(t *testing.T) {
	net := rpc.NewLabNetwork()
	defer net.Cleanup()

	const nCtrl = 3
	ctrlAddrs := make([]string, nCtrl)
	ctrlNames := make([][]string, nCtrl)
	ctrls := make([]*shardctrler.ShardCtrler, nCtrl)

	for i := 0; i < nCtrl; i++ {
		ctrlAddrs[i] = "ctrl-" + name()
		ctrlNames[i] = make([]string, nCtrl)
		for j := 0; j < nCtrl; j++ {
			ctrlNames[i][j] = name()
		}
	}
	for i := 0; i < nCtrl; i++ {
		peers := make([]raft.Peer, nCtrl)
		for j := 0; j < nCtrl; j++ {
			end := net.MakeEnd(ctrlNames[i][j])
			peers[j] = &rpcAdapter{end: end}
		}
		sc := shardctrler.StartServer(peers, i, storage.NewInMemoryPersister())
		ctrls[i] = sc
		s := rpc.NewLabServer()
		s.AddService(rpc.NewLabService(sc))
		s.AddService(rpc.NewLabService(sc.Raft()))
		net.AddServer(ctrlAddrs[i], s)
	}
	for i := 0; i < nCtrl; i++ {
		for j := 0; j < nCtrl; j++ {
			net.Enable(ctrlNames[i][j], true)
			net.Connect(ctrlNames[i][j], ctrlAddrs[j])
		}
	}
	defer func() {
		for _, sc := range ctrls {
			sc.Kill()
		}
	}()

	clerkPeers := func() []raft.Peer {
		peers := make([]raft.Peer, nCtrl)
		for i := 0; i < nCtrl; i++ {
			n := name()
			end := net.MakeEnd(n)
			peers[i] = &rpcAdapter{end: end}
			net.Connect(n, ctrlAddrs[i])
			net.Enable(n, true)
		}
		return peers
	}
	mck := shardctrler.MakeClerk(clerkPeers())

	gid := 1
	addrs := []string{"g1-" + name(), "g1-" + name(), "g1-" + name()}
	groupNames := make([][]string, 3)
	for i := 0; i < 3; i++ {
		groupNames[i] = make([]string, 3)
		for j := 0; j < 3; j++ {
			groupNames[i][j] = name()
		}
	}
	makeEnd := func(addr string) raft.Peer {
		n := name()
		end := net.MakeEnd(n)
		net.Connect(n, addr)
		net.Enable(n, true)
		return &rpcAdapter{end: end}
	}
	for i := 0; i < 3; i++ {
		peers := make([]raft.Peer, 3)
		for j := 0; j < 3; j++ {
			end := net.MakeEnd(groupNames[i][j])
			peers[j] = &rpcAdapter{end: end}
		}
		kv := shardkv.StartServer(peers, i, storage.NewInMemoryPersister(), gid, clerkPeers(), makeEnd, -1)
		s := rpc.NewLabServer()
		s.AddService(rpc.NewLabService(kv))
		s.AddService(rpc.NewLabService(kv.Raft()))
		net.AddServer(addrs[i], s)
		defer kv.Kill()
	}
	for i := 0; i < 3; i++ {
		for j := 0; j < 3; j++ {
			net.Enable(groupNames[i][j], true)
			net.Connect(groupNames[i][j], addrs[j])
		}
	}
	mck.Join(map[int][]string{gid: addrs})

	clerk := shardkv.MakeClerk(clerkPeers(), makeEnd)
	clerk.Put("a", "1")
	if v := clerk.Get("a"); v != "1" {
		t.Fatalf("a=%q before chaos", v)
	}

	net.SetReliable(false)
	for i := 0; i < 10; i++ {
		clerk.Append("a", strconv.Itoa(i))
	}
	net.SetReliable(true)

	v := clerk.Get("a")
	if len(v) < 1 {
		t.Fatalf("expected non-empty got %q", v)
	}
}
