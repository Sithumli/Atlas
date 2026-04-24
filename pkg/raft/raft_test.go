package raft

import (
	"sync"
	"testing"
	"time"

	"github.com/Sithumli/atlas/internal/storage"
	"github.com/Sithumli/atlas/pkg/rpc"
)

type rpcAdapter struct {
	end *rpc.LabClientEnd
}

func (r *rpcAdapter) Call(method string, args any, reply any) bool {
	return r.end.Call(method, args, reply)
}

type cluster struct {
	t         *testing.T
	n         int
	rfs       []*Raft
	persists  []*storage.InMemoryPersister
	applies   []chan ApplyMsg
	logs      []map[int]any
	connected []bool
	endNames  [][]string
	servers   []*rpc.LabServer
	net       *rpc.LabNetwork
	mu        sync.Mutex
	stop      chan struct{}
}

func newCluster(t *testing.T, n int) *cluster {
	c := &cluster{
		t:         t,
		n:         n,
		rfs:       make([]*Raft, n),
		persists:  make([]*storage.InMemoryPersister, n),
		applies:   make([]chan ApplyMsg, n),
		connected: make([]bool, n),
		endNames:  make([][]string, n),
		servers:   make([]*rpc.LabServer, n),
		net:       rpc.NewLabNetwork(),
		stop:      make(chan struct{}),
		logs:      make([]map[int]any, n),
	}
	for i := 0; i < n; i++ {
		c.persists[i] = storage.NewInMemoryPersister()
		c.applies[i] = make(chan ApplyMsg, 1024)
		c.endNames[i] = make([]string, n)
		c.logs[i] = make(map[int]any)
	}
	for i := 0; i < n; i++ {
		c.start(i)
		go c.drain(i)
	}
	for i := 0; i < n; i++ {
		c.connect(i)
	}
	return c
}

func (c *cluster) drain(i int) {
	for {
		select {
		case <-c.stop:
			return
		case msg, ok := <-c.applies[i]:
			if !ok {
				return
			}
			if msg.CommandValid {
				c.mu.Lock()
				c.logs[i][msg.CommandIndex] = msg.Command
				c.mu.Unlock()
			}
		}
	}
}

func (c *cluster) start(i int) {
	for j := 0; j < c.n; j++ {
		c.endNames[i][j] = randName()
	}
	peers := make([]Peer, c.n)
	for j := 0; j < c.n; j++ {
		end := c.net.MakeEnd(c.endNames[i][j])
		peers[j] = &rpcAdapter{end: end}
	}
	rf := Make(peers, i, c.persists[i], c.applies[i])
	c.rfs[i] = rf
	srv := rpc.NewLabServer()
	srv.AddService(rpc.NewLabService(rf))
	c.servers[i] = srv
	c.net.AddServer(i, srv)
}

func (c *cluster) connect(i int) {
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

func (c *cluster) disconnect(i int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.connected[i] = false
	for j := 0; j < c.n; j++ {
		c.net.Enable(c.endNames[i][j], false)
		c.net.Enable(c.endNames[j][i], false)
	}
}

func (c *cluster) cleanup() {
	close(c.stop)
	for _, rf := range c.rfs {
		if rf != nil {
			rf.Kill()
		}
	}
	c.net.Cleanup()
}

func (c *cluster) checkOneLeader() int {
	for tries := 0; tries < 10; tries++ {
		time.Sleep(500 * time.Millisecond)
		leaders := make(map[int][]int)
		for i := 0; i < c.n; i++ {
			c.mu.Lock()
			conn := c.connected[i]
			c.mu.Unlock()
			if !conn {
				continue
			}
			term, isLeader := c.rfs[i].GetState()
			if isLeader {
				leaders[term] = append(leaders[term], i)
			}
		}
		latestTerm := -1
		for term := range leaders {
			if term > latestTerm {
				latestTerm = term
			}
		}
		if latestTerm < 0 {
			continue
		}
		if len(leaders[latestTerm]) == 1 {
			return leaders[latestTerm][0]
		}
		if len(leaders[latestTerm]) > 1 {
			c.t.Fatalf("multiple leaders in term %d: %v", latestTerm, leaders[latestTerm])
		}
	}
	c.t.Fatalf("expected one leader, got none")
	return -1
}

func (c *cluster) checkNoLeader() {
	for i := 0; i < c.n; i++ {
		c.mu.Lock()
		conn := c.connected[i]
		c.mu.Unlock()
		if conn {
			_, isLeader := c.rfs[i].GetState()
			if isLeader {
				c.t.Fatalf("expected no leader, but %d is leader", i)
			}
		}
	}
}

func (c *cluster) one(cmd any, expectedServers int, retry bool) int {
	t0 := time.Now()
	starts := 0
	for time.Since(t0) < 10*time.Second {
		index := -1
		for i := 0; i < c.n; i++ {
			starts = (starts + 1) % c.n
			c.mu.Lock()
			conn := c.connected[starts]
			c.mu.Unlock()
			if !conn {
				continue
			}
			idx, _, ok := c.rfs[starts].Start(cmd)
			if ok {
				index = idx
				break
			}
		}
		if index != -1 {
			t1 := time.Now()
			for time.Since(t1) < 2*time.Second {
				nd, cmd1 := c.nCommitted(index)
				if nd > 0 && nd >= expectedServers {
					if eq(cmd1, cmd) {
						return index
					}
				}
				time.Sleep(20 * time.Millisecond)
			}
			if !retry {
				c.t.Fatalf("one(%v) failed to reach agreement", cmd)
			}
		} else {
			time.Sleep(50 * time.Millisecond)
		}
	}
	c.t.Fatalf("one(%v) failed to reach agreement", cmd)
	return -1
}

func (c *cluster) nCommitted(index int) (int, any) {
	count := 0
	var cmd any
	c.mu.Lock()
	defer c.mu.Unlock()
	for i := 0; i < c.n; i++ {
		if v, ok := c.logs[i][index]; ok {
			if count > 0 && v != cmd {
				c.t.Fatalf("committed values disagree at index %d: %v vs %v", index, cmd, v)
			}
			count++
			cmd = v
		}
	}
	return count, cmd
}

func eq(a, b any) bool { return a == b }

var nameSeq int64
var nameMu sync.Mutex

func randName() string {
	nameMu.Lock()
	nameSeq++
	id := nameSeq
	nameMu.Unlock()
	b := make([]byte, 0, 32)
	for v := id; v > 0; v /= 26 {
		b = append(b, byte('a'+v%26))
	}
	if len(b) == 0 {
		b = append(b, 'a')
	}
	return string(b) + time.Now().Format("150405.000000000")
}

// ----- tests -----

func TestInitialElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.cleanup()
	c.checkOneLeader()
}

func TestReElection(t *testing.T) {
	c := newCluster(t, 3)
	defer c.cleanup()
	leader := c.checkOneLeader()
	c.disconnect(leader)
	time.Sleep(2 * time.Second)
	leader2 := c.checkOneLeader()
	if leader2 == leader {
		t.Fatalf("expected new leader after partition")
	}
}

func TestBasicAgreement(t *testing.T) {
	c := newCluster(t, 3)
	defer c.cleanup()
	c.checkOneLeader()
	for i := 1; i <= 3; i++ {
		c.one(i*100, 3, false)
	}
}

func TestNoLeaderInMinority(t *testing.T) {
	c := newCluster(t, 5)
	defer c.cleanup()
	leader := c.checkOneLeader()
	c.disconnect((leader + 1) % 5)
	c.disconnect((leader + 2) % 5)
	c.disconnect((leader + 3) % 5)
	time.Sleep(2 * time.Second)
	c.disconnect(leader)
	time.Sleep(2 * time.Second)
	c.checkNoLeader()
}

func TestFollowerCatchup(t *testing.T) {
	c := newCluster(t, 3)
	defer c.cleanup()
	leader := c.checkOneLeader()
	follower := (leader + 1) % 3
	c.disconnect(follower)
	for i := 1; i <= 5; i++ {
		c.one(i*1000, 2, false)
	}
	c.connect(follower)
	time.Sleep(2 * time.Second)
	c.one(99999, 3, true)
}

func TestUnreliableNetwork(t *testing.T) {
	c := newCluster(t, 5)
	defer c.cleanup()
	c.checkOneLeader()
	c.net.SetReliable(false)
	for i := 1; i <= 5; i++ {
		c.one(i*7, 4, true)
	}
}

func TestStartStop(t *testing.T) {
	c := newCluster(t, 3)
	defer c.cleanup()
	c.checkOneLeader()
	c.one(101, 3, false)
	c.one(102, 3, false)
}
