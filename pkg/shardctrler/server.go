// Package shardctrler implements the configuration service for Atlas.
package shardctrler

import (
	"encoding/gob"
	"sort"
	"sync"
	"time"

	"github.com/Sithumli/atlas/pkg/raft"
)

const rpcTimeout = 2 * time.Second

// Op is a single replicated controller operation.
type Op struct {
	Kind      string // "Join" | "Leave" | "Move" | "Query"
	Servers   map[int][]string
	GIDs      []int
	Shard     int
	GID       int
	Num       int
	ClientID  int64
	RequestID int64
}

type pendingResult struct {
	clientID  int64
	requestID int64
	err       Err
	config    Config
}

// ShardCtrler is a single replica of the configuration service.
type ShardCtrler struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    bool

	configs  []Config
	lastSeen map[int64]int64
	waiters  map[int]chan pendingResult
}

// JoinArgs adds replica groups.
type JoinArgs struct {
	Servers   map[int][]string
	ClientID  int64
	RequestID int64
}

// LeaveArgs removes replica groups.
type LeaveArgs struct {
	GIDs      []int
	ClientID  int64
	RequestID int64
}

// MoveArgs force-assigns a shard to a group.
type MoveArgs struct {
	Shard     int
	GID       int
	ClientID  int64
	RequestID int64
}

// QueryArgs requests a configuration.
type QueryArgs struct {
	Num int
}

// Reply is the common reply envelope.
type Reply struct {
	Err    Err
	Config Config
}

// Err enumerates protocol-level errors.
type Err string

const (
	OK          Err = "OK"
	WrongLeader Err = "WrongLeader"
	Timeout     Err = "Timeout"
)

// StartServer creates a new ShardCtrler replica.
func StartServer(peers []raft.Peer, me int, persister raft.Persister) *ShardCtrler {
	gob.Register(Op{})
	sc := &ShardCtrler{
		applyCh:  make(chan raft.ApplyMsg, 128),
		configs:  []Config{{Groups: map[int][]string{}}},
		lastSeen: make(map[int64]int64),
		waiters:  make(map[int]chan pendingResult),
	}
	sc.rf = raft.Make(peers, me, persister, sc.applyCh)
	go sc.applier()
	return sc
}

// Raft returns the underlying Raft instance.
func (sc *ShardCtrler) Raft() *raft.Raft { return sc.rf }

// Kill shuts the controller down.
func (sc *ShardCtrler) Kill() {
	sc.mu.Lock()
	sc.dead = true
	sc.mu.Unlock()
	sc.rf.Kill()
}

func (sc *ShardCtrler) killed() bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()
	return sc.dead
}

// Join handles a Join RPC.
func (sc *ShardCtrler) Join(args *JoinArgs, reply *Reply) {
	op := Op{Kind: "Join", Servers: args.Servers, ClientID: args.ClientID, RequestID: args.RequestID}
	_, err, _ := sc.submit(op)
	reply.Err = err
}

// Leave handles a Leave RPC.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *Reply) {
	op := Op{Kind: "Leave", GIDs: args.GIDs, ClientID: args.ClientID, RequestID: args.RequestID}
	_, err, _ := sc.submit(op)
	reply.Err = err
}

// Move handles a Move RPC.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *Reply) {
	op := Op{Kind: "Move", Shard: args.Shard, GID: args.GID, ClientID: args.ClientID, RequestID: args.RequestID}
	_, err, _ := sc.submit(op)
	reply.Err = err
}

// Query handles a Query RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *Reply) {
	op := Op{Kind: "Query", Num: args.Num}
	cfg, err, _ := sc.submit(op)
	reply.Err = err
	reply.Config = cfg
}

func (sc *ShardCtrler) submit(op Op) (Config, Err, int) {
	idx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return Config{}, WrongLeader, 0
	}

	sc.mu.Lock()
	ch := make(chan pendingResult, 1)
	sc.waiters[idx] = ch
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		delete(sc.waiters, idx)
		sc.mu.Unlock()
	}()

	select {
	case r := <-ch:
		if r.clientID != op.ClientID || r.requestID != op.RequestID {
			if op.Kind != "Query" {
				return Config{}, WrongLeader, 0
			}
		}
		return r.config, r.err, idx
	case <-time.After(rpcTimeout):
		return Config{}, Timeout, 0
	}
}

func (sc *ShardCtrler) applier() {
	for !sc.killed() {
		msg, ok := <-sc.applyCh
		if !ok {
			return
		}
		if !msg.CommandValid {
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}
		sc.mu.Lock()
		res := pendingResult{clientID: op.ClientID, requestID: op.RequestID, err: OK}
		dup := op.Kind != "Query" && sc.lastSeen[op.ClientID] >= op.RequestID

		switch op.Kind {
		case "Join":
			if !dup {
				sc.applyJoin(op.Servers)
				sc.lastSeen[op.ClientID] = op.RequestID
			}
		case "Leave":
			if !dup {
				sc.applyLeave(op.GIDs)
				sc.lastSeen[op.ClientID] = op.RequestID
			}
		case "Move":
			if !dup {
				sc.applyMove(op.Shard, op.GID)
				sc.lastSeen[op.ClientID] = op.RequestID
			}
		case "Query":
			n := op.Num
			if n == -1 || n >= len(sc.configs) {
				n = len(sc.configs) - 1
			}
			res.config = sc.configs[n]
		}

		ch, has := sc.waiters[msg.CommandIndex]
		sc.mu.Unlock()
		if has {
			select {
			case ch <- res:
			default:
			}
		}
	}
}

func (sc *ShardCtrler) cloneLatest() Config {
	prev := sc.configs[len(sc.configs)-1]
	cfg := Config{
		Num:    prev.Num + 1,
		Shards: prev.Shards,
		Groups: make(map[int][]string, len(prev.Groups)),
	}
	for gid, servers := range prev.Groups {
		cp := make([]string, len(servers))
		copy(cp, servers)
		cfg.Groups[gid] = cp
	}
	return cfg
}

func (sc *ShardCtrler) applyJoin(servers map[int][]string) {
	cfg := sc.cloneLatest()
	for gid, sv := range servers {
		cp := make([]string, len(sv))
		copy(cp, sv)
		cfg.Groups[gid] = cp
	}
	cfg.Shards = rebalance(cfg.Shards, cfg.Groups)
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) applyLeave(gids []int) {
	cfg := sc.cloneLatest()
	leaving := make(map[int]bool)
	for _, gid := range gids {
		leaving[gid] = true
		delete(cfg.Groups, gid)
	}
	for i, gid := range cfg.Shards {
		if leaving[gid] {
			cfg.Shards[i] = 0
		}
	}
	cfg.Shards = rebalance(cfg.Shards, cfg.Groups)
	sc.configs = append(sc.configs, cfg)
}

func (sc *ShardCtrler) applyMove(shard, gid int) {
	cfg := sc.cloneLatest()
	cfg.Shards[shard] = gid
	sc.configs = append(sc.configs, cfg)
}

// rebalance returns a shard assignment with minimum movement that
// equalises load across the given groups.
func rebalance(prev [NShards]int, groups map[int][]string) [NShards]int {
	if len(groups) == 0 {
		var zero [NShards]int
		return zero
	}

	gids := make([]int, 0, len(groups))
	for gid := range groups {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	owns := make(map[int][]int, len(gids))
	for _, gid := range gids {
		owns[gid] = nil
	}
	unassigned := []int{}
	for s, gid := range prev {
		if _, ok := groups[gid]; !ok || gid == 0 {
			unassigned = append(unassigned, s)
		} else {
			owns[gid] = append(owns[gid], s)
		}
	}

	target := NShards / len(gids)
	rem := NShards % len(gids)

	max := func() (int, int) {
		var bestGid, bestLen int
		first := true
		for _, gid := range gids {
			l := len(owns[gid])
			if first || l > bestLen || (l == bestLen && gid < bestGid) {
				bestGid, bestLen = gid, l
				first = false
			}
		}
		return bestGid, bestLen
	}

	min := func() (int, int) {
		var bestGid, bestLen int
		first := true
		for _, gid := range gids {
			l := len(owns[gid])
			if first || l < bestLen || (l == bestLen && gid < bestGid) {
				bestGid, bestLen = gid, l
				first = false
			}
		}
		return bestGid, bestLen
	}

	cap := func(gid int) int {
		c := target
		idx := -1
		for i, g := range gids {
			if g == gid {
				idx = i
				break
			}
		}
		if idx < rem {
			c++
		}
		return c
	}

	for _, s := range unassigned {
		gid, _ := min()
		owns[gid] = append(owns[gid], s)
	}

	for {
		hg, _ := max()
		lg, _ := min()
		if len(owns[hg])-len(owns[lg]) <= 1 && len(owns[hg]) <= cap(hg)+1 {
			break
		}
		s := owns[hg][0]
		owns[hg] = owns[hg][1:]
		owns[lg] = append(owns[lg], s)
	}

	var out [NShards]int
	for gid, shards := range owns {
		for _, s := range shards {
			out[s] = gid
		}
	}
	return out
}
