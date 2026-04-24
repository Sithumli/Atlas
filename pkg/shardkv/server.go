// Package shardkv implements a sharded, linearizable key-value store.
package shardkv

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/Sithumli/atlas/pkg/raft"
	"github.com/Sithumli/atlas/pkg/shardctrler"
)

const rpcTimeout = 2 * time.Second
const pollInterval = 80 * time.Millisecond

// Op is the command replicated through Raft.
type Op struct {
	Kind      string // "Get" | "Put" | "Append" | "Config" | "Install" | "Drop"
	Key       string
	Value     string
	ConfigNum int
	ClientID  int64
	RequestID int64

	NewConfig shardctrler.Config

	InstallShard    int
	InstallData     map[string]string
	InstallLastSeen map[int64]int64

	DropShard int
}

type pendingResult struct {
	clientID  int64
	requestID int64
	value     string
	err       Err
}

// ShardKV is one replica.
type ShardKV struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    bool

	gid          int
	me           int
	peers        []raft.Peer
	makeEnd      func(addr string) raft.Peer
	mck          *shardctrler.Clerk
	persister    raft.Persister
	maxRaftState int

	config     shardctrler.Config
	prevConfig shardctrler.Config

	shardData     map[int]map[string]string
	shardLastSeen map[int]map[int64]int64
	shardState    map[int]ShardState

	waiters     map[int]chan pendingResult
	lastApplied int
}

// Shard is the per-shard state owned by this group.
type Shard struct {
	Data     map[string]string
	LastSeen map[int64]int64
	State    ShardState
}

// ShardState tracks the migration status of a shard.
type ShardState int

const (
	NotOwned ShardState = iota
	Pulling
	Serving
	Pushing
)

// StartServer creates a new ShardKV replica.
func StartServer(
	peers []raft.Peer,
	me int,
	persister raft.Persister,
	gid int,
	ctrlers []raft.Peer,
	makeEnd func(addr string) raft.Peer,
	maxRaftState int,
) *ShardKV {
	gob.Register(Op{})
	gob.Register(shardctrler.Config{})

	kv := &ShardKV{
		applyCh:       make(chan raft.ApplyMsg, 128),
		gid:           gid,
		me:            me,
		peers:         peers,
		makeEnd:       makeEnd,
		mck:           shardctrler.MakeClerk(ctrlers),
		persister:     persister,
		maxRaftState:  maxRaftState,
		shardData:     make(map[int]map[string]string),
		shardLastSeen: make(map[int]map[int64]int64),
		shardState:    make(map[int]ShardState),
		waiters:       make(map[int]chan pendingResult),
		config:        shardctrler.Config{Groups: map[int][]string{}},
		prevConfig:    shardctrler.Config{Groups: map[int][]string{}},
	}
	for s := 0; s < shardctrler.NShards; s++ {
		kv.shardState[s] = NotOwned
	}
	kv.rf = raft.Make(peers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	go kv.configPoller()
	go kv.migrator()
	return kv
}

// Raft returns the underlying Raft.
func (kv *ShardKV) Raft() *raft.Raft { return kv.rf }

// Kill shuts the server down.
func (kv *ShardKV) Kill() {
	kv.mu.Lock()
	kv.dead = true
	kv.mu.Unlock()
	kv.rf.Kill()
}

func (kv *ShardKV) killed() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.dead
}

// Err enumerates protocol-level errors returned by shardkv.
type Err string

const (
	OK          Err = "OK"
	ErrNoKey    Err = "ErrNoKey"
	WrongGroup  Err = "WrongGroup"
	WrongLeader Err = "WrongLeader"
	NotReady    Err = "NotReady"
	Timeout     Err = "Timeout"
)

// GetArgs is a client Get request.
type GetArgs struct {
	Key       string
	ConfigNum int
	ClientID  int64
	RequestID int64
}

// PutAppendArgs is a client Put or Append request.
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	ConfigNum int
	ClientID  int64
	RequestID int64
}

// Reply is the common reply envelope.
type Reply struct {
	Err   Err
	Value string
}

// Get handles a linearizable read.
func (kv *ShardKV) Get(args *GetArgs, reply *Reply) {
	op := Op{Kind: "Get", Key: args.Key, ConfigNum: args.ConfigNum, ClientID: args.ClientID, RequestID: args.RequestID}
	res, err := kv.submit(op)
	reply.Err = err
	if err == OK {
		reply.Value = res
	}
}

// PutAppend handles a linearizable write.
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *Reply) {
	op := Op{Kind: args.Op, Key: args.Key, Value: args.Value, ConfigNum: args.ConfigNum, ClientID: args.ClientID, RequestID: args.RequestID}
	_, err := kv.submit(op)
	reply.Err = err
}

func (kv *ShardKV) submit(op Op) (string, Err) {
	kv.mu.Lock()
	shard := shardctrler.Key2Shard(op.Key)
	if op.ConfigNum != kv.config.Num {
		kv.mu.Unlock()
		return "", WrongGroup
	}
	if kv.config.Shards[shard] != kv.gid {
		kv.mu.Unlock()
		return "", WrongGroup
	}
	if kv.shardState[shard] != Serving {
		kv.mu.Unlock()
		return "", NotReady
	}
	kv.mu.Unlock()

	idx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return "", WrongLeader
	}

	kv.mu.Lock()
	ch := make(chan pendingResult, 1)
	kv.waiters[idx] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		delete(kv.waiters, idx)
		kv.mu.Unlock()
	}()

	select {
	case r := <-ch:
		if r.clientID != op.ClientID || r.requestID != op.RequestID {
			return "", WrongLeader
		}
		return r.value, r.err
	case <-time.After(rpcTimeout):
		return "", Timeout
	}
}

// MigrateArgs is used by one group to pull shard state from another.
type MigrateArgs struct {
	ConfigNum int
	ShardIDs  []int
}

// MigrateReply carries the requested shard state back to the puller.
type MigrateReply struct {
	Err      Err
	Data     map[int]map[string]string
	LastSeen map[int]map[int64]int64
}

// Migrate hands the requested shards to a peer group.
func (kv *ShardKV) Migrate(args *MigrateArgs, reply *MigrateReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if args.ConfigNum > kv.config.Num {
		reply.Err = NotReady
		return
	}

	reply.Data = make(map[int]map[string]string)
	reply.LastSeen = make(map[int]map[int64]int64)
	for _, s := range args.ShardIDs {
		data := make(map[string]string)
		for k, v := range kv.shardData[s] {
			data[k] = v
		}
		ls := make(map[int64]int64)
		for k, v := range kv.shardLastSeen[s] {
			ls[k] = v
		}
		reply.Data[s] = data
		reply.LastSeen[s] = ls
	}
	reply.Err = OK
}

// AckArgs is used by the new owner to confirm migration so that the old
// owner may drop the shard.
type AckArgs struct {
	ConfigNum int
	ShardIDs  []int
}

// AckReply is the response to Ack.
type AckReply struct {
	Err Err
}

// Ack confirms shard transfer; receiver drops the shard.
func (kv *ShardKV) Ack(args *AckArgs, reply *AckReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = WrongLeader
		return
	}
	for _, s := range args.ShardIDs {
		op := Op{Kind: "Drop", DropShard: s, ConfigNum: args.ConfigNum}
		kv.rf.Start(op)
	}
	reply.Err = OK
}

// ----- applier -----

func (kv *ShardKV) applier() {
	for !kv.killed() {
		msg, ok := <-kv.applyCh
		if !ok {
			return
		}
		if msg.SnapshotValid {
			kv.mu.Lock()
			kv.readSnapshot(msg.Snapshot)
			if msg.SnapshotIndex > kv.lastApplied {
				kv.lastApplied = msg.SnapshotIndex
			}
			kv.mu.Unlock()
			continue
		}
		if !msg.CommandValid {
			continue
		}
		op, ok := msg.Command.(Op)
		if !ok {
			continue
		}

		kv.mu.Lock()
		if msg.CommandIndex <= kv.lastApplied {
			kv.mu.Unlock()
			continue
		}
		kv.lastApplied = msg.CommandIndex

		res := pendingResult{clientID: op.ClientID, requestID: op.RequestID, err: OK}

		switch op.Kind {
		case "Get", "Put", "Append":
			res = kv.applyClientOp(op)
		case "Config":
			kv.applyConfig(op.NewConfig)
		case "Install":
			kv.applyInstall(op)
		case "Drop":
			kv.applyDrop(op)
		}

		ch, has := kv.waiters[msg.CommandIndex]
		kv.maybeSnapshot(msg.CommandIndex)
		kv.mu.Unlock()

		if has {
			select {
			case ch <- res:
			default:
			}
		}
	}
}

func (kv *ShardKV) applyClientOp(op Op) pendingResult {
	res := pendingResult{clientID: op.ClientID, requestID: op.RequestID, err: OK}
	shard := shardctrler.Key2Shard(op.Key)
	if kv.config.Shards[shard] != kv.gid {
		res.err = WrongGroup
		return res
	}
	if kv.shardState[shard] != Serving {
		res.err = NotReady
		return res
	}
	if kv.shardData[shard] == nil {
		kv.shardData[shard] = make(map[string]string)
	}
	if kv.shardLastSeen[shard] == nil {
		kv.shardLastSeen[shard] = make(map[int64]int64)
	}
	dup := kv.shardLastSeen[shard][op.ClientID] >= op.RequestID

	switch op.Kind {
	case "Get":
		v, ok := kv.shardData[shard][op.Key]
		if !ok {
			res.err = ErrNoKey
		}
		res.value = v
	case "Put":
		if !dup {
			kv.shardData[shard][op.Key] = op.Value
			kv.shardLastSeen[shard][op.ClientID] = op.RequestID
		}
	case "Append":
		if !dup {
			kv.shardData[shard][op.Key] = kv.shardData[shard][op.Key] + op.Value
			kv.shardLastSeen[shard][op.ClientID] = op.RequestID
		}
	}
	return res
}

func (kv *ShardKV) applyConfig(newCfg shardctrler.Config) {
	if newCfg.Num != kv.config.Num+1 {
		return
	}
	for s := 0; s < shardctrler.NShards; s++ {
		if !kv.allShardsStable() {
			return
		}
		_ = s
	}
	kv.prevConfig = kv.config
	kv.config = newCfg

	for s := 0; s < shardctrler.NShards; s++ {
		oldOwner := kv.prevConfig.Shards[s]
		newOwner := kv.config.Shards[s]
		if newOwner == kv.gid {
			if oldOwner == 0 {
				kv.shardState[s] = Serving
				if kv.shardData[s] == nil {
					kv.shardData[s] = make(map[string]string)
				}
				if kv.shardLastSeen[s] == nil {
					kv.shardLastSeen[s] = make(map[int64]int64)
				}
			} else if oldOwner == kv.gid {
				kv.shardState[s] = Serving
			} else {
				kv.shardState[s] = Pulling
			}
		} else {
			if oldOwner == kv.gid {
				kv.shardState[s] = Pushing
			} else {
				kv.shardState[s] = NotOwned
			}
		}
	}
}

func (kv *ShardKV) allShardsStable() bool {
	for s := 0; s < shardctrler.NShards; s++ {
		st := kv.shardState[s]
		if st == Pulling || st == Pushing {
			return false
		}
	}
	return true
}

func (kv *ShardKV) applyInstall(op Op) {
	if op.ConfigNum != kv.config.Num {
		return
	}
	if kv.shardState[op.InstallShard] != Pulling {
		return
	}
	d := make(map[string]string, len(op.InstallData))
	for k, v := range op.InstallData {
		d[k] = v
	}
	ls := make(map[int64]int64, len(op.InstallLastSeen))
	for k, v := range op.InstallLastSeen {
		ls[k] = v
	}
	kv.shardData[op.InstallShard] = d
	kv.shardLastSeen[op.InstallShard] = ls
	kv.shardState[op.InstallShard] = Serving
}

func (kv *ShardKV) applyDrop(op Op) {
	if kv.shardState[op.DropShard] != Pushing {
		return
	}
	delete(kv.shardData, op.DropShard)
	delete(kv.shardLastSeen, op.DropShard)
	kv.shardState[op.DropShard] = NotOwned
}

// ----- config poller -----

func (kv *ShardKV) configPoller() {
	for !kv.killed() {
		time.Sleep(pollInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}
		kv.mu.Lock()
		curNum := kv.config.Num
		stable := kv.allShardsStable()
		kv.mu.Unlock()
		if !stable {
			continue
		}
		nextCfg := kv.mck.Query(curNum + 1)
		if nextCfg.Num != curNum+1 {
			continue
		}
		kv.rf.Start(Op{Kind: "Config", NewConfig: nextCfg})
	}
}

// ----- migrator -----

func (kv *ShardKV) migrator() {
	for !kv.killed() {
		time.Sleep(pollInterval)
		if _, isLeader := kv.rf.GetState(); !isLeader {
			continue
		}

		kv.mu.Lock()
		curNum := kv.config.Num
		bySrc := make(map[int][]int)
		for s := 0; s < shardctrler.NShards; s++ {
			if kv.shardState[s] == Pulling {
				src := kv.prevConfig.Shards[s]
				if src != 0 && src != kv.gid {
					bySrc[src] = append(bySrc[src], s)
				}
			}
		}
		groups := make(map[int][]string, len(kv.prevConfig.Groups))
		for gid, servers := range kv.prevConfig.Groups {
			groups[gid] = append([]string(nil), servers...)
		}
		kv.mu.Unlock()

		for src, shards := range bySrc {
			servers := groups[src]
			args := &MigrateArgs{ConfigNum: curNum, ShardIDs: shards}
			for _, addr := range servers {
				peer := kv.makeEnd(addr)
				reply := &MigrateReply{}
				ok := peer.Call("ShardKV.Migrate", args, reply)
				if ok && reply.Err == OK {
					for _, s := range shards {
						kv.rf.Start(Op{
							Kind:            "Install",
							ConfigNum:       curNum,
							InstallShard:    s,
							InstallData:     reply.Data[s],
							InstallLastSeen: reply.LastSeen[s],
						})
					}
					ack := &AckArgs{ConfigNum: curNum, ShardIDs: shards}
					ackReply := &AckReply{}
					for _, addr2 := range servers {
						peer2 := kv.makeEnd(addr2)
						if peer2.Call("ShardKV.Ack", ack, ackReply) && ackReply.Err == OK {
							break
						}
					}
					break
				}
			}
		}
	}
}

// ----- snapshots -----

type snapshotState struct {
	Config        shardctrler.Config
	PrevConfig    shardctrler.Config
	ShardData     map[int]map[string]string
	ShardLastSeen map[int]map[int64]int64
	ShardState    map[int]ShardState
	LastApplied   int
}

func (kv *ShardKV) maybeSnapshot(index int) {
	if kv.maxRaftState <= 0 {
		return
	}
	if kv.persister.StateSize() < kv.maxRaftState {
		return
	}
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	st := snapshotState{
		Config:        kv.config,
		PrevConfig:    kv.prevConfig,
		ShardData:     kv.shardData,
		ShardLastSeen: kv.shardLastSeen,
		ShardState:    kv.shardState,
		LastApplied:   kv.lastApplied,
	}
	_ = enc.Encode(st)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	dec := gob.NewDecoder(r)
	var st snapshotState
	if err := dec.Decode(&st); err != nil {
		return
	}
	kv.config = st.Config
	kv.prevConfig = st.PrevConfig
	kv.shardData = st.ShardData
	kv.shardLastSeen = st.ShardLastSeen
	kv.shardState = st.ShardState
	kv.lastApplied = st.LastApplied
	if kv.shardData == nil {
		kv.shardData = make(map[int]map[string]string)
	}
	if kv.shardLastSeen == nil {
		kv.shardLastSeen = make(map[int]map[int64]int64)
	}
	if kv.shardState == nil {
		kv.shardState = make(map[int]ShardState)
		for s := 0; s < shardctrler.NShards; s++ {
			kv.shardState[s] = NotOwned
		}
	}
}
