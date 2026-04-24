// Package kvraft implements a linearizable key-value state machine layered
// on top of a single Raft group.
package kvraft

import (
	"bytes"
	"encoding/gob"
	"sync"
	"time"

	"github.com/Sithumli/atlas/pkg/raft"
)

const rpcTimeout = 2 * time.Second

// Op is the command replicated through Raft for each KV operation.
type Op struct {
	Kind      string
	Key       string
	Value     string
	ClientID  int64
	RequestID int64
}

type pendingResult struct {
	clientID  int64
	requestID int64
	value     string
	err       Err
}

// KVServer is one replica of the KV state machine.
type KVServer struct {
	mu      sync.Mutex
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    bool

	persister raft.Persister

	store    map[string]string
	lastSeen map[int64]int64
	waiters  map[int]chan pendingResult

	maxRaftState int
	lastApplied  int
}

// StartKVServer creates a new KV replica.
func StartKVServer(peers []raft.Peer, me int, persister raft.Persister, maxRaftState int) *KVServer {
	gob.Register(Op{})

	kv := &KVServer{
		applyCh:      make(chan raft.ApplyMsg, 128),
		store:        make(map[string]string),
		lastSeen:     make(map[int64]int64),
		waiters:      make(map[int]chan pendingResult),
		persister:    persister,
		maxRaftState: maxRaftState,
	}
	kv.rf = raft.Make(peers, me, persister, kv.applyCh)
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applier()
	return kv
}

// Raft returns the underlying Raft instance.
func (kv *KVServer) Raft() *raft.Raft { return kv.rf }

// Kill shuts the server down.
func (kv *KVServer) Kill() {
	kv.mu.Lock()
	kv.dead = true
	kv.mu.Unlock()
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	return kv.dead
}

// GetArgs is a client Get request.
type GetArgs struct {
	Key       string
	ClientID  int64
	RequestID int64
}

// PutAppendArgs is a client Put or Append request.
type PutAppendArgs struct {
	Key       string
	Value     string
	Op        string
	ClientID  int64
	RequestID int64
}

// Reply is the common reply envelope.
type Reply struct {
	Err   Err
	Value string
}

// Err enumerates protocol-level errors.
type Err string

const (
	OK          Err = "OK"
	ErrNoKey    Err = "ErrNoKey"
	WrongLeader Err = "WrongLeader"
	Timeout     Err = "Timeout"
)

// Get handles a linearizable read.
func (kv *KVServer) Get(args *GetArgs, reply *Reply) {
	op := Op{Kind: "Get", Key: args.Key, ClientID: args.ClientID, RequestID: args.RequestID}
	res, err := kv.submit(op)
	reply.Err = err
	if err == OK {
		reply.Value = res
	}
}

// PutAppend handles a linearizable write.
func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *Reply) {
	op := Op{Kind: args.Op, Key: args.Key, Value: args.Value, ClientID: args.ClientID, RequestID: args.RequestID}
	_, err := kv.submit(op)
	reply.Err = err
}

func (kv *KVServer) submit(op Op) (string, Err) {
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

func (kv *KVServer) applier() {
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
		dup := kv.lastSeen[op.ClientID] >= op.RequestID

		switch op.Kind {
		case "Get":
			v, exists := kv.store[op.Key]
			if !exists {
				res.err = ErrNoKey
			}
			res.value = v
		case "Put":
			if !dup {
				kv.store[op.Key] = op.Value
				kv.lastSeen[op.ClientID] = op.RequestID
			}
		case "Append":
			if !dup {
				kv.store[op.Key] = kv.store[op.Key] + op.Value
				kv.lastSeen[op.ClientID] = op.RequestID
			}
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

func (kv *KVServer) maybeSnapshot(index int) {
	if kv.maxRaftState <= 0 {
		return
	}
	if kv.persister.StateSize() < kv.maxRaftState {
		return
	}
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	_ = enc.Encode(kv.store)
	_ = enc.Encode(kv.lastSeen)
	kv.rf.Snapshot(index, w.Bytes())
}

func (kv *KVServer) readSnapshot(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	dec := gob.NewDecoder(r)
	var store map[string]string
	var lastSeen map[int64]int64
	if err := dec.Decode(&store); err != nil {
		return
	}
	if err := dec.Decode(&lastSeen); err != nil {
		return
	}
	kv.store = store
	kv.lastSeen = lastSeen
}
