// Package raft implements the Raft consensus algorithm as described in
// Ongaro and Ousterhout, "In Search of an Understandable Consensus Algorithm",
// USENIX ATC 2014.
//
// Each Raft instance communicates with its peers via an RPC layer provided
// by the caller. Clients submit commands via Start; committed commands are
// delivered in order on the applyCh channel provided at construction time.
package raft

import (
	"bytes"
	"encoding/gob"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// Role enumerates the three Raft states.
type Role int

const (
	Follower Role = iota
	Candidate
	Leader
)

// ApplyMsg is delivered on the apply channel for every committed log entry
// or installed snapshot.
type ApplyMsg struct {
	CommandValid bool
	Command      any
	CommandIndex int
	CommandTerm  int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// LogEntry is a single entry in the Raft log.
type LogEntry struct {
	Term    int
	Index   int
	Command any
}

// Persister abstracts durable storage of Raft state.
type Persister interface {
	SaveState(state []byte)
	ReadState() []byte
	SaveStateAndSnapshot(state, snapshot []byte)
	ReadSnapshot() []byte
	StateSize() int
}

// Peer is the transport handle used to send an RPC to another Raft member.
type Peer interface {
	Call(method string, args any, reply any) bool
}

const (
	heartbeatInterval = 100 * time.Millisecond
	electionTimeoutMin = 350 * time.Millisecond
	electionTimeoutMax = 700 * time.Millisecond
)

// Raft is one member of a Raft cluster.
type Raft struct {
	mu        sync.Mutex
	peers     []Peer
	me        int
	persister Persister
	applyCh   chan<- ApplyMsg
	dead      int32

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	role           Role
	lastHeartbeat  time.Time
	electionTimeout time.Duration

	lastIncludedIndex int
	lastIncludedTerm  int

	applyCond *sync.Cond
}

// Make creates a new Raft peer at index me within peers.
func Make(peers []Peer, me int, persister Persister, applyCh chan<- ApplyMsg) *Raft {
	rf := &Raft{
		peers:           peers,
		me:              me,
		persister:       persister,
		applyCh:         applyCh,
		role:            Follower,
		votedFor:        -1,
		log:             []LogEntry{{Term: 0, Index: 0}},
		nextIndex:       make([]int, len(peers)),
		matchIndex:      make([]int, len(peers)),
		lastHeartbeat:   time.Now(),
		electionTimeout: randElectionTimeout(),
	}
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.readPersist(persister.ReadState())

	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
	}

	go rf.ticker()
	go rf.applier()

	return rf
}

func randElectionTimeout() time.Duration {
	return electionTimeoutMin + time.Duration(rand.Int63n(int64(electionTimeoutMax-electionTimeoutMin)))
}

// GetState returns the current term and whether this peer believes it is leader.
func (rf *Raft) GetState() (term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

// Start submits a command to the replicated log.
func (rf *Raft) Start(command any) (index int, term int, isLeader bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.role != Leader {
		return -1, rf.currentTerm, false
	}

	idx := rf.lastLogIndex() + 1
	entry := LogEntry{
		Term:    rf.currentTerm,
		Index:   idx,
		Command: command,
	}
	rf.log = append(rf.log, entry)
	rf.persist()

	go rf.broadcastAppendEntries()

	return entry.Index, entry.Term, true
}

// Snapshot tells Raft that the service has created a snapshot.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.lastIncludedIndex {
		return
	}
	if index > rf.lastLogIndex() {
		return
	}

	term := rf.termAt(index)
	newLog := []LogEntry{{Term: term, Index: index}}
	for i := index + 1; i <= rf.lastLogIndex(); i++ {
		newLog = append(newLog, rf.entryAt(i))
	}
	rf.log = newLog
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = term
	if rf.lastApplied < index {
		rf.lastApplied = index
	}
	if rf.commitIndex < index {
		rf.commitIndex = index
	}
	rf.persistWithSnapshot(snapshot)
}

// Kill marks this Raft instance as dead.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	rf.mu.Lock()
	rf.applyCond.Broadcast()
	rf.mu.Unlock()
}

func (rf *Raft) killed() bool {
	return atomic.LoadInt32(&rf.dead) == 1
}

// ----- log accessors -----

func (rf *Raft) lastLogIndex() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	return rf.log[len(rf.log)-1].Index
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) entryAt(index int) LogEntry {
	if index == rf.lastIncludedIndex {
		return LogEntry{Term: rf.lastIncludedTerm, Index: rf.lastIncludedIndex}
	}
	off := index - rf.lastIncludedIndex
	if off < 0 || off >= len(rf.log) {
		return LogEntry{}
	}
	return rf.log[off]
}

func (rf *Raft) termAt(index int) int {
	if index == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	off := index - rf.lastIncludedIndex
	if off < 0 || off >= len(rf.log) {
		return -1
	}
	return rf.log[off].Term
}

func (rf *Raft) hasIndex(index int) bool {
	return index >= rf.lastIncludedIndex && index <= rf.lastLogIndex()
}

// ----- persistence -----

type persistedState struct {
	CurrentTerm       int
	VotedFor          int
	Log               []LogEntry
	LastIncludedIndex int
	LastIncludedTerm  int
}

func (rf *Raft) encodeState() []byte {
	w := new(bytes.Buffer)
	enc := gob.NewEncoder(w)
	st := persistedState{
		CurrentTerm:       rf.currentTerm,
		VotedFor:          rf.votedFor,
		Log:               rf.log,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
	}
	_ = enc.Encode(st)
	return w.Bytes()
}

func (rf *Raft) persist() {
	rf.persister.SaveState(rf.encodeState())
}

func (rf *Raft) persistWithSnapshot(snapshot []byte) {
	rf.persister.SaveStateAndSnapshot(rf.encodeState(), snapshot)
}

func (rf *Raft) readPersist(data []byte) {
	if len(data) == 0 {
		return
	}
	r := bytes.NewBuffer(data)
	dec := gob.NewDecoder(r)
	var st persistedState
	if err := dec.Decode(&st); err != nil {
		return
	}
	rf.currentTerm = st.CurrentTerm
	rf.votedFor = st.VotedFor
	rf.log = st.Log
	rf.lastIncludedIndex = st.LastIncludedIndex
	rf.lastIncludedTerm = st.LastIncludedTerm
	if len(rf.log) == 0 {
		rf.log = []LogEntry{{Term: rf.lastIncludedTerm, Index: rf.lastIncludedIndex}}
	}
}

// PersistStateSize returns the size of the persisted Raft state.
func (rf *Raft) PersistStateSize() int {
	return rf.persister.StateSize()
}

// ----- ticker / election -----

func (rf *Raft) ticker() {
	for !rf.killed() {
		time.Sleep(20 * time.Millisecond)
		rf.mu.Lock()
		if rf.role == Leader {
			if time.Since(rf.lastHeartbeat) >= heartbeatInterval {
				rf.lastHeartbeat = time.Now()
				rf.mu.Unlock()
				rf.broadcastAppendEntries()
				continue
			}
		} else {
			if time.Since(rf.lastHeartbeat) >= rf.electionTimeout {
				rf.startElection()
				rf.mu.Unlock()
				continue
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) becomeFollower(term int) {
	rf.role = Follower
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.persist()
	}
}

func (rf *Raft) becomeLeader() {
	rf.role = Leader
	for i := range rf.peers {
		rf.nextIndex[i] = rf.lastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.lastLogIndex()
	rf.lastHeartbeat = time.Now()
}

func (rf *Raft) startElection() {
	rf.role = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastHeartbeat = time.Now()
	rf.electionTimeout = randElectionTimeout()
	rf.persist()

	term := rf.currentTerm
	lastIdx := rf.lastLogIndex()
	lastTerm := rf.lastLogTerm()
	votes := int32(1)
	totalPeers := len(rf.peers)

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			args := &RequestVoteArgs{
				Term:         term,
				CandidateID:  rf.me,
				LastLogIndex: lastIdx,
				LastLogTerm:  lastTerm,
			}
			reply := &RequestVoteReply{}
			if !rf.peers[server].Call("Raft.RequestVote", args, reply) {
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if rf.currentTerm != term || rf.role != Candidate {
				return
			}
			if reply.Term > rf.currentTerm {
				rf.becomeFollower(reply.Term)
				return
			}
			if reply.VoteGranted {
				v := atomic.AddInt32(&votes, 1)
				if int(v) > totalPeers/2 && rf.role == Candidate {
					rf.becomeLeader()
					go rf.broadcastAppendEntries()
				}
			}
		}(i)
	}
}

// ----- replication -----

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	rf.lastHeartbeat = time.Now()
	term := rf.currentTerm
	rf.mu.Unlock()

	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.replicateOne(i, term)
	}
}

func (rf *Raft) replicateOne(server, term int) {
	rf.mu.Lock()
	if rf.role != Leader || rf.currentTerm != term {
		rf.mu.Unlock()
		return
	}
	next := rf.nextIndex[server]
	if next <= rf.lastIncludedIndex {
		args := &InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderID:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()
		reply := &InstallSnapshotReply{}
		if !rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
			return
		}
		rf.mu.Lock()
		defer rf.mu.Unlock()
		if rf.role != Leader || rf.currentTerm != term {
			return
		}
		if reply.Term > rf.currentTerm {
			rf.becomeFollower(reply.Term)
			return
		}
		if args.LastIncludedIndex > rf.matchIndex[server] {
			rf.matchIndex[server] = args.LastIncludedIndex
			rf.nextIndex[server] = args.LastIncludedIndex + 1
		}
		return
	}

	prevIdx := next - 1
	prevTerm := rf.termAt(prevIdx)
	entries := []LogEntry{}
	for i := next; i <= rf.lastLogIndex(); i++ {
		entries = append(entries, rf.entryAt(i))
	}
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderID:     rf.me,
		PrevLogIndex: prevIdx,
		PrevLogTerm:  prevTerm,
		Entries:      entries,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := &AppendEntriesReply{}
	if !rf.peers[server].Call("Raft.AppendEntries", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader || rf.currentTerm != term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.becomeFollower(reply.Term)
		return
	}
	if reply.Success {
		newMatch := args.PrevLogIndex + len(args.Entries)
		if newMatch > rf.matchIndex[server] {
			rf.matchIndex[server] = newMatch
			rf.nextIndex[server] = newMatch + 1
		}
		rf.advanceCommitIndex()
	} else {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}
		} else {
			lastIdxForTerm := -1
			for i := rf.lastLogIndex(); i > rf.lastIncludedIndex; i-- {
				if rf.termAt(i) == reply.ConflictTerm {
					lastIdxForTerm = i
					break
				}
			}
			if lastIdxForTerm >= 0 {
				rf.nextIndex[server] = lastIdxForTerm + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
			if rf.nextIndex[server] < 1 {
				rf.nextIndex[server] = 1
			}
		}
	}
}

func (rf *Raft) advanceCommitIndex() {
	if rf.role != Leader {
		return
	}
	matches := make([]int, len(rf.peers))
	copy(matches, rf.matchIndex)
	matches[rf.me] = rf.lastLogIndex()
	sorted := append([]int(nil), matches...)
	sort.Ints(sorted)
	majority := sorted[(len(sorted)-1)/2]
	if majority > rf.commitIndex && rf.termAt(majority) == rf.currentTerm {
		rf.commitIndex = majority
		rf.applyCond.Broadcast()
	}
}

// ----- applier -----

func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()
		for !rf.killed() && rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		if rf.killed() {
			rf.mu.Unlock()
			return
		}

		toApply := []ApplyMsg{}
		for rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex {
				continue
			}
			e := rf.entryAt(rf.lastApplied)
			if e.Command == nil && e.Index == 0 {
				continue
			}
			toApply = append(toApply, ApplyMsg{
				CommandValid: true,
				Command:      e.Command,
				CommandIndex: e.Index,
				CommandTerm:  e.Term,
			})
		}
		rf.mu.Unlock()

		for _, m := range toApply {
			if rf.killed() {
				return
			}
			rf.applyCh <- m
		}
	}
}
