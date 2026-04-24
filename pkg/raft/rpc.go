package raft

// RequestVoteArgs is sent by a candidate to solicit a vote.
type RequestVoteArgs struct {
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply is the response to a RequestVote RPC.
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// AppendEntriesArgs is sent by the leader to replicate log entries.
type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

// AppendEntriesReply carries the follower's response plus fast-backtrack hints.
type AppendEntriesReply struct {
	Term    int
	Success bool

	ConflictTerm  int
	ConflictIndex int
}

// InstallSnapshotArgs transfers a snapshot from leader to a lagging follower.
type InstallSnapshotArgs struct {
	Term              int
	LeaderID          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

// InstallSnapshotReply is the follower's response.
type InstallSnapshotReply struct {
	Term int
}

// RequestVote handles a candidate's vote solicitation.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	}

	upToDate := false
	myLastTerm := rf.lastLogTerm()
	myLastIdx := rf.lastLogIndex()
	if args.LastLogTerm > myLastTerm {
		upToDate = true
	} else if args.LastLogTerm == myLastTerm && args.LastLogIndex >= myLastIdx {
		upToDate = true
	}

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) && upToDate {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		rf.lastHeartbeat = nowFn()
		rf.electionTimeout = randElectionTimeout()
		rf.persist()
	}
}

// AppendEntries handles replication and heartbeat from the leader.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictTerm = -1
	reply.ConflictIndex = -1

	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	} else if rf.role == Candidate {
		rf.role = Follower
	}

	rf.lastHeartbeat = nowFn()
	rf.electionTimeout = randElectionTimeout()

	if args.PrevLogIndex < rf.lastIncludedIndex {
		shift := rf.lastIncludedIndex - args.PrevLogIndex
		if shift > len(args.Entries) {
			reply.Success = true
			return
		}
		args.PrevLogIndex = rf.lastIncludedIndex
		args.PrevLogTerm = rf.lastIncludedTerm
		args.Entries = args.Entries[shift:]
	}

	if args.PrevLogIndex > rf.lastLogIndex() {
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.lastLogIndex() + 1
		return
	}

	if rf.termAt(args.PrevLogIndex) != args.PrevLogTerm {
		conflictTerm := rf.termAt(args.PrevLogIndex)
		reply.ConflictTerm = conflictTerm
		idx := args.PrevLogIndex
		for idx > rf.lastIncludedIndex && rf.termAt(idx-1) == conflictTerm {
			idx--
		}
		reply.ConflictIndex = idx
		return
	}

	for i, e := range args.Entries {
		idx := args.PrevLogIndex + 1 + i
		if idx > rf.lastLogIndex() {
			rf.log = append(rf.log, args.Entries[i:]...)
			break
		}
		if rf.termAt(idx) != e.Term {
			off := idx - rf.lastIncludedIndex
			rf.log = append(rf.log[:off], args.Entries[i:]...)
			break
		}
	}
	rf.persist()

	if args.LeaderCommit > rf.commitIndex {
		newCommit := args.LeaderCommit
		if newCommit > rf.lastLogIndex() {
			newCommit = rf.lastLogIndex()
		}
		if newCommit > rf.commitIndex {
			rf.commitIndex = newCommit
			rf.applyCond.Broadcast()
		}
	}

	reply.Success = true
}

// InstallSnapshot handles a snapshot transfer from the leader.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		rf.mu.Unlock()
		return
	}
	if args.Term > rf.currentTerm {
		rf.becomeFollower(args.Term)
		reply.Term = rf.currentTerm
	} else if rf.role == Candidate {
		rf.role = Follower
	}
	rf.lastHeartbeat = nowFn()
	rf.electionTimeout = randElectionTimeout()

	if args.LastIncludedIndex <= rf.lastIncludedIndex {
		rf.mu.Unlock()
		return
	}

	if args.LastIncludedIndex <= rf.lastLogIndex() && rf.termAt(args.LastIncludedIndex) == args.LastIncludedTerm {
		off := args.LastIncludedIndex - rf.lastIncludedIndex
		rf.log = append([]LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}, rf.log[off+1:]...)
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}
	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}
	rf.persistWithSnapshot(args.Data)

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.Unlock()

	rf.applyCh <- msg
}
