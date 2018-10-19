package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"labgob"
	"labrpc"
	"math/rand"
	"sort"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

const (
	LEADER   = 0
	CANDIATE = 1
	FOLLOWER = 2
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile stat on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state              int // leader, candiate or follower
	resetElectionTimer chan int
	votes              int

	applyCh chan ApplyMsg // for test
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isleader = rf.state == LEADER

	DPrintf("%d GetState result.term:%d, state:%d, isLeader:%t", rf.me, term, rf.state, isleader)

	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currenTerm int
	var votedFor int
	var log []LogEntry
	if d.Decode(&currenTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		DPrintf("%d decode state failed", rf.me)
		return
	} else {
		rf.currentTerm = currenTerm
		rf.votedFor = votedFor
		rf.log = log
	}
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate's term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate's last log entry
	LastLogTerm  int // term of candidate's last log entry
}

func (req *RequestVoteArgs) String() string {
	return fmt.Sprintf("{term:%d, candidateId:%d, lastLogIndex:%d, LastLogTerm:%d}",
		req.Term, req.CandidateId, req.LastLogIndex, req.LastLogTerm)
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

func (reply *RequestVoteReply) String() string {
	return fmt.Sprintf("{Term:%d, VoteGranted:%t}", reply.Term, reply.VoteGranted)
}

//
// AppendEntries RPC structure
//
type AppendEntriesArgs struct {
	Term         int        // leader's term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store(empty for heatbeat; may send more than one for efficiency)
	LeaderCommit int        // leader's commitIndex
}

func (req *AppendEntriesArgs) String() string {
	return fmt.Sprintf("{Term:%d, Leaderid:%d, PrevLogIndex;%d, PrevLogTerm:%d, Entries:%v, LeaderCommit:%d}",
		req.Term, req.LeaderId, req.PrevLogIndex, req.PrevLogTerm, req.Entries, req.LeaderCommit)
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm

	// for leader quickly back up nextIndex
	ConflictTerm      int
	ConflictTermStart int
}

func (reply *AppendEntriesReply) String() string {
	return fmt.Sprintf("{Term:%d, Success:%t}", reply.Term, reply.Success)
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	if args == nil {
		DPrintf("%d ignore nil RequestVote", rf.me)
		return
	}

	// Your code here (2A, 2B).
	DPrintf("%d receive request vote:%s", rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		DPrintf("%d receive stale request vote,currentTerm:%d, term:%d,candidateId:%d",
			rf.me, rf.currentTerm, args.Term, args.CandidateId)
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	shouldPersist := new(bool)
	*shouldPersist = false
	defer persistState(shouldPersist, rf)

	if args.Term > rf.currentTerm {
		DPrintf("%d receive request vote term %d > current term %d, candidateId:%d, become follower",
			rf.me, args.Term, rf.currentTerm, args.CandidateId)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		*shouldPersist = true
	}

	// same term
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	vote := false
	if rf.votedFor < 0 || rf.votedFor == args.CandidateId {
		if (args.LastLogTerm > lastLogTerm) || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex) {
			vote = true
			rf.votedFor = args.CandidateId
			*shouldPersist = true
			DPrintf("%d vote for candidate %d at term:%d", rf.me, args.CandidateId, args.Term)
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = vote

	if vote {
		rf.resetElection()
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	if args == nil {
		DPrintf("%d ignore nil AppendEntries", rf.me)
		return
	}

	DPrintf("%d receive append entries.req:%s", rf.me, args)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf("%d receive stale append entries term %d < currentTerm:%d and return false", rf.me, args.Term, rf.currentTerm)
		return
	}

	defer rf.resetElection()

	shouldPersist := new(bool)
	*shouldPersist = false
	defer persistState(shouldPersist, rf)

	if args.Term > rf.currentTerm {
		DPrintf("%d receive append entries term %d > currentTerm:%d and become follower", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.state = FOLLOWER
		*shouldPersist = true
	}
	rf.votedFor = args.LeaderId
	*shouldPersist = true

	reply.Term = rf.currentTerm

	lastLogIndex := len(rf.log) - 1
	if lastLogIndex < args.PrevLogIndex {
		reply.Success = false

		// next entry is unknown
		reply.ConflictTerm = 0 // unknown term
		reply.ConflictTermStart = lastLogIndex + 1

		DPrintf("%d receive append entries and return false.PrevLogIndex %d > lastLogIndex:%d",
			rf.me, args.PrevLogIndex, lastLogIndex)
		return
	}

	if args.PrevLogIndex >= 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false

		// found the start index of conflict term
		reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for ; i >= 0; i-- {
			if rf.log[i].Term != reply.ConflictTerm {
				break
			}
		}
		reply.ConflictTermStart = Max(i, 0)

		DPrintf("%d receive append entries and return false.PrevLogTerm %d != args.PrevLogTerm:%d",
			rf.me, rf.log[args.PrevLogIndex].Term, args.PrevLogTerm)
		return
	}
	// compare to find conflict index
	elen := len(args.Entries)
	if elen > 0 {
		// exists entries
		len := Min(lastLogIndex-args.PrevLogIndex, elen)
		i := 0
		for ; i < len; i++ {
			// confict
			if rf.log[i+args.PrevLogIndex+1].Term != args.Entries[i].Term {
				// trim follow
				DPrintf("%d conflict with leader at index %d.log:%v", rf.me, i+args.PrevLogIndex+1, rf.log)
				rf.log = rf.log[0 : i+args.PrevLogIndex+1]
				*shouldPersist = true
				DPrintf("%d solve conflict.log:%v", rf.me, rf.log)
				break
			}
		}
		for ; i < elen; i++ {
			rf.log = append(rf.log, args.Entries[i])
			*shouldPersist = true
		}
	}

	reply.Success = true

	// no conflict entry
	reply.ConflictTerm = -1
	reply.ConflictTermStart = -1

	DPrintf("%d receive append entries and return true.args:%s", rf.me, args)

	// commit index
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
	}
	for rf.commitIndex > rf.lastApplied {
		rf.lastApplied += 1
		// TODO: apply log to state machine
		// For tester
		rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied + 1}
	}
	// test
	DPrintf("%d log=%v, commitIndex:%d", rf.me, rf.log, rf.commitIndex)
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	isLeader = rf.state == LEADER
	term = rf.currentTerm

	if !isLeader {
		DPrintf("AtStart %d is not leader, term:%d", rf.me, term)
		return index, term, isLeader
	}
	DPrintf("AtStart %d is leader, term:%d, begin sync msg", rf.me, term)

	rf.log = append(rf.log, LogEntry{term, command})
	rf.persist()

	index = len(rf.log) // the paper's index starts from 1

	// sync to peers
	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go doAppendEntries(idx, rf)
	}
	return index, term, isLeader
}

func (rf *Raft) resetElection() {
	go func() {
		rf.resetElectionTimer <- 1
	}()
}

func (rf *Raft) startElectionRoutine() {
	go func() {
		lastReset := time.Now().UnixNano()
		for {
			duration := time.Duration(500+rand.Int31n(200)) * time.Millisecond
			elapseTimer := time.After(duration)
			// FIMXE: not optimal for leader
			select {
			case <-rf.resetElectionTimer:
				lastReset = time.Now().UnixNano()
				DPrintf("%d reset election timer,lastReset:%d", rf.me, lastReset)

			case <-elapseTimer:
				now := time.Now().UnixNano()
				// timeout
				rf.mu.Lock()

				lastLogIndex := len(rf.log) - 1 // start from 0
				lastLogTerm := 0
				if lastLogIndex >= 0 {
					lastLogTerm = rf.log[lastLogIndex].Term
				}
				if rf.state == FOLLOWER {
					DPrintf("%d follower election timeout.Time:%d,now:%d, ms:%d, term:%d",
						rf.me, lastReset, now, (now-lastReset)/1000000, rf.currentTerm)

					rf.state = CANDIATE
					rf.votedFor = rf.me // vote for itself
					rf.votes = 1

					rf.currentTerm += 1
					rf.persist()

					go doElection(rf.currentTerm, lastLogIndex, lastLogTerm, rf)
				} else if rf.state == CANDIATE {
					DPrintf("%d candidate election timeout.Time:%d,now:%d, ms:%d, term:%d",
						rf.me, lastReset, now, (now-lastReset)/1000000, rf.currentTerm)

					rf.currentTerm += 1 // start a new election
					rf.persist()

					rf.votes = 1
					go doElection(rf.currentTerm, lastLogIndex, lastLogTerm, rf)
				}

				rf.mu.Unlock()
			}
		}
	}()
}

func (rf *Raft) startHeartbeat() {
	go func() {
		for {
			elapseTimer := time.After(time.Duration(120) * time.Millisecond)
			// FIMXE: not optimal for follower or candidate
			select {
			case <-elapseTimer:
				rf.mu.Lock()
				if rf.state == LEADER {
					DPrintf("leader %d send heatbeat", rf.me)
					// heartbeat
					for idx := range rf.peers {
						if idx == rf.me {
							continue
						}
						go doAppendEntries(idx, rf)
					}

					// test
					DPrintf("%d log:%v, term:%d, commitIndex=%d", rf.me, rf.log, rf.currentTerm, rf.commitIndex)
				}
				rf.mu.Unlock()
			}
		}
	}()
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func doElection(currentTerm, lastLogIndex, lastLogTerm int, rf *Raft) {
	req := &RequestVoteArgs{
		Term:         currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(peerIdx int) {
			reply := new(RequestVoteReply)
			if rf.sendRequestVote(peerIdx, req, reply) {
				rf.handleRequestVoteReply(peerIdx, req, reply)
			} else {
				DPrintf("%d send request to peer %d failed", rf.me, peerIdx)
			}
		}(idx)
	}
}

func (rf *Raft) handleRequestVoteReply(peerIdx int, req *RequestVoteArgs, reply *RequestVoteReply) {
	if reply == nil {
		DPrintf("%d ignore nil RequestVoteReply", rf.me)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	shouldPersist := new(bool)
	*shouldPersist = false
	defer persistState(shouldPersist, rf)

	DPrintf("%d handleRequestVoteReply, current state:%d, req:%s", rf.me, rf.state, req)

	if reply.Term > rf.currentTerm {
		DPrintf("%d found request vote from %d term %d > currentTerm %d, become follower",
			rf.me, peerIdx, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER // change to follower
		*shouldPersist = true
	} else if reply.Term == rf.currentTerm && rf.state == CANDIATE {
		if reply.VoteGranted {
			rf.votes += 1
			DPrintf("%d receive vote from %d", rf.me, peerIdx)
			if rf.votes >= len(rf.peers)/2+1 {
				DPrintf("%d become leader", rf.me)
				// become leader
				rf.state = LEADER

				// reset matchIndex and nextIndex
				for i := 0; i < len(rf.peers); i++ {
					rf.matchIndex[i] = -1
					rf.nextIndex[i] = len(rf.log)
				}

				for idx := range rf.peers {
					if idx == rf.me {
						continue
					}
					go doAppendEntries(idx, rf)
				}
			}
		} else {
			DPrintf("%d not receive vote from %d", rf.me, peerIdx)
		}
	} else {
		DPrintf("%d not receive stale vote from %d", rf.me, peerIdx)
	}
}

func doAppendEntries(peerIdx int, rf *Raft) {
	DPrintf("%d doAppendEntries. peerIdx:%d", rf.me, peerIdx)

	msg := rf.makeAppendEntry(peerIdx)
	reply := new(AppendEntriesReply)
	if !rf.sendAppendEntries(peerIdx, msg, reply) {
		DPrintf("%d send append entries to peer %d failed", rf.me, peerIdx)
		return
	}
	if reply == nil {
		DPrintf("%d ignore nil append entries to peerIdx:%d", rf.me, peerIdx)
		return
	}

	DPrintf("%d send append entries to peer %d ok", rf.me, peerIdx)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	shouldPersist := new(bool)
	*shouldPersist = false
	defer persistState(shouldPersist, rf)

	if reply.Term > rf.currentTerm {
		DPrintf("%d currentTerm %d < reply.Term:%d, change to follower", rf.me, rf.currentTerm, reply.Term)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER // change to follower
		*shouldPersist = true
	} else if reply.Term == rf.currentTerm && rf.state == LEADER {
		if reply.Success {
			rf.matchIndex[peerIdx] = msg.PrevLogIndex + len(msg.Entries)
			rf.nextIndex[peerIdx] = rf.matchIndex[peerIdx] + 1

			DPrintf("%d update peerIdx %d matchIdx:%d, nextIdx:%d", rf.me, peerIdx, rf.matchIndex[peerIdx], rf.nextIndex[peerIdx])

			// infer commit index
			sortedMatchIndex := make([]int, len(rf.peers)-1)
			j := 0
			for i := 0; i < len(rf.peers); i++ {
				if rf.me == i {
					continue
				}
				sortedMatchIndex[j] = i
				j += 1
			}
			sort.Slice(sortedMatchIndex, func(i, j int) bool {
				return rf.matchIndex[sortedMatchIndex[i]] > rf.matchIndex[sortedMatchIndex[j]]
			})

			// find majority matchIndex N > commitIndex, where log[N].term == currentTerm
			mid := rf.matchIndex[sortedMatchIndex[len(sortedMatchIndex)/2-1]]
			DPrintf("%d sortedMatchIndex:%v, matchIdx:%v, mid:%d", rf.me, sortedMatchIndex, rf.matchIndex, mid)
			if mid >= 0 && mid <= len(rf.log)-1 {
				for i := mid; i > rf.commitIndex; i-- {
					if rf.log[i].Term == rf.currentTerm {
						// found new commitIndex
						DPrintf("%d found new commitIndex:%d, origCommitIndex:%d, term:%d",
							rf.me, i, rf.commitIndex, rf.currentTerm)
						rf.commitIndex = i
						break
					}
				}
			}
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied += 1
				// TODO: apply logs to state machine

				// For tester
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.log[rf.lastApplied].Command, CommandIndex: rf.lastApplied + 1}
			}
		} else {
			DPrintf("%d found peer %d inconsiste follower,try again with nextIndex:%d",
				rf.me, peerIdx, rf.nextIndex[peerIdx])

			logLen := len(rf.log)
			if reply.ConflictTerm >= 0 && reply.ConflictTermStart >= 0 {
				// infer nextIndex
				if reply.ConflictTerm == 0 { // PrevLogIndex > peer log length
					if reply.ConflictTermStart < logLen {
						rf.nextIndex[peerIdx] = reply.ConflictTermStart
					} else {
						rf.nextIndex[peerIdx] = logLen
					}
				} else {
					i := reply.ConflictTermStart
					for ; i < logLen; i++ {
						if rf.log[i].Term != reply.ConflictTerm {
							break
						}
					}
					rf.nextIndex[peerIdx] = i
				}
				DPrintf("%d infer nextIndex for peer %d, nextIndex:%d", rf.me, peerIdx, rf.nextIndex[peerIdx])
			} else {
				if rf.nextIndex[peerIdx] > 0 {
					rf.nextIndex[peerIdx] -= 1
				}
			}

			// try again
			go doAppendEntries(peerIdx, rf)
		}
	} else {
		DPrintf("%d receive stale AppendEntriesReply:%s, term:%d, state:%d", rf.me, reply, rf.currentTerm, rf.state)
	}
}

func (rf *Raft) makeAppendEntry(peerIdx int) *AppendEntriesArgs {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	nextIdx := rf.nextIndex[peerIdx]
	lastLogIdx := len(rf.log) - 1

	var entries []LogEntry
	prevLogIndex := -1
	prevLogTerm := 0
	if nextIdx > 0 {
		prevLogIndex = nextIdx - 1
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	if lastLogIdx >= nextIdx {
		entries = rf.log[nextIdx : lastLogIdx+1]
	}

	req := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
		Entries:      entries,
	}

	DPrintf("%d make append entry to peer %d.req:%s",
		rf.me, peerIdx, req)

	return req
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	// Your initialization code here (2A, 2B, 2C).
	rf.votedFor = -1
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.matchIndex = make([]int, len(peers))
	rf.nextIndex = make([]int, len(peers))
	rf.resetElectionTimer = make(chan int)
	rf.currentTerm = 0
	rf.votes = 0
	rf.state = FOLLOWER

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// for follower, candiate
	rf.startElectionRoutine()
	// for leader
	rf.startHeartbeat()

	return rf
}

func persistState(shouldPersist *bool, rf *Raft) {
	if *shouldPersist {
		DPrintf("%d persist state", rf.me)
		rf.persist()
	}
}
