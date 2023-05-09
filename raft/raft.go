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
//	 Should return immediately, so start a goroutine if necessary. Differs from Raft paper.
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"bytes"
	"log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"cs426.yale.edu/final/labgob"
	"cs426.yale.edu/final/labrpc"

	"fmt"
)

type Role int

const (
	Follower  Role = 0
	Candidate Role = 1
	Leader    Role = 2

	ElectionTimeout  = 500 * time.Millisecond
	RPCTimeout       = 100 * time.Millisecond
	HeartbeatTimeout = 150 * time.Millisecond
	ApplyTime        = 100 * time.Millisecond
)

type LogEntry struct {
	Term    int
	Command interface{}
}

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	role        Role
	votedFor    int
	logEntries  []LogEntry

	// Volatile state on all servers
	commitIndex   int
	lastApplied   int
	lockStr       string
	applyCh       chan ApplyMsg
	stopChannel   chan struct{}
	signalApplyCh chan struct{}

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int // index of highest log entry known to be replicated on server

	electionTimer       *time.Timer
	appendEntriesTimers []*time.Timer
	applyTimer          *time.Timer
	DebugFlag           bool
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	rf.lock("GetState")
	defer rf.unlock("GetState")
	return rf.currentTerm, rf.role == Leader
}

func (rf *Raft) lock(s string) {
	rf.mu.Lock()
	rf.lockStr = s
}

func (rf *Raft) unlock(s string) {
	rf.lockStr = ""
	rf.mu.Unlock()
}

func (rf *Raft) changeRole(role Role) {
	rf.role = role
	switch role {
	case Leader:
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		_, lastLogIndex := rf.getLastLogTermIndex()
		for i, _ := range rf.peers {
			rf.nextIndex[i] = lastLogIndex + 1
			// rf.matchIndex[i] = 0
		}
		rf.matchIndex[rf.me] = lastLogIndex
		rf.resetElectionTimer()

	case Follower:
	case Candidate:
		rf.currentTerm++
		rf.votedFor = rf.me
		rf.resetElectionTimer()

	default:
		panic("unknown role")
	}

}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.logEntries)
	e.Encode(rf.commitIndex)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, votedFor, commitIndex int
	var logEntries []LogEntry

	if d.Decode(&term) != nil || d.Decode(&votedFor) != nil || d.Decode(&logEntries) != nil || d.Decode(&commitIndex) != nil {
		log.Fatalf("decode error")
	} else {
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logEntries = logEntries
		rf.commitIndex = commitIndex
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.lock("RequestVote")
	defer rf.unlock("RequestVote")
	defer func() {
		rf.delog("RequestVote: args: %+v, reply: %+v", args, reply)
	}()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()

	if args.Term < rf.currentTerm {
		return
	} else if args.Term == rf.currentTerm {
		if rf.role == Leader {
			return
		}
		if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
			return
			// Already voted for someone else
		}
		if rf.votedFor == args.CandidateId {
			reply.VoteGranted = true
			return
		}
	}
	defer rf.persist()
	if args.Term > rf.currentTerm {
		// Accept leader and change myself to follower
		rf.changeRole(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	if args.LastLogTerm < lastLogTerm || (args.LastLogTerm == lastLogTerm && lastLogIndex > args.LastLogIndex) {
		return
	}
	rf.currentTerm = args.Term
	rf.votedFor = args.CandidateId
	reply.VoteGranted = true
	rf.changeRole(Follower)
	rf.delog("Voted for %d", args.CandidateId)
	rf.resetElectionTimer()
	return

}

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	// Fills in reply??
	timer := time.NewTimer(RPCTimeout)
	defer timer.Stop()
	rpcTimer := time.NewTimer(RPCTimeout)
	defer rpcTimer.Stop()

	for {
		rpcTimer.Stop()
		rpcTimer.Reset(RPCTimeout)
		ch := make(chan bool, 1)
		reply_to_fill := RequestVoteReply{}

		go func() {
			ok := rf.peers[server].Call("Raft.RequestVote", args, &reply_to_fill)
			if !ok {
				time.Sleep(10 * time.Millisecond)
			}
			ch <- ok
		}()

		select {
		case <-timer.C:
			return false
		case <-rpcTimer.C:
			continue
		case ok := <-ch:
			if ok {
				reply.Term = reply_to_fill.Term
				reply.VoteGranted = reply_to_fill.VoteGranted
				return true
			} else {
				continue
			}
		}
	}
}

func (rf *Raft) startElection() {
	rf.lock("startElection")
	rf.electionTimer.Reset(getRandomElectionTimeout())
	if rf.role == Leader {
		rf.unlock("startElection")
		return
	}
	rf.delog("Start Election")
	rf.changeRole(Candidate)
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	rf.persist()
	rf.unlock("startElection")

	voteCount := 1
	chResultCount := 1
	voteChannel := make(chan bool, len(rf.peers))
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go func(ch chan bool, server_ind int) {
			reply := RequestVoteReply{}
			rf.sendRequestVote(server_ind, &args, &reply)
			ch <- reply.VoteGranted
			if reply.Term > args.Term {
				rf.lock("start-election-term")
				// extra check
				if rf.currentTerm < reply.Term {
					rf.currentTerm = reply.Term
					rf.changeRole(Follower)
					rf.resetElectionTimer()
					rf.persist()
				}
				rf.unlock("start-election-term")
			}

		}(voteChannel, i)
	}

	for {
		vote := <-voteChannel
		chResultCount++
		if vote {
			voteCount++
		}
		// Good enough
		if chResultCount == len(rf.peers) || voteCount > len(rf.peers)/2 || chResultCount-voteCount > len(rf.peers)/2 {
			break
		}
	}

	if voteCount <= len(rf.peers)/2 {
		rf.delog("Not enough votes, Vote count: %d", voteCount)
		return
	}
	rf.lock("start-election-term-2")
	rf.delog("Changing to leader, Vote count: %d", voteCount)
	if rf.currentTerm == args.Term && rf.role == Candidate && voteCount > len(rf.peers)/2 {
		rf.changeRole(Leader)
		rf.resetElectionTimer() // Check this
		rf.persist()
	}
	if rf.role == Leader {
		rf.resetAllHeartbeatTimer()
	}
	rf.unlock("start-election-term-2")
}

func (rf *Raft) getLastLogTermIndex() (int, int) {
	if len(rf.logEntries) == 0 {
		return 0, 0
	}
	lastLog := rf.logEntries[len(rf.logEntries)-1]
	index := len(rf.logEntries) - 1
	return lastLog.Term, index
}

func (rf *Raft) getIdxByLogIndex(logIndex int) int {
	res := logIndex
	if res < 0 {
		return -1
	}
	return res
}

func (rf *Raft) getLogEntryByIndex(logIndex int) LogEntry {
	return rf.logEntries[logIndex]
}

func (rf *Raft) IsLeader() bool {
	rf.lock("Start")
	defer rf.unlock("Start")
	return rf.role == Leader
}

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
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (3B).
	rf.lock("Start")
	term := rf.currentTerm
	isLeader := rf.role == Leader
	_, index := rf.getLastLogTermIndex()
	index++

	if isLeader {
		rf.logEntries = append(rf.logEntries, LogEntry{
			Term:    rf.currentTerm,
			Command: command,
		})
		rf.matchIndex[rf.me] = index
		rf.persist()
	}
	rf.resetAllHeartbeatTimer()
	rf.unlock("Start")

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {

	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		select {
		case <-rf.electionTimer.C:
			rf.startElection()
		case <-rf.stopChannel:
			return
		}
	}
}

func (rf *Raft) applyLogs() {
	defer rf.applyTimer.Reset(ApplyTime)
	rf.lock("applyLogs-1")
	var messages []ApplyMsg
	if rf.lastApplied >= rf.commitIndex {
		// commit index is not updated yet ??
		messages = make([]ApplyMsg, 0)
	} else {
		rf.delog("Applying logs")
		messages = make([]ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
			messages = append(messages, ApplyMsg{
				CommandValid: true,
				Command:      rf.logEntries[rf.getIdxByLogIndex(i)].Command,
				CommandIndex: i,
			})
		}
	}
	rf.unlock("applyLogs-1")

	for _, msg := range messages {
		rf.applyCh <- msg
		rf.lock("applyLogs-2")
		rf.lastApplied = msg.CommandIndex
		rf.delog("Applied log index: %d", rf.lastApplied)
		rf.unlock("applyLogs-2")
	}
}

func (rf *Raft) resetElectionTimer() {
	rf.electionTimer.Stop()
	rf.electionTimer.Reset(getRandomElectionTimeout())
}

func getRandomElectionTimeout() time.Duration {
	return time.Duration(rand.Int63())%ElectionTimeout + ElectionTimeout
}

func (rf *Raft) delog(content string, a ...interface{}) {
	if !rf.DebugFlag {
		return
	}
	t, id := rf.getLastLogTermIndex()
	s := fmt.Sprintf("Raft me: %d, role:%v, term:%d, commit_index:%d, lastlogterm:%d, lastlogidx: %d, next_indices:%+v", rf.me, rf.role, rf.currentTerm, rf.commitIndex, t, id, rf.nextIndex)
	c := fmt.Sprintf(content, a...)
	log.Printf("%s DebugLog: %s", s, c)
	// Can add last term and index in the debug logging as well.
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.DebugFlag = false

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.logEntries = make([]LogEntry, 1) // index 0 is dummy
	rf.role = Follower
	rf.stopChannel = make(chan struct{})

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.electionTimer = time.NewTimer(getRandomElectionTimeout())
	rf.appendEntriesTimers = make([]*time.Timer, 0)
	for i := 0; i < len(rf.peers); i++ {
		rf.appendEntriesTimers = append(rf.appendEntriesTimers, time.NewTimer(HeartbeatTimeout))
	}
	rf.applyTimer = time.NewTimer(ApplyTime)
	rf.signalApplyCh = make(chan struct{}, 100)

	// apply channels
	go func() {
		for {
			select {
			case <-rf.applyTimer.C:
				rf.signalApplyCh <- struct{}{}
			case <-rf.signalApplyCh:
				rf.applyLogs()
			case <-rf.stopChannel:
				return
			}
		}
	}()

	// start ticker goroutine to start elections
	go rf.ticker()

	// Leader sends heartbeats to all the followers
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(idx int) {
			for {
				select {
				case <-rf.appendEntriesTimers[idx].C:
					rf.sendAppendEntries(idx)
				case <-rf.stopChannel:
					return
				}
			}
		}(i)
	}

	return rf
}
