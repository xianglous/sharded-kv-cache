package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term     int
	Success  bool
	NexIndex int
}

func (rf *Raft) resetAllHeartbeatTimer() {
	for i, _ := range rf.appendEntriesTimers {
		rf.appendEntriesTimers[i].Stop()
		rf.appendEntriesTimers[i].Reset(0)
	}
}

func (rf *Raft) resetHeartbeatTimer(peeridx int) {
	rf.appendEntriesTimers[peeridx].Stop()
	rf.appendEntriesTimers[peeridx].Reset(HeartbeatTimeout)
}

func (rf *Raft) makeAppendEntriesArgs(peeridx int) AppendEntriesArgs {
	// Make the logentries to append
	nextIdx := rf.nextIndex[peeridx]
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	logEntries := make([]LogEntry, 0)
	var prevLogIndex int
	var prevLogTerm int

	if nextIdx > lastLogIndex {
		// No log to send
		rf.delog("makeAppendEntriesArgs: no log to send")
		prevLogIndex = lastLogIndex
		prevLogTerm = lastLogTerm
	} else {
		logEntries = append(logEntries, rf.logEntries[rf.getIdxByLogIndex(nextIdx):]...)
		prevLogIndex = nextIdx - 1

		prevLogTerm = rf.getLogEntryByIndex(prevLogIndex).Term

	}

	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		LeaderCommit: rf.commitIndex,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      logEntries,
	}
	return args
}

func (rf *Raft) sendAppendEntries(peeridx int) {
	RPCTimer := time.NewTimer(RPCTimeout)
	defer RPCTimer.Stop()
	for !rf.killed() {
		rf.lock("sendAppendEntries1")
		if rf.role != Leader {
			rf.resetHeartbeatTimer(peeridx)
			rf.unlock("sendAppendEntries1")
			return
		}
		rf.resetHeartbeatTimer(peeridx)
		args := rf.makeAppendEntriesArgs(peeridx)
		rf.unlock("sendAppendEntries1")
		RPCTimer.Stop()
		RPCTimer.Reset(RPCTimeout)
		reply := AppendEntriesReply{}
		resCh := make(chan bool, 1)
		go func(args *AppendEntriesArgs, reply *AppendEntriesReply) {
			ok := rf.peers[peeridx].Call("Raft.AppendEntries", args, reply)
			if !ok {
				time.Sleep(10 * time.Millisecond)
			}
			resCh <- ok
		}(&args, &reply)

		select {
		case <-RPCTimer.C:
			rf.delog("sendAppendEntries timeout: peeridx=%v", peeridx)
			continue
		case <-rf.stopChannel:
			rf.delog("sendAppendEntries: stopped")
			return
		case ok := <-resCh:
			if !ok {
				rf.delog("sendAppendEntries: GGGGGGG")
				continue
			}
		}

		rf.delog("sendAppendEntries: peeridx=%v, args=%+v, reply=%+v", peeridx, args, reply)
		// handle the reply
		rf.lock("sendAppendEntries2")
		if rf.currentTerm < reply.Term {
			// stfu
			rf.delog("WAS SHUSHED STFU")
			rf.currentTerm = reply.Term
			rf.changeRole(Follower)
			rf.resetElectionTimer()
			rf.persist()
			rf.unlock("sendAppendEntries2")
			return
		}
		if rf.role != Leader || rf.currentTerm != args.Term {
			// stfu
			rf.delog("WEIRD STFU")
			rf.unlock("sendAppendEntries2")
			return
		}
		if reply.Success {
			if rf.nextIndex[peeridx] < reply.NexIndex {
				rf.nextIndex[peeridx] = reply.NexIndex
				rf.matchIndex[peeridx] = reply.NexIndex - 1
			}
			if len(args.Entries) > 0 && rf.currentTerm == args.Entries[len(args.Entries)-1].Term {
				rf.updateCommitIndex()
			}
			rf.persist()
			rf.unlock("sendAppendEntries2")
			return
		} else { // reply False
			if reply.NexIndex != 0 { // reply.NexIndex == 0 means the follower's log is empty
				if 0 < reply.NexIndex {
					rf.nextIndex[peeridx] = reply.NexIndex
					rf.unlock("sendAppendEntries2")
					rf.delog("Retrying sendAppendEntries")
					continue
				}
			} else {
				rf.unlock("sendAppendEntries2")
			}
		}
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.lock("AppendEntries")
	rf.delog("AppendEntries: args=%+v", args)
	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		reply.Success = false
		rf.unlock("AppendEntries")
		return
	}
	rf.currentTerm = args.Term
	rf.changeRole(Follower)
	rf.resetElectionTimer()

	_, lastLogIndex := rf.getLastLogTermIndex()
	if args.PrevLogIndex > lastLogIndex { // Missing logs in the middle
		rf.delog("AppendEntries: Missing logs in the middle, args.PrevLogIndex=%v, lastLogIndex=%v", args.PrevLogIndex, lastLogIndex)
		reply.Success = false
		reply.NexIndex = lastLogIndex + 1
	} else if args.PrevLogIndex == 0 { // Can fill the gap
		if rf.isArgsEntriesOutOfOrder(args) {
			reply.Success = false
			reply.NexIndex = 0
		} else {
			reply.Success = true
			rf.delog("AppendEntries: successfully filled the gap, Now i have %+v log entries", len(rf.logEntries))
			rf.logEntries = append(rf.logEntries[:1], args.Entries...)
			_, tempLastIndex := rf.getLastLogTermIndex()
			reply.NexIndex = tempLastIndex + 1
		}
	} else if rf.logEntries[rf.getIdxByLogIndex(args.PrevLogIndex)].Term == args.PrevLogTerm {
		if rf.isArgsEntriesOutOfOrder(args) {
			reply.Success = false
			reply.NexIndex = 0
		} else { // Can fill the gap with the log entries in args
			reply.Success = true
			rf.delog("AppendEntries: successfully filled the gap with the log entries in args")
			rf.logEntries = append(rf.logEntries[:rf.getIdxByLogIndex(args.PrevLogIndex)+1], args.Entries...)
			_, tempLastIndex := rf.getLastLogTermIndex()
			reply.NexIndex = tempLastIndex + 1
		}
	} else {
		// Need to find the last index of the previous term
		reply.Success = false
		idx := args.PrevLogIndex
		argTerm := rf.logEntries[rf.getIdxByLogIndex(args.PrevLogIndex)].Term
		for idx > rf.commitIndex && idx > 0 && rf.logEntries[rf.getIdxByLogIndex(idx)].Term == argTerm {
			idx--
		}
		reply.NexIndex = idx + 1
		rf.delog("AppendEntries logs dont match")
	}

	if reply.Success && args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = args.LeaderCommit
		rf.signalApplyCh <- struct{}{}
	}

	rf.persist()
	rf.delog("Handle AppendEntries: reply=%+v", reply)
	rf.unlock("AppendEntries")
}

func (rf *Raft) updateCommitIndex() {
	rf.delog("Start in updateCommitIndex()")
	updated := false
	for i := rf.commitIndex + 1; i <= len(rf.logEntries); i++ {
		count := 0
		for _, matchIndex := range rf.matchIndex {
			if matchIndex >= i {
				count++
				if count > len(rf.peers)/2 {
					rf.delog("Updating commit index: %d.", i)
					rf.commitIndex = i
					updated = true
					break
				}
			}
		}
		if rf.commitIndex != i {
			break
		}
	}
	if updated {
		rf.signalApplyCh <- struct{}{}
	}
	rf.delog("updateCommitIndex() returns")
}

func (rf *Raft) isArgsEntriesOutOfOrder(args *AppendEntriesArgs) bool {
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	if args.PrevLogIndex+len(args.Entries) < lastLogIndex && lastLogTerm == args.Term {
		return true
	}
	return false
}
