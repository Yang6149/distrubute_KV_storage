package raft

import "time"

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
	//PreLogIndex
	//PreLogTerm
	//Entries[]
	//LeaderCommit
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

//目前写的很复杂，最后回同一删除精简
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		if args.Term >= rf.currentTerm {
			rf.state = follower
			rf.currentTerm = args.Term
			rf.electionTimer = time.Now().UnixNano()/int64(time.Millisecond) + electionConstTime()
			reply.Term = args.Term
			reply.Success = true
		} else {
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	} else {
		DPrintf("%d :leader 收到了 心跳", rf.me)
		if args.Term > rf.currentTerm {
			DPrintf("%d :我承认你%d 我的term %d ,你的term %d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
			rf.state = follower
			rf.currentTerm = args.Term
			rf.electionTimer = time.Now().UnixNano()/int64(time.Millisecond) + electionConstTime()
			reply.Term = args.Term
			reply.Success = true
		} else {
			DPrintf("%d： 我不承认你%d 我的 term %d ，你的term %d", rf.me, args.LeaderId, rf.currentTerm, args.Term)
			reply.Term = rf.currentTerm
			reply.Success = false
		}
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
