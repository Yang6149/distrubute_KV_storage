package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PreLogIndex  int
	PreLogTerm   int
	Entries      []Entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term       int
	Success    bool
	MatchIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.appendChan <- 1
		DPrintf("%d：重置ele时间", rf.me)
		if args.PreLogIndex >= len(rf.log) {
			//preIndex 越界
			DPrintf("%d :因为leader传来的preIndex大于自己的len(log)所以return false", rf.me)
			reply.Term = rf.currentTerm
			reply.Success = false
			return
		}
		if rf.log[args.PreLogIndex].Term != args.PreLogTerm {
			//
			//index and  term can't match,return false
			reply.Term = rf.currentTerm
			reply.Success = false
			DPrintf("%d :index and term can't match ,return false", rf.me)
		} else {
			// index and term is matched
			reply.Term = rf.currentTerm
			reply.Success = true
			if len(args.Entries) > 0 {
				//sent a entry
				//determine whether it is overwrited
				if args.PreLogIndex+1 < len(rf.log) {
					//if it is overwrite
					rf.log[args.PreLogIndex+1] = args.Entries[0]
				} else {
					//else append to log
					rf.log = append(rf.log, args.Entries[0])
				}
				reply.MatchIndex = args.PreLogIndex + 1
				DPrintf("%d ：成功接收一个 entry，matchIndex 为%d", rf.me, reply.MatchIndex)
			} else {
				//just a heartbeat
				reply.Term = rf.currentTerm
				reply.Success = true
				reply.MatchIndex = args.PreLogIndex
				DPrintf("%d ：just a heartBeat 为%d", rf.me, reply.MatchIndex)
			}
			if args.LeaderCommit > rf.commitIndex {
				//commit all index before args.LeaderCommit
				rf.commitIndex = min(args.LeaderCommit, reply.MatchIndex)
				rf.sendApply<-rf.commitIndex
			}
		}

	} else {
		//我的 Term 更大 返回false
		DPrintf("%d :因为自己的term比leader %d 更大return false", rf.me, args.LeaderId)
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func min(x int, y int) int {
	if x > y {
		return y
	} else {
		return x
	}
}
