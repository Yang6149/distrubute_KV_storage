package raft

/**
election 的 timeout
是leader 的话就 wait 等待没有leader的时候，重新竞选新leader的时候signal?????????????????
*/
func (rf *Raft) election() {
	rf.currentTerm++
	DPrintf("%d ： 开始竞选 leadrt,当前term为%d", rf.me, rf.currentTerm)
	voteForMe := 0
	voteForMe++
	rf.voteFor = rf.me
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		args := &RequestVoteArgs{
			CandidateId:  rf.me,
			Term:         rf.currentTerm,
			LastLogIndex: len(rf.log) - 1,
			LastLogTerm:  rf.log[len(rf.log)-1].Term,
		}
		reply := &RequestVoteReply{}
		DPrintf("%d ask vote from %d,顺便一说我当前的状态是 %d", rf.me, i, rf.state)
		go func(i int) {
			rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				DPrintf("%d:收到的选举返回竟然term 比我大", rf.me)
				rf.findBiggerChan <- 1
				return
			}
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				voteForMe++
				if rf.state != candidate {
					return
				}
				if voteForMe >= rf.menkan {
					DPrintf("%d 当选leader", rf.me)
					rf.voteGrantedChan <- 1

				}
			}

		}(i)

	}
}
