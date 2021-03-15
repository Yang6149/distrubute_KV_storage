package raft

/**
election 的 timeout
是leader 的话就 wait 等待没有leader的时候，重新竞选新leader的时候signal?????????????????
*/
func (rf *Raft) election() {
	rf.currentTerm++
	rf.persist()
	voteForMe := 0
	voteForMe++
	rf.voteFor = rf.me
	if rf.me == 2 {
		DPrintf("election")
	}
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		args := &RequestVoteArgs{
			CandidateId:  rf.me,
			Term:         rf.currentTerm,
			LastLogIndex: rf.logLen() - 1,
			LastLogTerm:  rf.logTerm(rf.logLen() - 1),
		}
		reply := &RequestVoteReply{}
		go func(i int) {
			DPrintf("%d 发送election，Term=%d,lastIndex=%d,lastTerm=%d", rf.me,args.Term, args.LastLogIndex, args.LastLogTerm)
			rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				rf.findBiggerChan <- 1
				rf.convert(follower)
				return
			}
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				voteForMe++
				if rf.state != candidate {
					return
				}
				if voteForMe >= rf.menkan {
					rf.voteGrantedChan <- 1
					rf.convert(leader)
				}
			}

		}(i)

	}
}
