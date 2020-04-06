package raft

/**
election 的 timeout
是leader 的话就 wait 等待没有leader的时候，重新竞选新leader的时候signal?????????????????
*/
func (rf *Raft) election() {
	rf.electionTimer.Reset(electionConstTime()) //重新设置超时时间
	rf.currentTerm++
	DPrintf("%d ： 开始竞选 leadrt,当前term为%d", rf.me, rf.currentTerm)
	voteForMe := 0
	voteForMe++
	rf.voteFor = rf.me
	for i := range rf.peers {
		if rf.me == i {
			continue
		}
		args := &RequestVoteArgs{CandidateId: rf.me, Term: rf.currentTerm}
		reply := &RequestVoteReply{}
		DPrintf("%d ask vote from %d,顺便一说我当前的状态是 %d", rf.me, i, rf.state)
		go func(i int) {
			//rf.mu.Lock()
			//defer rf.mu.Unlock()
			rf.sendRequestVote(i, args, reply)
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				DPrintf("%d:收到的选举返回心跳竟然term 比我大", rf.me)
				rf.state = follower
			}
			if reply.VoteGranted && reply.Term == rf.currentTerm {
				voteForMe++
				if voteForMe >= rf.menkan {
					DPrintf("%d 我的 term为 %d ，本次收到投票 %d,它的 term 为%d", rf.me, rf.currentTerm, i, reply.Term)
					DPrintf("%d 当选leader", rf.me)
					rf.conver(leader)
					//初始化 leader 的nextIndex
					rf.nextIndex = make([]int, len(rf.peers))
					for a := range rf.nextIndex {
						rf.nextIndex[a] = rf.commitIndex + 1
					}

				}
			}

		}(i)

	}
}
