package raft

/**
heartBeat 的 timeout
leader 用来并发的向 follower 们发送 AppendEntries
*/
func (rf *Raft) heartBeat() {
	//DPrintf("%d ： 我现在的 dead为 %d",rf.me,rf.dead)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}

		go func(i int) {
			rf.mu.Lock()
			if rf.state == follower {
				rf.mu.Unlock()
				return
			}
			rf.mu.Unlock()
			args := &AppendEntriesArgs{
				Term:     rf.currentTerm,
				LeaderId: rf.me,
			}
			reply := &AppendEntriesReply{}
			DPrintf("%d 发送心跳给 %d", rf.me, i)
			boo := rf.sendAppendEntries(i, args, reply)
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.conver(follower)
				DPrintf("%d :发现 term 更高的leader %d,yield！！", rf.me, i)
			} else {
				if boo {
					DPrintf("%d 发给 %d 的 heartBeat success", rf.me, i)

				} else {
					DPrintf("%d 发给 %d 的 heartBeat Fail~~~", rf.me, i)
				}
			}
			rf.mu.Unlock()

		}(i)
	}
}
