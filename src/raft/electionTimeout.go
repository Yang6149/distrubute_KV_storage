package raft

import (
	"sync/atomic"
	"time"
)

/**
election 的 timeout
是leader 的话就 wait 等待没有leader的时候，重新竞选新leader的时候signal?????????????????
*/
func (rf *Raft) electionTimeoutTick() {
	for {
		//DPrintf("%d ： 我现在的 dead为 %d",rf.me,rf.dead)
		if atomic.LoadInt32(&rf.dead) == 1 {
			DPrintf("%d :原地去世", rf.me)
			return
		}
		rf.mu.Lock()
		if rf.state == leader {
			//已经是 leader 了
			go rf.heartBeatTimeout()
			rf.mu.Unlock()
			return
			//rf.LeaderCond.Wait()
		} else {
			//现在的状态是 follower 或 candidate
			if time.Now().UnixNano()/int64(time.Millisecond) > rf.electionTimer {
				//超时需要重新竞选 leader
				DPrintf("%d ： 开始竞选 leadrt,当前状态为 %d", rf.me, rf.state)
				rf.state = candidate
				rf.currentTerm++
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
						DPrintf("%d 请求 %d 投票", rf.me, i)
						rf.sendRequestVote(i, args, reply)
						DPrintf("%d 请求 %d 投票 返回了！", rf.me, i)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term > rf.currentTerm {
							DPrintf("%d:收到的心跳竟然term 比我大", rf.me)
							rf.state = follower
						}
						if reply.VoteGranted && reply.Term == rf.currentTerm {
							voteForMe++
						}
						if voteForMe >= rf.menkan {
							DPrintf("%d 我的 term为 %d ，本次收到投票 %d,它的 term 为%d", rf.me, rf.currentTerm, i, reply.Term)
							DPrintf("%d 当选leader", rf.me)
							rf.state = leader
						}
					}(i)

				}

				rf.electionTimer = time.Now().UnixNano()/int64(time.Millisecond) + electionConstTime()

			}

			time.Sleep(time.Millisecond * 10)
		}
		rf.mu.Unlock()
	}
}
