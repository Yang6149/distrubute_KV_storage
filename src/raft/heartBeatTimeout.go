package raft

import (
	"sync/atomic"
	"time"
)

/**
heartBeat 的 timeout
leader 用来并发的向 follower 们发送 AppendEntries
*/
func (rf *Raft) heartBeatTimeout() {
	//DPrintf("%d ： 我现在的 dead为 %d",rf.me,rf.dead)
	for {
		if atomic.LoadInt32(&rf.dead) == 1 {
			DPrintf("%d :原地去世", rf.me)
			return
		}
		rf.mu.Lock()

		if rf.state == leader {
			//并发的进行操作

			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go func(i int) {
					rf.mu.Lock()
					if rf.state == follower {
						return
					}
					rf.mu.Unlock()
					args := &AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply := &AppendEntriesReply{}
					boo := rf.sendAppendEntries(i, args, reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = follower
						rf.electionTimer = time.Now().UnixNano()/int64(time.Millisecond) + electionConstTime()
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
		} else {
			//阻塞等待唤醒
			DPrintf("%d ：已经不是leader 了退出heartbeat", rf.me)
			go rf.electionTimeoutTick()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond * 50)
	}
}
