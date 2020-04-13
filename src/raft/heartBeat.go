package raft

import "time"

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

		go rf.sendAppendEntry(i)
	}
}

func (rf *Raft) sendAppendEntry(i int) {
	rf.mu.Lock()
	if rf.state != leader {
		rf.mu.Unlock()
		return
	}

	//初始化 append args
	// DPrintf("%d:%d的nextIndex %d", rf.me, i, rf.nextIndex[i])
	// DPrintf("%d 的len log %d ", rf.me, len(rf.log))
	args := &AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PreLogIndex:  rf.nextIndex[i] - 1,
		PreLogTerm:   rf.log[rf.nextIndex[i]-1].Term,
		LeaderCommit: rf.commitIndex,
		Entries:      make([]Entry, 0),
	}
	args.Entries = rf.log[rf.nextIndex[i]:min(len(rf.log), rf.nextIndex[i]+5)]

	reply := &AppendEntriesReply{}
	yourLastMatchIndex := rf.matchIndex[i]
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(i, args, reply)
	rf.mu.Lock()
	if ok && rf.state == leader {
		if args.PreLogIndex == rf.nextIndex[i]-1 && yourLastMatchIndex == rf.matchIndex[i] && args.Term == rf.currentTerm { //证明传输后信息没有变化
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.persist()
				rf.findBiggerChan <- 1
				rf.convert(follower)
			} else {
				if reply.Success {
					myLastMatch := rf.matchIndex[i]

					rf.matchIndex[i] = reply.MatchIndex

					//1. check MatchIndex
					DPrintf("%d :check->send entries to %d :%d", rf.me, i, args.Entries)
					DPrintf("%d :get reply MatchIndex is %d", rf.me, reply.MatchIndex)
					DPrintf("%d 是否能是我的Term ", rf.me, rf.log[reply.MatchIndex].Term == rf.currentTerm)
					if rf.log[reply.MatchIndex].Term == rf.currentTerm && rf.commitIndex < reply.MatchIndex && reply.MatchIndex > myLastMatch {
						//检测match数量，大于一大半就commit
						DPrintf("%d 进行检测是否commit,curCommit is %d ", rf.me, rf.matchIndex)
						matchNum := 1
						for m := range rf.matchIndex {
							if m == rf.me {
								continue
							}
							if rf.matchIndex[m] >= reply.MatchIndex {
								matchNum++
							}
							if matchNum >= rf.menkan {
								rf.commitIndex = rf.matchIndex[i]
								rf.sendApply <- rf.commitIndex
								break
							}
						}

					}
					rf.nextIndex[i] = reply.MatchIndex + 1
					//处理leader 的commitedindex
					if rf.matchIndex[i] < rf.commitIndex {
						rf.heartBeatchs[i].c <- 1
					}
				} else {
					//false两种情况：它的Term比我的大被上面解决了，这里只会是prevIndex的Term不匹配
					//这里要做到秒发
					//fmt.Println(rf.nextIndex, "leader is :", rf.me, ",send to ", i)
					//fmt.Println(rf.me, "-- args:", args, "reply:", reply)

					if reply.MatchIndex != 0 {
						rf.nextIndex[i] = reply.MatchIndex + 1
					} else if reply.TargetTerm != 0 {
						index := reply.TargetIndex
						for a := index; a >= 0; a-- {
							if rf.log[a].Term <= reply.TargetTerm {
								rf.nextIndex[i] = a + 1
								break
							}
						}
					} else {
						rf.nextIndex[i]--
					}
					if reply.TargetTerm != 0 && reply.MatchIndex != 0 {
					}
					//fmt.Println(rf.me, "减完后 ", i, "的nextIndex 为", rf.nextIndex[i])
					rf.heartBeatchs[i].c <- 1
				}
			}
		} else {
		}
	} else {
		//发送 rpc 失败了
	}

	rf.mu.Unlock()

}

func (rf *Raft) heartBeatForN(i int) {

	// Send periodic heartbeats, as long as still leader.
	for {

		go rf.sendAppendEntry(i)
		select {
		case <-rf.heartBeatchs[i].c:
			//收到了请求
		case <-time.After(heartbeatConstTime):
			//超时
		}

		rf.mu.Lock()
		if rf.state != leader {
			rf.heartBeatchs[i].c = make(chan int, 100)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}
}
func (rf *Raft) heartBeatInit() {
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.heartBeatForN(i)
	}
}
