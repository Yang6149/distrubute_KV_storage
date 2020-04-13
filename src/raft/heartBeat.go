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
	DPrintf("%d append给 %d", rf.me, i)
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
				DPrintf("%d :发现 term 更高的node %d,yield！！", rf.me, i)
			} else {
				if reply.Success {
					myLastMatch := rf.matchIndex[i]

					rf.matchIndex[i] = reply.MatchIndex

					//1. check MatchIndex
					DPrintf("%d :check->send entries to %d :%d", rf.me, i, args.Entries)
					if rf.log[reply.MatchIndex].Term == rf.currentTerm && rf.commitIndex < reply.MatchIndex && reply.MatchIndex > myLastMatch {
						//检测match数量，大于一大半就commit
						DPrintf("%d term and index match between %d and index is %d", rf.me, i, reply.MatchIndex)
						matchNum := 1
						for m := range rf.matchIndex {
							if m == rf.me {
								continue
							}
							if rf.matchIndex[m] >= reply.MatchIndex {
								matchNum++
							}
							if matchNum >= rf.menkan {
								DPrintf("%d 开始 commit at index %d", rf.me, rf.nextIndex[i])
								DPrintf("%d now my term is %d ,and commitEntry is", rf.me, rf.currentTerm, rf.log[rf.nextIndex[i]])
								rf.commitIndex = rf.nextIndex[i]
								rf.sendApply <- rf.commitIndex
								DPrintf("%d 发送 commit at index %d---成功", rf.me, rf.nextIndex[i])
								break
							}
						}

					}
					DPrintf("%d 接收到了 %d 返回的matchIndex %d", rf.me, i, reply.MatchIndex)
					rf.nextIndex[i] = reply.MatchIndex + 1
					//处理leader 的commitedindex
					if rf.matchIndex[i] < rf.commitIndex {
						//rf.heartBeatchs[i].c <- 1
						DPrintf("%d follower commit<自己，再发一次", rf.me)
					}
				} else {
					//false两种情况：它的Term比我的大被上面解决了，这里只会是prevIndex的Term不匹配
					//这里要做到秒发
					DPrintf("%d: preIndex %d,preTerm %d,myTerm %d", rf.me, args.PreLogIndex, args.PreLogTerm, rf.log[args.PreLogIndex].Term)
					DPrintf("%d:减一下 %d 的nextInt呗", rf.me, i)
					//fmt.Println(rf.nextIndex, "leader is :", rf.me, ",send to ", i)
					//fmt.Println(rf.me, "-- args:", args, "reply:", reply)

					if reply.MatchIndex != 0 {
						DPrintf("%d 直接把%d的nextInt 跳到%d，原来是%d", rf.me, i, reply.MatchIndex+1, rf.nextIndex[i])
						rf.nextIndex[i] = reply.MatchIndex + 1
					} else if reply.TargetTerm != 0 {
						index := reply.TargetIndex
						for a := index; a >= 0; a-- {
							if rf.log[a].Term <= reply.TargetTerm {
								rf.nextIndex[i] = a + 1
								break
							}
						}
						DPrintf("%d 直接把%d的nextInt 跳到%d，原来是%d,跳到<=index:%d并且term<=reply.preTerm-1.term：%d的最后一个", rf.me, i, rf.nextIndex[i], args.PreLogIndex+1, index, rf.log[index].Term)
					} else {
						rf.nextIndex[i]--
					}
					if reply.TargetTerm != 0 && reply.MatchIndex != 0 {
						DPrintf("%d 警告，returnfalse 后MathIndex和TargetTerm同时不为零", rf.me)
					}
					//fmt.Println(rf.me, "减完后 ", i, "的nextIndex 为", rf.nextIndex[i])
					//rf.heartBeatchs[i].c <- 1
					DPrintf("%d返回false，调整后继续发送", rf.me)
				}
			}
		} else {
			DPrintf("%d heartbeat get modified reply F**K %d!", rf.me, i)
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
			DPrintf("%d 收到强制HB请求", rf.me)
		case <-time.After(heartbeatConstTime):
			DPrintf("%d 超时发送HB", rf.me)
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
