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

		go rf.sendAppendEntry(i)
	}
}

func (rf *Raft) sendAppendEntry(i int) {
	rf.mu.Lock()
	if rf.state == follower {
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
	if len(rf.log) > rf.nextIndex[i] {
		args.Entries = append(args.Entries, rf.log[rf.nextIndex[i]])
		DPrintf("%d 发送 index %d 的log 给 %d", rf.me, rf.nextIndex[i], i)
	}
	reply := &AppendEntriesReply{}
	DPrintf("%d append给 %d", rf.me, i)
	rf.mu.Unlock()
	ok := rf.sendAppendEntries(i, args, reply)
	rf.mu.Lock()
	if ok && rf.state == leader {
		if args.PreLogIndex == rf.nextIndex[i]-1 {
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.findBiggerChan <- 1
				DPrintf("%d :发现 term 更高的node %d,yield！！", rf.me, i)
			} else {
				if reply.MatchIndex == -1 {
					DPrintf("%d 重复了，不用进行任何操作:%d", rf.me, i)

				} else if reply.Success {
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
							DPrintf("%d :matchindex m is %d ,nextIndex m is %d  this follower is %d", rf.me, rf.matchIndex[m], rf.nextIndex[m], m)
							if rf.matchIndex[m] >= reply.MatchIndex {
								matchNum++
								DPrintf("%d the matchNum is %d,the index is %d", rf.me, matchNum, reply.MatchIndex)
							}
							if matchNum >= rf.menkan {
								DPrintf("%d 开始 commit at index %d", rf.me, rf.nextIndex[i])
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
						go rf.sendAppendEntry(i)
					}
				} else {
					//false两种情况：它的Term比我的大被上面解决了，这里只会是prevIndex的Term不匹配
					//这里要做到秒发

					DPrintf("%d:减一下 %d 的nextInt呗", rf.me, i)
					//fmt.Println(rf.nextIndex, "leader is :", rf.me, ",send to ", i)
					//fmt.Println(rf.me, "-- args:", args, "reply:", reply)
					rf.nextIndex[i]--
					//fmt.Println(rf.me, "减完后 ", i, "的nextIndex 为", rf.nextIndex[i])
					go rf.sendAppendEntry(i)

				}
			}
		}
	} else {
		//发送 rpc 失败了
	}

	rf.mu.Unlock()

}
