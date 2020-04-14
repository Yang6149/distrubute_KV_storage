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
	Term        int
	Success     bool
	MatchIndex  int
	TargetTerm  int
	TargetIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term >= rf.currentTerm {
		rf.state = follower
		if args.Term > rf.currentTerm {
			rf.currentTerm = args.Term
			rf.persist()
		}
		rf.appendChan <- 1
		rf.convert(follower)
		if args.PreLogIndex >= len(rf.log) {
			//preIndex 越界
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.MatchIndex = len(rf.log) - 1
			return
		}
		if rf.log[args.PreLogIndex].Term != args.PreLogTerm {

			//index and  term can't match,return false
			reply.Term = rf.currentTerm
			reply.Success = false
			index := args.PreLogIndex - 1
			for a := index; a >= 0; a-- {
				if rf.log[a].Term <= args.PreLogTerm {
					reply.TargetIndex = a
					reply.TargetTerm = rf.log[a].Term
					break
				}
			}
		} else {
			// index and term is matched
			reply.Term = rf.currentTerm
			reply.Success = true
			if len(args.Entries) > 0 {

				//sent a entry
				//determine whether it is overwrited
				// if args.PreLogIndex+1 < len(rf.log) {
				// 	//如果我要 overwrite 的内容和我要写的内容一模一样，就证明有一个线程走在我前面，我退出
				// 	if args.Entries[0].Term == rf.log[args.PreLogIndex+1].Term {
				// 		DPrintf("%d我这个是个重复overwrite操作，有个线程在我之前，直接下一个", rf.me)
				// 		reply.MatchIndex = args.PreLogIndex + 1
				// 		return
				// 	} else {
				// 		//if it is overwrite
				// 		DPrintf("%d 原来的 log 为 %d ", rf.me, rf.log)
				// 		rf.log[args.PreLogIndex+1] = args.Entries[0]
				// 		rf.log = rf.log[0 : args.PreLogIndex+2]
				// 		rf.persist()
				// 		DPrintf("%d overwrite 后的 log 为%d", rf.me, rf.log)
				// 	}

				// } else {
				// 	//else append to log
				// 	rf.log = append(rf.log, args.Entries[0])
				// 	rf.persist()
				// 	DPrintf("%d 添加一个新log,现在长度为：%d", rf.me, len(rf.log))
				// }
				Index := args.PreLogIndex + 1
				for a := range args.Entries {
					if Index == len(rf.log) {
						rf.log = append(rf.log, args.Entries[a])
					} else {
						rf.log[Index] = args.Entries[a]
					}
					Index++
				}
				rf.log = rf.log[:Index]

				rf.persist()
				reply.MatchIndex = args.PreLogIndex + len(args.Entries)
			} else {
				//just a heartbeat
				reply.Term = rf.currentTerm
				reply.Success = true
				reply.MatchIndex = args.PreLogIndex
			}
			if args.LeaderCommit > rf.commitIndex {
				//commit all index before args.LeaderCommit
				newCommitNum := min(args.LeaderCommit, reply.MatchIndex)
				if newCommitNum > rf.commitIndex {
					rf.commitIndex = newCommitNum
					rf.sendApply <- rf.commitIndex
				}
			}
			DPrintf("%d :commit index is %d 现在的log 是%d", rf.me, rf.commitIndex, rf.log)
		}

	} else {
		//我的 Term 更大 返回false
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
func max(x int, y int) int {
	if x < y {
		return y
	} else {
		return x
	}
}
