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

type InstallSnapshotsArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotsReply struct {
	Success    bool
	Term       int
	MatchIndex int
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
		if args.PreLogIndex >= rf.logLen() {
			//preIndex 越界
			reply.Term = rf.currentTerm
			reply.Success = false
			reply.MatchIndex = rf.logLen() - 1
			return
		}
		if rf.logTerm(args.PreLogIndex) != args.PreLogTerm {

			//index and  term can't match,return false
			reply.Term = rf.currentTerm
			reply.Success = false
			index := args.PreLogIndex - 1
			for a := index; a >= 0; a-- {
				if rf.logTerm(a) <= args.PreLogTerm {
					reply.TargetIndex = a
					reply.TargetTerm = rf.logTerm(a)
					break
				}
			}
		} else {
			// index and term is matched
			reply.Term = rf.currentTerm
			reply.Success = true
			if len(args.Entries) > 0 {

				if args.PreLogIndex+len(args.Entries) > rf.commitIndex {
					Index := args.PreLogIndex + 1
					for a := range args.Entries {
						if Index == rf.logLen() {
							rf.log = append(rf.log, args.Entries[a])
						} else {
							rf.logSet(Index, args.Entries[a])
						}
						Index++
					}
					rf.log = rf.logGets(rf.lastIncludedIndex+1, Index)

					rf.persist()
				}
				reply.MatchIndex = max(args.PreLogIndex+len(args.Entries), rf.commitIndex)
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
		}

	} else {
		//我的 Term 更大 返回false
		reply.Term = rf.currentTerm
		reply.Success = false
	}
}

func (rf *Raft) InstallSnapshots(args *InstallSnapshotsArgs, reply *InstallSnapshotsReply) {
	DPrintf("%d unlock接受 snap", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	//follower 接收到 snapshot 进行处理
	if max(rf.lastIncludedIndex, rf.commitIndex) >= args.LastIncludedIndex || rf.currentTerm > args.Term {
		DPrintf("%d mu接受 snap", rf.me)
		reply.Success = false
		reply.Term = rf.currentTerm
	}
	DPrintf("%d 接受 snap", rf.me)
	rf.appendChan <- 1
	DPrintf("%d 111", rf.me)
	rf.SnapshotF = make(chan int, 1)
	msg := ApplyMsg{false, args.Data, args.LastIncludedIndex}
	rf.mu.Unlock()
	rf.applyCh <- msg
	DPrintf("%d 222", rf.me)
	a := <-rf.SnapshotF
	rf.mu.Lock()
	if a == -1 {
		return
	}
	DPrintf("%d 333", rf.me)
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)
	reply.Success = true
	reply.MatchIndex = rf.lastIncludedIndex
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendInstallSnapshots(server int, args *InstallSnapshotsArgs, reply *InstallSnapshotsReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshots", args, reply)
	return ok
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
