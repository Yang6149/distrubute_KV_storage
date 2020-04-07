package raft

func (rf *Raft) conver(state int) {
	if state == rf.state {
		return
	}
	rf.state = state
	switch state {
	case follower:
		rf.chanReset()
		DPrintf("%d 转变为 follower", rf.me)
		rf.voteFor = -1
	case candidate:
		rf.chanReset()
		DPrintf("%d 转变为 candidate", rf.me)
		rf.voteFor = rf.me
	case leader:
		rf.chanReset()
		DPrintf("%d 转变为 leader", rf.me)
		//初始化 leader 的nextIndex
		rf.nextIndex = make([]int, len(rf.peers))
		for a := range rf.nextIndex {
			rf.nextIndex[a] = rf.commitIndex + 1
		}
		rf.heartBeat()
	}
}
func (rf *Raft) chanReset() {
	rf.appendChan = make(chan int, 10000)
	rf.voteGrantedChan = make(chan int, 10000)
	rf.findBiggerChan = make(chan int, 10000)
}
