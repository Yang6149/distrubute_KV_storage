package raft

func (rf *Raft) convert(state int) {
	if state == rf.state {
		return
	}
	rf.state = state
	switch state {
	case follower:
		//rf.chanReset()
		DPrintf("%d 转变为 follower", rf.me)
		rf.voteFor = -1
	case candidate:
		//rf.chanReset()
		DPrintf("%d 转变为 candidate", rf.me)
		rf.voteFor = rf.me
	case leader:
		//rf.chanReset()
		DPrintf("%d 转变为 leader", rf.me)
		//初始化 每个follower 的HBchs
		//初始化 leader 的nextIndex
		rf.nextIndex = make([]int, len(rf.peers))
		for a := range rf.nextIndex {
			rf.nextIndex[a] = rf.logLen()
			DPrintf("%d 初始化 nextIndex %d ", rf.me, rf.nextIndex[a])
		}
		rf.matchIndex = make([]int, len(rf.peers))
		rf.heartBeat()
		go rf.heartBeatInit()
		DPrintf("%d log=%d,len = %d commit = %d", rf.me, rf.log, rf.logLen(), rf.commitIndex)
	}
}
func (rf *Raft) chanReset() {
	rf.appendChan = make(chan int, 10000)
	rf.voteGrantedChan = make(chan int, 10000)
	rf.findBiggerChan = make(chan int, 10000)

}
