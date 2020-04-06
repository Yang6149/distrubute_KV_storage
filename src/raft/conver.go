package raft

func (rf *Raft) conver(state int) {
	if state == rf.state {
		return
	}
	rf.state = state
	switch state {
	case follower:
		rf.heartbeatTimer.Stop()
		rf.electionTimer.Reset(electionConstTime())
		rf.voteFor = -1
	case candidate:
		rf.electionTimer.Reset(electionConstTime())
		rf.voteFor = rf.me
	case leader:
		rf.heartbeatTimer.Reset(heartbeatConstTime)
		//初始化++++++
		rf.electionTimer.Stop()
		rf.heartBeat()

	}

}
