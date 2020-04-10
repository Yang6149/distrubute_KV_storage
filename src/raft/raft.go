package raft

// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"../labrpc"
)

// import "bytes"
// import "../labgob"
//
// A Go object implementing a single Raft peer.
//
const leader = 2
const follower = 0
const candidate = 1
const heartbeatConstTime = 50 * time.Millisecond

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//raft comment
type Raft struct {
	mu         sync.Mutex // Lock to protect shared access to this peer's state
	LeaderCond sync.Cond
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *Persister          // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	state      int

	//持久化
	currentTerm int     // 当前的 term
	voteFor     int     // 为某人投票
	menkan      int     //大多数的一个阈值
	log         []Entry //logEntries

	//log
	//所有可变属性
	commitIndex int //	当前 commit 的index
	lastApplied int //	本机apply的最大的log的index

	//可变 on leader
	nextIndex  []int //	下一个要发送给server的logEntry的index
	matchIndex []int //	直到server 中各个log的index到哪了

	voteGrantedChan chan int
	appendChan      chan int
	findBiggerChan  chan int
	applyCh         chan ApplyMsg
	sendApply       chan int
	// Your data here (2A, 2B, 2C).-------------------------------------

}

// GetState get command .
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//log.Println("GetState")
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.state == leader
	// Your code here (2A).------------------------------------------
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state != leader {
		return -1, -1, false
	} else {
		entry := Entry{Term: rf.currentTerm, Command: command}
		rf.log = append(rf.log, entry) //向log 中加入client 最新的request
		DPrintf("%d get command from Start at index %d", rf.me, len(rf.log)-1)
		return len(rf.log) - 1, rf.currentTerm, true

	}
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	DPrintf("%d:去死吧！！！！！！！！！！！！！！！！！！！！！！！！！！！%d", rf.me, rf.dead)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.menkan = len(rf.peers)/2 + 1
	rf.applyCh = applyCh
	rf.currentTerm = 1
	rf.log = make([]Entry, 1)
	rf.log[0] = Entry{Term: rf.currentTerm}
	rf.sendApply = make(chan int, 1000)
	rf.chanReset()
	// Your initialization code here (2A, 2B, 2C).-------------------------------

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func(rf *Raft) {
		DPrintf("%d林克四大头", rf.me)
		for {
			if rf.killed() {
				DPrintf("%d当场去世", rf.me)
				return
			}
			rf.mu.Lock()
			st := rf.state
			rf.mu.Unlock()
			switch st {
			case follower:
				//
				select {
				case <-rf.findBiggerChan:
					DPrintf("%d is follower ,find bigger term", rf.me)
				case <-rf.appendChan:
					//follower收到有效append，重置超时
				case <-time.After(electionConstTime()):
					//超时啦，进行选举
					rf.mu.Lock()
					rf.conver(candidate)
					rf.election()
					rf.mu.Unlock()
				}
			case candidate:
				select {
				case <-rf.findBiggerChan:
					//发现了更大地 term ，转为follower
				case <-rf.appendChan:
					//candidate 收到有效心跳，转回follower
					rf.mu.Lock()
					rf.conver(follower)
					rf.mu.Unlock()
				case <-rf.voteGrantedChan:
					//candidate 收到多数投票结果，升级为 leader
					rf.mu.Lock()
					rf.conver(leader)
					rf.mu.Unlock()
				case <-time.After(electionConstTime()):
					//没有投票结果，也没有收到有效append，重新giao
					rf.mu.Lock()
					rf.election()
					rf.mu.Unlock()
				}
				//
			case leader:
				select {
				case <-rf.findBiggerChan:
					rf.mu.Lock()
					rf.conver(follower)
					rf.mu.Unlock()
				case <-rf.appendChan:
					//收到有效地心跳，转为follower
					rf.mu.Lock()
					rf.conver(follower)
					rf.mu.Unlock()
				case <-time.After(heartbeatConstTime):
					//进行一次append
					rf.mu.Lock()
					rf.heartBeat()
					rf.mu.Unlock()
				}

			}
		}
	}(rf)
	go func(rf *Raft) {
		rf.apply()
	}(rf)
	return rf
}

func electionConstTime() time.Duration {
	return time.Duration(150+rand.Intn(150)) * time.Millisecond
}

func (rf *Raft) apply() {
	DPrintf("%d 初始化 apply", rf.me)
	for {
		select {
		case index := <-rf.sendApply:
			for i := rf.lastApplied + 1; i <= index; i++ {
				rf.mu.Lock()
				DPrintf("%d len(rf.log) :%d", rf.me,len(rf.log))
				command := rf.log[i].Command
				DPrintf("%d apply at index %d", rf.me, i)
				DPrintf("%d : %d", rf.me, rf.log)
				rf.mu.Unlock()
				msg := ApplyMsg{
					CommandValid: true,
					Command:      command,
					CommandIndex: i,
				}

				rf.applyCh <- msg
				rf.lastApplied = i
			}
		}
	}
}
