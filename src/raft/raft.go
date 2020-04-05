package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
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

import "sync"
import "sync/atomic"
import "../labrpc"
import "log"
import "time"
import "math/rand"
// import "bytes"
// import "../labgob"

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

//
// A Go object implementing a single Raft peer.
//
const leader=2
const follower=0
const candidate=1
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	LeaderCond sync.Cond

	electionTimer int64		// 选举的超时


	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	state	  int
	//持久状态
	currentTerm int 			  // 当前的 term
	voteFor	  int				  // 为某人投票
	//log
	//所有可变属性
	commitIndex int
	lastApplied int

	//可变 on leader
	nextIndex []int
	matchIndex []int



	// Your data here (2A, 2B, 2C).-------------------------------------

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	//rf.mu.Lock()
	//defer rf.mu.Unlock()
	//log.Println("GetState")
	var term int
	var isleader bool
	term = rf.currentTerm;
	isleader=rf.state==leader
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
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).-----------------------------------
	Term int
	CandidateId int
	//lastLogIndex
	//lastLogTerm
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).-----------------------------------------
	Term int
	VoteGranted bool
}

type AppendEntriesArgs struct{
	Term int
	LeaderId int
	//PreLogIndex
	//PreLogTerm
	//Entries[]
	//LeaderCommit
}

type AppendEntriesReply struct{
	Term int
	Success bool
}

//
// example RequestVote RPC handler.handler、handler、handler
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).----------------------------------------
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term>rf.currentTerm{
		rf.currentTerm=args.Term
		rf.voteFor=args.CandidateId
		rf.state=follower
		rf.electionTimer=time.Now().UnixNano()/int64(time.Millisecond)+electionConstTime()
		DPrintf("%d votefor %d,当前 term %d",rf.me,args.CandidateId,rf.currentTerm)
		reply.Term=args.Term
		reply.VoteGranted=true
	}else{
		reply.Term=args.Term
		reply.VoteGranted=false
	}



}

//目前写的很复杂，最后回同一删除精简
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state!=leader{
		if args.Term>=rf.currentTerm {
			rf.state=follower
			rf.currentTerm=args.Term
			rf.electionTimer=time.Now().UnixNano()/int64(time.Millisecond)+electionConstTime()
			reply.Term=args.Term
			reply.Success=true
		}else{
			reply.Term=rf.currentTerm
			reply.Success=false
		}
	}else{
		DPrintf("%d :leader 收到了 心跳",rf.me)
		if args.Term > rf.currentTerm {
			DPrintf("%d :我承认你%d 我的term %d ,你的term %d",rf.me,args.LeaderId,rf.currentTerm,args.Term)
			rf.state=follower
			rf.currentTerm=args.Term
			rf.electionTimer=time.Now().UnixNano()/int64(time.Millisecond)+electionConstTime()
			reply.Term=args.Term
			reply.Success=true
		}else{
			DPrintf("%d： 我不承认你%d 我的 term %d ，你的term %d",rf.me,args.LeaderId,rf.currentTerm,args.Term)
			reply.Term=rf.currentTerm
			reply.Success=false
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool{
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true
	log.Println("start-------------------------")
	// Your code here (2B).


	return index, term, isLeader
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
	DPrintf("%d:去死吧！！！！！！！！！！！！！！！！！！！！！！！！！！！%d",rf.me,rf.dead)
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
	rf.electionTimer = time.Now().UnixNano()/int64(time.Millisecond)+int64(150+rand.Intn(150))

	// Your initialization code here (2A, 2B, 2C).-------------------------------

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go func() {
		go rf.electionTimeoutTick()

		DPrintf("%d:哈吉马路由",me)
	}()
	//timeout 等Leader 信息

	return rf
}

/**
election 的 timeout
是leader 的话就 wait 等待没有leader的时候，重新竞选新leader的时候signal?????????????????
 */
func (rf *Raft)electionTimeoutTick(){
	for{
		//DPrintf("%d ： 我现在的 dead为 %d",rf.me,rf.dead)
		if rf.dead==1{
			DPrintf("%d :原地去世",rf.me)
			return
		}
		rf.mu.Lock()
		if rf.state==leader{
			//已经是 leader 了
			go rf.heartBeatTimeout()
			rf.mu.Unlock()
			return
			//rf.LeaderCond.Wait()
		}else{
			//现在的状态是 follower 或 candidate
			if time.Now().UnixNano()/int64(time.Millisecond)>rf.electionTimer{
				//超时需要重新竞选 leader
				DPrintf("%d ： 开始竞选 leadrt,当前状态为 %d",rf.me,rf.state)
				rf.state=candidate
				rf.currentTerm++
				voteForMe :=0
				voteForMe++
				rf.voteFor=rf.me
				menkan :=len(rf.peers)/2+1
				for i :=range rf.peers{
					if(rf.me==i){
						continue
					}
					args := &RequestVoteArgs{CandidateId:rf.me,Term:rf.currentTerm}
					reply := &RequestVoteReply{}
					DPrintf("%d ask vote from %d,顺便一说我当前的状态是 %d",rf.me,i,rf.state)
					go func(i int) {
						//rf.mu.Lock()
						//defer rf.mu.Unlock()
						DPrintf("%d 请求 %d 投票",rf.me,i)
						rf.sendRequestVote(i,args,reply)
						DPrintf("%d 请求 %d 投票 返回了！",rf.me,i)
						rf.mu.Lock()
						defer rf.mu.Unlock()
						if reply.Term>rf.currentTerm {
							DPrintf("%d:收到的心跳竟然term 比我大",rf.me)
							rf.state=follower
						}
						if reply.VoteGranted {
							voteForMe++
						}
						if voteForMe>=menkan{
							DPrintf("%d 当选leader",rf.me)
							rf.state=leader
						}
					}(i)


				}

				rf.electionTimer=time.Now().UnixNano()/int64(time.Millisecond)+electionConstTime()

			}


			time.Sleep(time.Millisecond*10)
		}
		rf.mu.Unlock()
	}
}

/**
heartBeat 的 timeout
leader 用来并发的向 follower 们发送 AppendEntries
 */
func (rf *Raft)heartBeatTimeout(){
	//DPrintf("%d ： 我现在的 dead为 %d",rf.me,rf.dead)
	for{
		if rf.dead==1{
			DPrintf("%d :原地去世",rf.me)
			return
		}
		rf.mu.Lock()
		if rf.state==leader{
			//并发的进行操作

			for i :=range rf.peers{
				if i==rf.me{
					continue
				}

				go func(i int) {
					rf.mu.Lock()
					if rf.state == follower {
						return
					}
					rf.mu.Unlock()
					args :=&AppendEntriesArgs{
						Term:     rf.currentTerm,
						LeaderId: rf.me,
					}
					reply :=&AppendEntriesReply{}
					boo :=rf.sendAppendEntries(i,args,reply)
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm=reply.Term
						rf.state = follower
						rf.electionTimer=time.Now().UnixNano()/int64(time.Millisecond)+electionConstTime()
						DPrintf("%d :发现 term 更高的leader %d,yield！！",rf.me,i)
					}
					rf.mu.Unlock()
					if boo {
						DPrintf("%d 发给 %d 的 heartBeat success",rf.me,i)

					}else{
						DPrintf("%d 发给 %d 的 heartBeat Fail~~~",rf.me,i)
					}
				}(i)
			}
		}else{
			//阻塞等待唤醒
			DPrintf("%d ：已经不是leader 了退出heartbeat",rf.me)
			go rf.electionTimeoutTick()
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(time.Millisecond*50)
	}
}

func electionConstTime() int64{
	return int64(150+rand.Intn(150))
}