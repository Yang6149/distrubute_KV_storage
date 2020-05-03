package shardmaster

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Join = "Join"
const Leave = "Leave"
const Move = "Move"
const Query = "Query"

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.

	configs []Config // indexed by config num
	apps    map[int]chan Op
	dup     map[int64]int
}

type Op struct {
	// Your data here.
	Type string
	//join
	Servers map[int][]string //join

	GIDs []int //leave

	Shard int
	GID   int //move

	Num int //query

	ClientId int64
	SerialId int
	Config   Config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{}
	op.Type = Join
	op.Servers = args.Servers
	op.SerialId = args.SerialId
	op.ClientId = args.ClientId

	wrongLeader, err, _ := sm.start(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	return

	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{}
	op.Type = Leave
	op.GIDs = args.GIDs
	op.SerialId = args.SerialId
	op.ClientId = args.ClientId

	wrongLeader, err, _ := sm.start(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	return
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	if _, isLeader := sm.rf.GetState(); !isLeader {
		reply.WrongLeader = true
		return
	}
	op := Op{}
	op.Type = Query
	op.Num = args.Num
	op.ClientId = args.ClientId
	op.SerialId = args.SerialId
	wrongLeader, err, config := sm.start(op)
	reply.WrongLeader = wrongLeader
	reply.Err = err
	reply.Config = config
	return
}

func (sm *ShardMaster) start(op Op) (bool, Err, Config) { //wrongLeader , Err
	sm.mu.Lock()
	resConfig := Config{}
	if !sm.checkDup(op.ClientId, op.SerialId) { //重复了
		fmt.Println("dup!!!")
		return false, DupCommand, resConfig
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		defer sm.mu.Unlock()
		return true, "", resConfig
	}
	ch := make(chan Op, 1)
	sm.apps[index] = ch
	sm.mu.Unlock()
	defer func() {
		sm.mu.Lock()
		delete(sm.apps, index)
		sm.mu.Unlock()
	}()
	select {
	case oop := <-ch:
		//返回成功
		if op.ClientId == oop.ClientId && op.SerialId == oop.SerialId {
			DPrintf("return op ", oop)
			sm.dup[op.ClientId] = op.SerialId
			return false, "", oop.Config
		} else {
			DPrintf("%d :wrongleader", sm.me)
			return true, "", resConfig
		}

	case <-time.After(500 * time.Millisecond):
		DPrintf("%d :start timeout", sm.me)
		return false, ErrTimeOut, resConfig
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	DPrintf("%d :杀死一个 server", sm.me)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	sm.apps = make(map[int]chan Op)
	sm.dup = make(map[int64]int)
	// Your code here.
	go sm.apply()
	return sm
}

func (sm *ShardMaster) initConfig() Config {
	config := Config{}
	config.Num = 0
	for i := 0; i < NShards; i++ {
		config.Shards[i] = 0
	}
	config.Groups = make(map[int][]string)
	return config
}
func (sm *ShardMaster) apply() {
	for {
		sm.mu.Lock()
		st := sm.killed()
		sm.mu.Unlock()
		if st {
			return
		}
		msg := <-sm.applyCh
		if msg.Command == nil {
			DPrintf("%d : this is nil", sm.me)
			continue
		}
		sm.mu.Lock()
		op := msg.Command.(Op)
		op.Config = sm.initConfig()
		sm.copyConfig(&op.Config, &sm.configs[len(sm.configs)-1])
		op.Config.Num++
		if sm.checkDup(op.ClientId, op.SerialId) {

			switch op.Type {
			case Join:

				for k, v := range op.Servers {
					op.Config.Groups[k] = make([]string, len(v))
					for i := 0; i < len(v); i++ {
						op.Config.Groups[k][i] = v[i]
					}
				}
				sm.loadBalance(&op.Config)
				sm.configs = append(sm.configs, op.Config)
				DPrintf("%d : join config", sm.me)
			case Leave:
				for _, v := range op.GIDs {
					delete(op.Config.Groups, v)
				}
				sm.loadBalance(&op.Config)
				sm.configs = append(sm.configs, op.Config)
				DPrintf("%d : leave config", sm.me)
			case Move:
				//args := op.Args.(MoveArgs)
			case Query:
				num := op.Num
				if num < 0 || num >= len(sm.configs)-1 {
					op.Config = sm.configs[len(sm.configs)-1]
				} else {
					op.Config = sm.configs[num]
				}
			}

			ch, ok := sm.apps[msg.CommandIndex]
			if ok {
				ch <- op
			}
			sm.mu.Unlock()
		}
	}
}

func (sm *ShardMaster) checkDup(id int64, seq int) bool {
	//no dup = true
	res, ok := sm.dup[id]
	if ok {
		return seq > res
	}
	return true
}

func (sm *ShardMaster) copyConfig(newConfig *Config, oldConfig *Config) {
	newConfig.Num = oldConfig.Num
	for i := 0; i < NShards; i++ {
		newConfig.Shards[i] = oldConfig.Shards[i]
	}
	for k, v := range oldConfig.Groups {
		newConfig.Groups[k] = make([]string, len(v))
		for j := 0; j < len(v); j++ {
			newConfig.Groups[k][j] = v[j]
		}

	}
}

func (sm *ShardMaster) loadBalance(config *Config) {
	GNum := len(config.Groups)
	if GNum <= 0 {
		return
	}

	temp := make([]int, 0)
	for k, _ := range config.Groups {
		temp = append(temp, k)
	}
	cur := 0
	for i := 0; i < NShards; i++ {
		config.Shards[i] = temp[cur]
		cur++
		cur = cur % len(temp)
	}
}
