package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key      string
	Value    string
	ClientId int64
	SerialId int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()
	data    map[string]string
	apps    map[int]chan Op
	dup     map[int64]int

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	_, isLeader := kv.rf.GetState()
	if isLeader {
		key := args.Key
		value := kv.data[key]
		reply.Value = value
		if _, ok := kv.data[key]; !ok {
			reply.Err = ErrNoKey
			return
		}
		reply.Err = OK
	} else {
		reply.Err = ErrWrongLeader
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	//处理重复put
	if res, ok := kv.dup[args.ClientId]; ok && res >= args.SerialID {
		reply.Err = OK
		return
	}
	//handle args
	key := args.Key
	value := args.Value
	op := args.Op
	if op == "Append" {
		value = kv.data[key] + value
	}
	command := Op{key, value, args.ClientId, args.SerialID}
	//send command
	kv.mu.Lock()
	index, _, isLeader := kv.rf.Start(command)
	if !isLeader {
		reply.Err = ErrWrongLeader
		kv.mu.Unlock()
		return
	}
	kv.apps[index] = make(chan Op, 1)
	kv.mu.Unlock()
	//等待返回数据
	select {
	case command := <-kv.apps[index]:

		DPrintf("%d apply %d", kv.me, command)
		reply.Err = OK
		DPrintf("insert a command %d", command)
	case <-time.After(500 * time.Millisecond):
		reply.Err = ErrTimeOut
	}

	// Your code here.
}

//s
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("%d :杀死一个 server", kv.me)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	kv.data = make(map[string]string)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.apps = make(map[int]chan Op)
	kv.dup = make(map[int64]int)
	DPrintf("%d :init finished", kv.me)
	// You may need initialization code here.
	go kv.apply()
	return kv
}

func (kv *KVServer) apply() {
	for {
		msg := <-kv.applyCh
		//msg.CommandValid is true,otherwise the command is snapshot
		if msg.CommandValid {
			command := msg.Command.(Op)
			index := msg.CommandIndex
			DPrintf("%d transBack %d", kv.me, command)
			kv.data[command.Key] = command.Value
			kv.dup[command.ClientId] = command.SerialId
			res, ok := kv.apps[index]
			if ok {
				res <- command
			}
		}
	}
}
