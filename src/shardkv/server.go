package shardkv

// import "../shardmaster"
import (
	"bytes"
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
	Type     string
	Key      string
	Value    string
	ClientId int64
	SerialId int
}

type ShardKV struct {
	mu           sync.Mutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	dead int32 // set by Kill()
	data map[string]string
	apps map[int]chan Op
	dup  map[int64]int

	lastIncludedIndex int
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if isLeader {
		op := Op{"Get", args.Key, "", args.ClientId, args.SerialId}
		reply.Value, reply.Err = kv.start(op)
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	_, isLeader := kv.rf.GetState()
	if isLeader {
		op := Op{args.Op, args.Key, args.Value, args.ClientId, args.SerialId}
		_, Err := kv.start(op)
		reply.Err = Err
		return
	} else {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) start(op Op) (string, Err) {
	kv.mu.Lock()
	DPrintf("%d start %d", kv.me, op)
	//检查put重复或是否直接返回get
	if res, ok := kv.dup[op.ClientId]; ok && res >= op.SerialId {
		res := ""
		if op.Type == "Get" {
			res = kv.data[op.Key]
		}
		defer kv.mu.Unlock()
		return res, OK
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		defer kv.mu.Unlock()
		return "", ErrWrongLeader
	}
	ch := make(chan Op, 1)
	kv.apps[index] = ch
	kv.mu.Unlock()
	defer func() {
		kv.mu.Lock()
		delete(kv.apps, index)
		kv.mu.Unlock()
	}()
	select {
	case oop := <-ch:
		//返回成功
		if op.ClientId == oop.ClientId && op.SerialId == oop.SerialId {
			DPrintf("return op ", oop)
			res := oop.Value
			return res, OK
		} else {
			DPrintf("%d :wrongleader", kv.me)
			return "", ErrWrongLeader
		}

	case <-time.After(500 * time.Millisecond):
		DPrintf("%d :start timeout", kv.me)
		return "", ErrTimeOut
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	DPrintf("%d :杀死一个 server", kv.me)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.data = make(map[string]string)
	kv.apps = make(map[int]chan Op)
	kv.dup = make(map[int64]int)
	kv.maxraftstate = maxraftstate
	kv.LoadSnapshot(kv.rf.GetSnapshots())
	DPrintf("%d :init finished", kv.me)
	// You may need initialization code here.
	go kv.apply()

	return kv
}

func (kv *ShardKV) apply() {
	for {
		kv.mu.Lock()
		st := kv.killed()
		kv.mu.Unlock()
		if st {
			return
		}
		msg := <-kv.applyCh
		//msg.CommandValid is true,otherwise the command is snapshot
		if msg.CommandValid {
			kv.mu.Lock()
			op := msg.Command.(Op)
			//判断是否重复指令
			DPrintf("%d server get Command %d", kv.me, msg)
			if res, ok := kv.dup[op.ClientId]; !ok || (ok && op.SerialId > res) {
				switch op.Type {
				case "Put":
					kv.data[op.Key] = op.Value
				case "Append":
					DPrintf("%d :append 之前是", kv.me, kv.data[op.Key])
					DPrintf("%d :index 之前是", kv.me, msg.CommandIndex)
					kv.data[op.Key] = kv.data[op.Key] + op.Value
					DPrintf("%d :append %d res is ", kv.me, op.Value, kv.data[op.Key])
				}
				kv.dup[op.ClientId] = op.SerialId
			} else {
				DPrintf("重复指令 op.ser = %d, dup[i] = %d", op.SerialId, res)
			}
			if op.Type == "Get" {
				op.Value = kv.data[op.Key]
			}
			ch, ok := kv.apps[msg.CommandIndex]
			if ok {
				ch <- op
			}

			// 判断是否达到max
			kv.checkMaxState(msg.CommandIndex)
			kv.mu.Unlock()
		} else {
			DPrintf("server 0000")
			data := msg.Command.([]byte)
			index := msg.CommandIndex
			if index <= kv.lastIncludedIndex {
				DPrintf("server ::: %d - %d", index, kv.lastIncludedIndex)
				kv.rf.SnapshotF <- -1
				continue
			}
			DPrintf("index :: %d ,rf.ls :%d", index, kv.lastIncludedIndex)
			kv.LoadSnapshot(data)
			DPrintf("server %d :lastIndex", kv.lastIncludedIndex)
			kv.mu.Lock()
			kv.SnapshotPersister(index)
			kv.mu.Unlock()
			DPrintf("server 111")
			kv.rf.SnapshotF <- 1
			DPrintf("server 111")
		}
	}
}

func (kv *ShardKV) checkMaxState(commitIndex int) {
	if kv.maxraftstate == -1 {
		return
	}
	if kv.maxraftstate*9/10 > kv.rf.GetStateSize() {
		return
	}
	kv.SnapshotPersister(commitIndex - 1)
}
func (kv *ShardKV) encodeSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.data)
	e.Encode(kv.dup)
	e.Encode(kv.lastIncludedIndex)
	snapshot := w.Bytes()
	return snapshot
}

func (kv *ShardKV) SnapshotPersister(index int) {
	kv.rf.SaveSnapshot(kv.encodeSnapshot())
	kv.rf.Discard(index)
	kv.lastIncludedIndex = index
}

func (kv *ShardKV) LoadSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var data map[string]string
	var dup map[int64]int
	var lastIncludedIndex int
	if d.Decode(&data) != nil ||
		d.Decode(&dup) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		DPrintf("%d:load 之前：%d", kv.me, kv.data)
		DPrintf("%d:之后：%d", kv.me, data)
		kv.data = data
		kv.dup = dup
		kv.lastIncludedIndex = lastIncludedIndex
	}
}
