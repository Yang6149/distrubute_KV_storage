package shardkv

// import "../shardmaster"
import (
	"bytes"
	"fmt"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
	"../shardmaster"
)

const Debug = 1

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

type Shard struct {
	Id      int
	Data    map[string]string
	Dup     map[int64]int
	Version int
}
type GC struct {
	Shard   int
	Version int
}

type MigrateArgs struct {
	Shard     Shard
	ConfigNum int
}
type MigrateReply struct {
	Err Err
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
	sm           *shardmaster.Clerk
	config       shardmaster.Config
	// Your definitions here.
	dead          int32 // set by Kill()
	apps          map[int]chan Op
	appsforConfig map[int]chan shardmaster.Config
	appsforShard  map[int]chan Shard
	appsforGC     map[int]chan GC
	shards        map[int]Shard

	GCch              chan GC
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
		res, Err := kv.start(op)
		fmt.Println(kv.gid, res, "this is res value")
		reply.Err = Err
		fmt.Println(Err)
		return
	} else {
		reply.Err = ErrWrongLeader
		return
	}
}

func (kv *ShardKV) start(op Op) (string, Err) {
	kv.mu.Lock()
	//判断是否改变了配置而拒绝请求
	shard := key2shard(op.Key)
	gid := kv.config.Shards[shard]
	if s, ok := kv.shards[shard]; gid != kv.gid || !ok || s.Version != kv.config.Num {
		//gid 对不上、不存在该 shard、存在但是不可用状态
		defer kv.mu.Unlock()
		fmt.Println(kv.gid, kv.me, gid != kv.gid, !ok, s.Version != kv.config.Num, s.Version, kv.config.Num)
		return "", ErrWrongGroup
	}
	//检查put重复或是否直接返回get
	shardId := key2shard(op.Key)
	if res, ok := kv.shards[shardId].Dup[op.ClientId]; ok && res >= op.SerialId {
		res := ""
		if op.Type == "Get" {
			res = kv.shards[shardId].Data[op.Key]
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
			res := oop.Value
			return res, OK
		} else {
			return "", ErrWrongLeader
		}

	case <-time.After(500 * time.Millisecond):
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
	DPrintf("%d %d :杀死一个 server", kv.gid, kv.me)
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
	kv.sm = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.apps = make(map[int]chan Op)
	kv.appsforConfig = make(map[int]chan shardmaster.Config)
	kv.appsforGC = make(map[int]chan GC)
	kv.appsforShard = make(map[int]chan Shard)
	kv.GCch = make(chan GC, 100)
	kv.shards = make(map[int]Shard)
	kv.initShard()
	kv.maxraftstate = maxraftstate
	kv.LoadSnapshot(kv.rf.GetSnapshots())
	DPrintf("%d :init finished", kv.me)

	//labgob init
	labgob.Register(MigrateArgs{})
	labgob.Register(Shard{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(GC{})
	// You may need initialization code here.
	go kv.apply()
	go kv.fetchLatestConfig()
	go kv.detectConfig()
	go kv.GCDeamon()
	return kv
}

func (kv *ShardKV) initShard() {
	for i := 0; i < shardmaster.NShards; i++ {
		res := Shard{}
		res.Version = 0
		res.Data = make(map[string]string)
		res.Dup = make(map[int64]int)
		kv.shards[i] = res
	}
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
			switch command := msg.Command.(type) {
			case Op:
				op := command
				shardId := key2shard(op.Key)
				if res, ok := kv.shards[shardId].Dup[op.ClientId]; !ok || (ok && op.SerialId > res) {
					switch op.Type {
					case "Put":
						kv.shards[shardId].Data[op.Key] = op.Value
						DPrintf("%d %d put %s -> %s ", kv.gid, kv.me, op.Key, op.Value)
					case "Append":
						kv.shards[shardId].Data[op.Key] = kv.shards[shardId].Data[op.Key] + op.Value
						op.Value = kv.shards[shardId].Data[op.Key]
						DPrintf("%d %d append %s -> %s ", kv.gid, kv.me, op.Key, kv.shards[shardId].Data[op.Key]+op.Value)
					}
					kv.shards[shardId].Dup[op.ClientId] = op.SerialId
				} else {
				}
				if op.Type == "Get" {
					op.Value = kv.shards[shardId].Data[op.Key]
				}
				ch, ok := kv.apps[msg.CommandIndex]
				if ok {
					ch <- op
				}

				// 判断是否达到max
				kv.checkMaxState(msg.CommandIndex)
			case shardmaster.Config:
				config := command
				if config.Num > kv.config.Num {
					//为那些没有改变shard 的 version 进行同步
					for i := 0; i < shardmaster.NShards; i++ {
						if config.Shards[i] == kv.gid && kv.config.Shards[i] == kv.gid {
							shard := kv.shards[i]
							shard.Version = config.Num
						}
					}
					if kv.config.Num == 0 {
						for i, _ := range config.Shards {
							if config.Shards[i] == kv.gid {
								shard := Shard{}
								shard.Id = i
								shard.Data = make(map[string]string)
								shard.Dup = make(map[int64]int)
								shard.Version = config.Num
								kv.shards[i] = shard
							}
						}
					}

					kv.config = config
					DPrintf("%d %d 更新 config%d ", kv.gid, kv.me, config)
				} else {
					DPrintf("ignore 小于当前 config 的 config")
				}
				ch, ok := kv.appsforConfig[msg.CommandIndex]
				if ok {
					ch <- config
				}
			case Shard:
				shard := command
				if shard.Version > kv.shards[shard.Id].Version {
					kv.shards[shard.Id] = shard
					DPrintf("%d %d 更新 shard%d ", kv.gid, kv.me, shard)
					// for k, _ := range kv.shards {
					// 	if kv.shards[k].Version == 0 {
					// 		fmt.Println("********************************")
					// 	}
					// }
				}
				ch, ok := kv.appsforShard[msg.CommandIndex]
				if ok {
					ch <- shard
				}
			case GC:
				gc := command
				shard, ok := kv.shards[gc.Shard]
				if ok {
					//版本正好正确
					if shard.Version == gc.Version {
						DPrintf("%d %d gc shard %d ", kv.gid, kv.me, gc.Shard)
						delete(kv.shards, gc.Shard)
					} else {
						DPrintf("%d %d 你想gc%d 版本号为 %d ，我的版本号为 %d", kv.gid, kv.me, gc.Shard, gc.Version, shard.Version)
					}
				}
				ch, ok := kv.appsforGC[msg.CommandIndex]
				if ok {
					ch <- gc
				}
			}
			kv.mu.Unlock()
			//判断是否重复指令

		} else {
			data := msg.Command.([]byte)
			index := msg.CommandIndex
			if index <= kv.lastIncludedIndex {
				kv.rf.SnapshotF <- -1
				continue
			}
			kv.LoadSnapshot(data)
			kv.mu.Lock()
			kv.SnapshotPersister(index)
			kv.mu.Unlock()
			kv.rf.SnapshotF <- 1
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
	e.Encode(kv.shards)
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
	var shards map[int]Shard
	var lastIncludedIndex int
	if d.Decode(&shards) != nil ||
		d.Decode(&lastIncludedIndex) != nil {
	} else {
		kv.mu.Lock()
		defer kv.mu.Unlock()
		kv.shards = shards
		kv.lastIncludedIndex = lastIncludedIndex
	}
}

func (kv *ShardKV) fetchLatestConfig() {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			kv.mu.Lock()
			config := kv.sm.Query(-1)
			if !kv.check_same_config(config, kv.config) {
				index, _, _ := kv.rf.Start(config)
				ch := make(chan shardmaster.Config, 1)
				kv.appsforConfig[index] = ch
				defer func() {
					kv.mu.Lock()
					delete(kv.appsforConfig, index)
					kv.mu.Unlock()
				}()
				select {
				case <-ch:
					//其实我觉得这里没差，因为如果修改成功的话，就不会进入判断重复的这里了，所以等会把这个删掉
					//成功
				case <-time.After(1000 * time.Millisecond):
					//没成功
				}
			} else {
				//DPrintf("%d %d 没变哟", kv.gid, kv.me)
			}
			kv.mu.Unlock()
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) detectConfig() {
	for {
		select {
		case <-time.After(50 * time.Millisecond):
			_, isLeader := kv.rf.GetState()
			if isLeader {
				kv.mu.Lock()
				//检查当前 shard 是否发送了
				for i := 0; i < shardmaster.NShards; i++ {
					_, ok := kv.shards[i]
					if kv.config.Shards[i] != kv.gid && ok {
						//拥有 shard 但config上指示我应该没有，所以我要把它发送给有用的人。再删除掉
						go kv.sendMigration(kv.config.Shards[i], i)
					}
				}
				kv.mu.Unlock()
			}

		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) check_same_config(c1 shardmaster.Config, c2 shardmaster.Config) bool {
	if c1.Num != c2.Num {
		return false
	}
	return true
}

func (kv *ShardKV) sendMigrateArgs(cli *labrpc.ClientEnd, args *MigrateArgs, reply *MigrateReply) bool {
	ok := cli.Call("ShardKV.MigrateReply", args, reply)
	return ok
}

func (kv *ShardKV) MigrateReply(args *MigrateArgs, reply *MigrateReply) {
	_, isLeader := kv.rf.GetState()
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}
	if args.ConfigNum < kv.config.Num {
		reply.Err = "stale Shard"
		return
	}
	if shard, ok := kv.shards[args.Shard.Id]; ok && shard.Version == args.Shard.Version {
		//已经拥有该shard ，判断重复直接返回确认。
		reply.Err = OK
		return
	}
	index, _, _ := kv.rf.Start(args.Shard)
	ch := make(chan Shard, 1)
	kv.appsforShard[index] = ch
	defer func() {
		kv.mu.Lock()
		delete(kv.appsforShard, index)
		kv.mu.Unlock()
	}()
	select {
	case <-ch:
		reply.Err = OK
		return
	case <-time.After(2000 * time.Millisecond):
		reply.Err = ErrTimeOut
		return
	}
}

// func (kv *ShardKV) updateDataAndDup(data map[string]string, dup map[int64]int) {

// }

func (kv *ShardKV) sendMigration(gid int, shard int) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	if servers, ok := kv.config.Groups[gid]; ok {
		for si := 0; si < len(servers); si++ {
			srv := kv.make_end(servers[si])
			var args MigrateArgs
			args.Shard = kv.copyOfShard(shard)
			shardVersion := args.Shard.Version
			args.ConfigNum = kv.config.Num

			var reply MigrateReply
			ok := kv.sendMigrateArgs(srv, &args, &reply)
			if ok && reply.Err == OK {
				//成功发送Migration ，开始删除
				//delete shard[i]
				if kv.gid == 102 {
					fmt.Println("99999999999999999999999999999999999999999", gid)
				}
				gc := GC{}
				gc.Shard = shard
				gc.Version = shardVersion
				kv.GCch <- gc

			} else {
				if reply.Err == ErrWrongLeader {
					continue
				}
				DPrintf("%d %d 发送 migration ：%d", kv.gid, kv.me, reply.Err)
				go kv.sendMigration(gid, shard)
			}
			// ... not ok, or ErrWrongLeader
		}
	}
}

func (kv *ShardKV) copyOfShard(i int) Shard {
	res := Shard{}
	for k, v := range kv.shards[i].Data {
		res.Data[k] = v
	}
	for k, v := range kv.shards[i].Dup {
		res.Dup[k] = v
	}
	res.Id = kv.shards[i].Id
	res.Version = kv.shards[i].Version
	return res
}

func (kv *ShardKV) GC(shard int, version int) Err {
	gc := GC{}
	gc.Shard = shard
	gc.Version = version
	index, _, isLeader := kv.rf.Start(gc)
	if !isLeader {
		return ErrWrongLeader
	}
	ch := make(chan GC, 1)
	kv.appsforGC[index] = ch
	defer func() {
		kv.mu.Lock()
		delete(kv.appsforGC, index)
		kv.mu.Unlock()
	}()
	select {
	case <-ch:
		//gc 成功
		return OK
	case <-time.After(1000 * time.Millisecond):
		return ErrTimeOut
	}

}
func (kv *ShardKV) GCDeamon() {
	for {
		//信号量
		gc := <-kv.GCch
		Err := kv.GC(gc.Shard, gc.Version)
		if Err != OK {
			kv.GCch <- gc
		}
	}
}
