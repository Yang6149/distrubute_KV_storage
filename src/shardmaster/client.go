package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	id       int64
	serialId int
	leader   int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = nrand()
	ck.serialId = 0

	// Your code here.
	return ck
}

func (ck *Clerk) Query(num int) Config {
	//如果多个client 同时发起命令还想保持线性化结果，就不能注释掉下一行
	//ck.serialId++
	args := &QueryArgs{Num: num}
	// Your code here.
	args.Num = num
	args.ClientId = ck.id
	args.SerialId = ck.serialId
	for {
		// try each known server.
		srv := ck.servers[ck.leader]
		var reply QueryReply
		ok := srv.Call("ShardMaster.Query", args, &reply)
		if ok && reply.WrongLeader == false {
			return reply.Config
		}
		time.Sleep(100 * time.Millisecond)
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.serialId++
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.id
	args.SerialId = ck.serialId
	for {
		// try each known server.
		srv := ck.servers[ck.leader]
		var reply JoinReply
		ok := srv.Call("ShardMaster.Join", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		time.Sleep(100 * time.Millisecond)
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.serialId++
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.id
	args.SerialId = ck.serialId
	for {
		// try each known server.
		srv := ck.servers[ck.leader]
		var reply LeaveReply
		ok := srv.Call("ShardMaster.Leave", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		time.Sleep(100 * time.Millisecond)
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.serialId++
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.id
	args.SerialId = ck.serialId
	for {
		// try each known server.
		srv := ck.servers[ck.leader]
		var reply MoveReply
		ok := srv.Call("ShardMaster.Move", args, &reply)
		if ok && reply.WrongLeader == false {
			return
		}
		time.Sleep(100 * time.Millisecond)
		ck.leader = (ck.leader + 1) % len(ck.servers)
	}
}
