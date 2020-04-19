package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	me      int64
	serial  int
	leader  int
	// You will have to modify this struct.
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
	ck.me = nrand()
	ck.serial = 0
	ck.leader = 0
	// You'll have to add code here.
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{}
	args.Key = key
	num := len(ck.servers)
	i := ck.leader
	for {
		DPrintf("get loop")
		reply := GetReply{}
		ok := ck.servers[i].Call("KVServer.Get", &args, &reply)

		// You will have to modify this function.
		if reply.Err == OK && ok {
			DPrintf("%d :key :(%s) get value (%s)", i, args.Key, reply.Value)
			DPrintf("get success")
			ck.leader = i
			return reply.Value
		} else {
			DPrintf("key :(%s) get value (%s), Err is %s", args.Key, reply.Value, reply.Err)
		}
		i++
		i = i % num
		time.Sleep(200 * time.Millisecond)
	}
	return ""
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
// ctx, _ := context.WithTimeout(context.Background(), m.timeout)
// 				go func() {
// 					select {
// 					case <-ctx.Done():
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.serial++

	args := PutAppendArgs{}
	args.SerialID = ck.serial
	args.ClientId = ck.me
	args.Key = key
	args.Value = value
	args.Op = op
	num := len(ck.servers)
	target := ck.leader
	for {
		reply := PutAppendReply{}

		rep := make(chan bool)
		go func() {
			ok := ck.servers[target].Call("KVServer.PutAppend", &args, &reply)
			rep <- ok
		}()
		//DPrintf("put  %d ", target)
		select {
		case ok := <-rep:
			if reply.Err == OK && ok {
				DPrintf("%d : send (%s):(%s)", target, args.Key, args.Value)
				DPrintf("put success")
				ck.leader = target
				return
			} else if reply.Err == ErrWrongLeader {

			}
		case <-time.After(600 * time.Millisecond):
			DPrintf("timeout request to  %d ", target)
		}
		target = (target + 1) % num

	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
