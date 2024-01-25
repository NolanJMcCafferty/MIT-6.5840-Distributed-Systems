package kvraft

import (
	"6.5840/labrpc"
	"time"
)
import "crypto/rand"
import "math/big"


type Clerk struct {
	servers []*labrpc.ClientEnd
	id int64
	requestNum int
	currentLeader int
}

const WaitInterval = 10 * time.Millisecond

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
	ck.requestNum = 1

	return ck
}

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
func (ck *Clerk) Get(key string) string {
	ck.requestNum++

	args := GetArgs{
		key,
		ck.id,
		ck.requestNum,
	}

	serverIndex := ck.currentLeader
	for {
		reply := GetReply{}

		ok := ck.servers[serverIndex % len(ck.servers)].Call("KVServer.Get", &args, &reply)

		if ok && reply.Err == OK {
			ck.currentLeader = serverIndex % len(ck.servers)
			return reply.Value
		}

		serverIndex++
		time.Sleep(WaitInterval)
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	ck.requestNum++

	args := PutAppendArgs{
		key,
		value,
		op,
		ck.id,
		ck.requestNum,
	}

	serverIndex := ck.currentLeader
	for {
		reply := PutAppendReply{}

		ok := ck.servers[serverIndex % len(ck.servers)].Call("KVServer.PutAppend", &args, &reply)

		if ok && reply.Err == OK {
			ck.currentLeader = serverIndex % len(ck.servers)
			return
		}

		serverIndex++
		time.Sleep(WaitInterval)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
