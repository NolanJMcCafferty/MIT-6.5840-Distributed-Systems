package shardctrler

//
// Shardctrler clerk.
//

import (
	"6.5840/labrpc"
)
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	servers []*labrpc.ClientEnd
	id int64
	requestNum int
	currentLeader int
}

const RequestWaitInterval = 100 * time.Millisecond

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

func (ck *Clerk) Query(num int) Config {
	ck.requestNum++

	args := &QueryArgs{}
	args.ClientId = ck.id
	args.RequestNum = ck.requestNum
	args.Num = num

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardCtrler.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				return reply.Config
			}
		}
		time.Sleep(RequestWaitInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.requestNum++

	args := &JoinArgs{}
	args.ClientId = ck.id
	args.RequestNum = ck.requestNum
	args.Servers = servers

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardCtrler.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(RequestWaitInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.requestNum++

	args := &LeaveArgs{}
	args.ClientId = ck.id
	args.RequestNum = ck.requestNum
	args.GIDs = gids

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardCtrler.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(RequestWaitInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.requestNum++

	args := &MoveArgs{}
	args.ClientId = ck.id
	args.RequestNum = ck.requestNum
	args.Shard = shard
	args.GID = gid

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardCtrler.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				return
			}
		}
		time.Sleep(RequestWaitInterval)
	}
}
