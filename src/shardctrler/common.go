package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 30

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

const (
	JOIN 	OpType = "Join"
	LEAVE 	OpType = "Leave"
	MOVE 	OpType = "Move"
	QUERY 	OpType = "Query"
)
type OpType string

type Op struct {
	Type OpType
	ClientId int64
	RequestNum int
	GIDs []int
	Servers map[int][]string
	Shard int
	GID int
	ConfigNum int
}

type Err string

type JoinArgs struct {
	ClientId int64
	RequestNum int
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	ClientId int64
	RequestNum int
	GIDs []int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	ClientId int64
	RequestNum int
	Shard int
	GID   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	ClientId int64
	RequestNum int
	Num int // desired config number
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type RequestResult struct {
	RequestNum int
	Value Config
}

func contains(values []int, value int) bool {
	for _, val := range values {
		if val == value {
			return true
		}
	}

	return false
}

func union(map1 map[int][]string, map2 map[int][]string) map[int][]string {
	newMap := map[int][]string{}

	for key, value := range map1 {
		newMap[key] = value
	}
	for key, value := range map2 {
		newMap[key] = value
	}

	return newMap
}

func removeKeys(map1 map[int][]string, keys []int) map[int][]string {
	newMap := map[int][]string{}

	for key, values := range map1 {
		if !contains(keys, key) {
			newMap[key] = values
		}
	}

	return newMap
}