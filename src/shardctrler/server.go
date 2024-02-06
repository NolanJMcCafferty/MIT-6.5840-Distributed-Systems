package shardctrler


import (
	"6.5840/raft"
	"sort"
	"time"
)
import "6.5840/labrpc"
import "sync"
import "6.5840/labgob"


type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	clientRequests map[int64]RequestResult
	configs        []Config
}

const WaitInterval = 10 * time.Millisecond

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	op := Op{
		Type: 		JOIN,
		ClientId: 	args.ClientId,
		RequestNum: args.RequestNum,
		Servers: 	args.Servers,
	}

	reply.WrongLeader = sc.applyOp(op)
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	op := Op{
		Type: 		LEAVE,
		ClientId: 	args.ClientId,
		RequestNum: args.RequestNum,
		GIDs: 		args.GIDs,
	}

	reply.WrongLeader = sc.applyOp(op)
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	op := Op{
		Type: 		MOVE,
		ClientId: 	args.ClientId,
		RequestNum: args.RequestNum,
		Shard: 		args.Shard,
		GID: 		args.GID,
	}

	reply.WrongLeader = sc.applyOp(op)
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	op := Op{
		Type: 		QUERY,
		ClientId: 	args.ClientId,
		RequestNum: args.RequestNum,
		ConfigNum: 	args.Num,
	}

	reply.WrongLeader = sc.applyOp(op)

	if !reply.WrongLeader {
		sc.mu.Lock()
		defer sc.mu.Unlock()

		reply.Config = sc.clientRequests[args.ClientId].Value
	}
}

func (sc *ShardCtrler) applyOp(op Op) bool {
	sc.mu.Lock()
	defer sc.mu.Unlock()

	if result, keyExists := sc.clientRequests[op.ClientId]; keyExists {
		if op.RequestNum <= result.RequestNum {
			return false
		}
	}

	sc.mu.Unlock()

	_, term, _ := sc.rf.Start(op)

	for {
		sc.mu.Lock()

		currentTerm, stillLeader := sc.rf.GetState()

		if !stillLeader || currentTerm != term {
			return true
		}

		if result, exists := sc.clientRequests[op.ClientId] ; exists && result.RequestNum == op.RequestNum {
			break
		}

		sc.mu.Unlock()
		time.Sleep(WaitInterval)
	}

	return false
}

// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) handleAppliedOps() {
	for applyChMsg := range sc.applyCh {
		sc.mu.Lock()

		op := applyChMsg.Command.(Op)

		if !sc.isApplied(op) {
			sc.clientRequests[op.ClientId] = RequestResult{
				RequestNum: op.RequestNum,
				Value:      sc.getConfig(op.ConfigNum),
			}

			switch op.Type {
			case JOIN:
				newShards := sc.balanceShardsAfterJoin(op.Servers)
				newGroups := union(sc.latestConfig().Groups, op.Servers)

				sc.createNewConfig(newShards, newGroups)
				break
			case LEAVE:
				newShards := sc.balanceShardsAfterLeave(op.GIDs)
				newGroups := removeKeys(sc.latestConfig().Groups, op.GIDs)

				sc.createNewConfig(newShards, newGroups)
				break
			case MOVE:
				previousShards := sc.latestConfig().Shards
				previousShards[op.Shard] = op.GID

				sc.createNewConfig(previousShards, sc.latestConfig().Groups)
				break
			}
		}
		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) isApplied(op Op) bool {
	result, exists := sc.clientRequests[op.ClientId]

	return exists && op.RequestNum <= result.RequestNum
}

func (sc *ShardCtrler) getConfig(num int) Config {
	if num < 0 || num > len(sc.configs) - 1 {
		return sc.latestConfig()
	}

	return sc.configs[num]
}

func (sc *ShardCtrler) latestConfig() Config {
	return sc.configs[len(sc.configs) - 1]
}

func (sc *ShardCtrler) balanceShardsAfterJoin(servers map[int][]string) [NShards]int {
	var newShards [NShards]int
	for shard, gid := range sc.latestConfig().Shards {
		newShards[shard] = gid
	}

	keys := make([]int, 0, len(servers))
	for k := range servers {
		keys = append(keys, k)
	}
	sort.Ints(keys)

	numNewGroups := 0
	for _, gid := range keys {
		numNewGroups++
		newShards = sc.addGroupToShards(gid, len(sc.latestConfig().Groups) + numNewGroups, newShards)
	}

	return newShards
}

func (sc *ShardCtrler) addGroupToShards(
	newGroup int,
	numGroups int,
	previousShards [NShards]int,
) [NShards]int {
	var newShards [NShards]int

	if numGroups == 1 {
		for index, _ := range newShards {
			newShards[index] = newGroup
		}

		return newShards
	}

	for shard, gid := range previousShards {
		newShards[shard] = gid
	}

	shardsPerGroup := NShards / numGroups
	if shardsPerGroup < 1 {
		shardsPerGroup = 1
	}

	shardsForNewGroup := 0

	for shardsForNewGroup < shardsPerGroup {
		newShards = sc.assignShardToGroup(newGroup, newShards)
		shardsForNewGroup++
	}

	return newShards
}

func (sc *ShardCtrler) assignShardToGroup(newGroup int, shards [NShards]int) [NShards]int {
	groupWithMostShards := sc.findGroupWithMostShards(shards)

	for index, group := range shards {
		if group == groupWithMostShards {
			shards[index] = newGroup
			break
		}
	}

	return shards
}

func (sc *ShardCtrler) findGroupWithMostShards(shards [NShards]int) int {
	maxShards := 0
	groupWithMostShards := 0
	shardsPerGroup := make(map[int]int)

	for _, group := range shards {
		shardsPerGroup[group] = shardsPerGroup[group] + 1

		if shardsPerGroup[group] > maxShards {
			maxShards = shardsPerGroup[group]
			groupWithMostShards = group
		}
	}

	return groupWithMostShards
}

func (sc *ShardCtrler) balanceShardsAfterLeave(groups []int) [NShards]int {
	var newShards [NShards]int

	for shard, gid := range sc.latestConfig().Shards {
		newShards[shard] = gid
	}

	for _, gid := range groups {
		newShards = sc.removeGroupFromShards(gid, newShards)
	}

	return newShards
}

func (sc *ShardCtrler) removeGroupFromShards(
	group int,
	shards [NShards]int,
) [NShards]int {
	var newShards [NShards]int

	for shard, gid := range shards {
		newShards[shard] = gid
	}

	for contains(newShards[:], group) {
		newShards = replaceGroupInShards(newShards, group)
	}

	return newShards
}

func replaceGroupInShards(shards [NShards]int, newGroup int) [NShards]int {
	groupWithLeastShards := findGroupWithLeastShards(shards, newGroup)

	for index, gid := range shards {
		if gid == newGroup {
			shards[index] = groupWithLeastShards
			break
		}
	}

	return shards
}

func findGroupWithLeastShards(shards [NShards]int, excludedGroup int) int {
	minShards := len(shards)
	groupWithLeastShards := 0
	shardsPerGroup := make(map[int]int)

	for _, group := range shards {
		shardsPerGroup[group] = shardsPerGroup[group] + 1
	}

	for _, group := range shards {
		if shardsPerGroup[group] < minShards && group != excludedGroup {
			minShards = shardsPerGroup[group]
			groupWithLeastShards = group
		}
	}

	return groupWithLeastShards
}

func (sc *ShardCtrler) createNewConfig(shards [NShards]int, groups map[int][]string) {
	newConfig := Config{
		Num:    len(sc.configs),
		Shards: shards,
		Groups: groups,
	}

	sc.configs = append(sc.configs, newConfig)
}

// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.clientRequests = make(map[int64]RequestResult)

	go sc.handleAppliedOps()

	return sc
}