package kvraft

import (
	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raft"
	"bytes"
	"sync"
	"sync/atomic"
	"time"
)


type Op struct {
	Type string
	Key string
	Value string
	ClientId int64
	RequestNum int
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxRaftState int // snapshot if log grows this big
	persister *raft.Persister

	lastAppliedIndex int
	dbMap          	map[string]string
	requests		map[int64]RequestResult
}

const SnapshotBytesBuffer = 100

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if result, keyExists := kv.requests[args.ClientId]; keyExists {
		if args.RequestNum <= result.RequestNum {
			reply.Err = OK
			reply.Value = result.Value

			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()

	op := Op{
		"Get",
		args.Key,
		"",
		args.ClientId,
		args.RequestNum,
	}

	reply.Err = kv.applyOp(op)

	kv.mu.Lock()
	defer kv.mu.Unlock()

	if reply.Err == OK {
		reply.Value = kv.requests[args.ClientId].Value
	}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.mu.Lock()

	if result, keyExists := kv.requests[args.ClientId]; keyExists {
		if args.RequestNum <= result.RequestNum {
			reply.Err = OK

			kv.mu.Unlock()
			return
		}
	}

	kv.mu.Unlock()

	op := Op{
		args.Op,
		args.Key,
		args.Value,
		args.ClientId,
		args.RequestNum,
	}

	reply.Err = kv.applyOp(op)
}

// sends the operation to Raft
// and waits for it to be committed in order to return
func(kv *KVServer) applyOp(op Op) Err {
	_, term, _ := kv.rf.Start(op)

	for {
		if kv.killed() {
			return ErrWrongLeader
		}

		currentTerm, stillLeader := kv.rf.GetState()

		if !stillLeader || currentTerm != term {
			return ErrWrongLeader
		}

		kv.mu.Lock()
		if result, exists := kv.requests[op.ClientId] ; exists && result.RequestNum == op.RequestNum {
			kv.mu.Unlock()
			break
		}

		kv.mu.Unlock()
		time.Sleep(WaitInterval)
	}

	return OK
}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxRaftState int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxRaftState = maxRaftState

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.persister = persister
	kv.dbMap = make(map[string]string)
	kv.requests = make(map[int64]RequestResult)

	kv.applySnapshot(persister.ReadSnapshot())

	go kv.handleAppliedOps()
	go kv.monitorRaftStateSize()

	return kv
}

// periodically checks for Operations that have been committed by Raft
// and Snapshots to apply
func (kv *KVServer) handleAppliedOps() {
	const SleepInterval = 1 * time.Millisecond

	for !kv.killed() {
		applyChMsg := <-kv.applyCh

		kv.mu.Lock()

		if applyChMsg.CommandValid {
			appliedOp := applyChMsg.Command.(Op)
			kv.lastAppliedIndex = applyChMsg.CommandIndex

			result, exists := kv.requests[appliedOp.ClientId]
			if !exists || (exists && result.RequestNum < appliedOp.RequestNum) {
				kv.requests[appliedOp.ClientId] = RequestResult{
					RequestNum: appliedOp.RequestNum,
					Value: kv.dbMap[appliedOp.Key],
				}

				if appliedOp.Type == "Put" {
					kv.dbMap[appliedOp.Key] = appliedOp.Value
				} else if appliedOp.Type == "Append" {
					kv.dbMap[appliedOp.Key] = kv.dbMap[appliedOp.Key] + appliedOp.Value
				}
			}
		} else if applyChMsg.SnapshotValid {
			kv.applySnapshot(applyChMsg.Snapshot)
		}

		kv.mu.Unlock()

		time.Sleep(SleepInterval)
	}
}

func (kv *KVServer) applySnapshot(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	decoder := labgob.NewDecoder(r)

	var lastAppliedIndex int
	var dbMap map[string]string
	var requests map[int64]RequestResult

	if decoder.Decode(&lastAppliedIndex) == nil &&
		decoder.Decode(&dbMap) == nil &&
		decoder.Decode(&requests) == nil {
		kv.lastAppliedIndex = lastAppliedIndex
		kv.dbMap = dbMap
		kv.requests = requests
	}
}

// periodically check if the Raft state has grown large enough
// to require a Snapshot
func (kv *KVServer) monitorRaftStateSize() {
	for !kv.killed() {
		kv.mu.Lock()
		if kv.shouldTakeSnapshot() {
			kv.takeSnapshot()
		}
		kv.mu.Unlock()

		time.Sleep(WaitInterval)
	}
}

func (kv *KVServer) shouldTakeSnapshot() bool {
	return kv.maxRaftState > 0 && kv.persister.RaftStateSize() > kv.maxRaftState - SnapshotBytesBuffer
}

func (kv *KVServer) takeSnapshot() {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)

	encoder.Encode(kv.lastAppliedIndex)
	encoder.Encode(kv.dbMap)
	encoder.Encode(kv.requests)
	data := w.Bytes()

	kv.rf.Snapshot(kv.lastAppliedIndex, data)
}