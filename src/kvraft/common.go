package kvraft

const (
	OK             = "OK"
	ErrWrongLeader = "ErrWrongLeader"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	ClientId int64
	RequestNum int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	ClientId int64
	RequestNum int
}

type GetReply struct {
	Err   Err
	Value string
}

type RequestResult struct {
	RequestNum int
	Value string
}