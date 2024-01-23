package raft

//
// RPC Definitions
//

// RequestVote
type RequestVoteArgs struct {
	CandidateTerm int
	CandidateId int
	LastLogIndex int
	LastLogTerm int
}

type RequestVoteReply struct {
	VoterTerm int
	VoteGranted bool
}

// AppendEntries
type AppendEntriesArgs struct {
	LeaderTerm int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	FollowerTerm int
	Success bool

	// to determine the nextIndex to use after a failure
	ConflictingEntryTerm int			// term in the conflicting entry (if any)
	FirstIndexWithConflictingTerm int	// index of first entry with that term (if any)
	LogLength int						// log length
}

// InstallSnapshot
type InstallSnapshotArgs struct {
	Term int
	LastIncludedIndex int 	// the snapshot replaces all entries up through and including this index
	LastIncludedTerm int	// term of LastIncludedIndex
	Data []byte
}

type InstallSnapshotReply struct {
	CurrentTerm int
}