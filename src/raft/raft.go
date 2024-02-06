package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
	"6.5840/labgob"
	"6.5840/labrpc"
)


type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Command interface{}
	CommandTerm int
	CommandIndex int
}

type Raft struct {
	mu        	sync.Mutex          // Lock to protect shared access to this peer's state
	peers     	[]*labrpc.ClientEnd // RPC end points of all peers
	persister 	*Persister          // Object to hold this peer's persisted state
	me        	int                 // this peer's index into peers[]
	dead      	int32               // set by Kill()
	applyCh		chan ApplyMsg

	// persistent state on all servers
	currentTerm 	int
	votedFor 		int
	log 			[]LogEntry
	state 			string
	lastRecieved 	time.Time

	// volatile state on all servers
	commitIndex int
	lastApplied int

	// volatile state on all leaders
	nextIndex 	[]int
	matchIndex 	[]int

	snapshot                  []byte
	snapshotLastIncludedTerm  int
	snapshotLastIncludedIndex int
	indexOffset               int
}

const BaseWaitInterval = 10 * time.Millisecond
const ElectionTimeoutMin = 50
const ElectionTimeoutMax = 300
const HeartbeatInterval = 100 * time.Millisecond

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return rf.currentTerm, rf.isLeader()
}

func (rf *Raft) isLeader() bool {
	return rf.state == "leader"
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := rf.indexOffset + len(rf.log) + 1
	term := rf.currentTerm
	isLeader := rf.isLeader()

	if isLeader {
		nextEntry := LogEntry{command, term, index}
		rf.log = append(rf.log, nextEntry)
		rf.persist()

		for s, _ := range rf.peers {
			if s != rf.me {
				go rf.sendAppendEntry(s)
			}
		}
	}

	return index, term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	encoder := labgob.NewEncoder(w)

	encoder.Encode(rf.currentTerm)
	encoder.Encode(rf.votedFor)
	encoder.Encode(rf.log)
	encoder.Encode(rf.indexOffset)
	encoder.Encode(rf.snapshotLastIncludedIndex)
	encoder.Encode(rf.snapshotLastIncludedTerm)

	data := w.Bytes()
	rf.persister.Save(data, rf.snapshot)
}

// restore previously persisted state.
func (rf *Raft) readPersist(
	data []byte,
	snapshot []byte,
) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	decoder := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry
	var indexOffset int
	var snapshotLastIncludedIndex int
	var snapshotLastIncludedTerm int

	if decoder.Decode(&currentTerm) != nil ||
		decoder.Decode(&votedFor) != nil ||
		decoder.Decode(&log) != nil ||
		decoder.Decode(&indexOffset) != nil ||
		decoder.Decode(&snapshotLastIncludedIndex) != nil ||
		decoder.Decode(&snapshotLastIncludedTerm) != nil {
		DPrintf("ERROR -- there was an error decoding persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.indexOffset = indexOffset
		rf.snapshotLastIncludedIndex = snapshotLastIncludedIndex
		rf.snapshotLastIncludedTerm = snapshotLastIncludedTerm
	}

	rf.snapshot = snapshot
	rf.commitIndex = rf.indexOffset
	rf.lastApplied = rf.indexOffset
}

// RPC to request a vote from another server in an election
func (rf *Raft) RequestVote(
	args *RequestVoteArgs,
	reply *RequestVoteReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm < args.CandidateTerm {
		rf.convertToFollower(args.CandidateTerm)
	}

	reply.VoterTerm = rf.currentTerm

	if rf.voteIsValid(args) {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.lastRecieved = time.Now()
		rf.persist()
	} else {
		reply.VoteGranted = false
	}
}

func (rf *Raft) convertToFollower(newTerm int) {
	rf.state = "follower"
	rf.currentTerm = newTerm
	rf.votedFor = -1
	rf.lastRecieved = time.Now()
	rf.persist()
}

func (rf *Raft) voteIsValid(args *RequestVoteArgs) bool {
	return rf.serverCanVote(args) &&
		rf.candidateLogUpToDate(args) &&
		args.CandidateTerm >= rf.currentTerm
}

func (rf *Raft) serverCanVote(args *RequestVoteArgs) bool {
	return rf.votedFor == -1 || rf.votedFor == args.CandidateId
}

func (rf *Raft) candidateLogUpToDate(args *RequestVoteArgs) bool {
	lastLogIndex, lastLogTerm := rf.lastLogEntry()

	if args.LastLogTerm == lastLogTerm {
		return args.LastLogIndex >= lastLogIndex
	}

	return args.LastLogTerm > lastLogTerm
}

func (rf *Raft) lastLogEntry() (int, int) {
	if len(rf.log) == 0 {
		return rf.snapshotLastIncludedIndex, rf.snapshotLastIncludedTerm
	}

	lastLogEntry := rf.log[len(rf.log) - 1]
	return lastLogEntry.CommandIndex, lastLogEntry.CommandTerm
}

// RPC to send entries from leader to followers
func (rf *Raft) AppendEntries(
	args *AppendEntriesArgs,
	reply *AppendEntriesReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.newLeaderWasElected(args.LeaderTerm) {
		rf.convertToFollower(args.LeaderTerm)
	}

	if args.LeaderTerm < rf.currentTerm {
		reply.Success = false
		return
	}

	rf.lastRecieved = time.Now()

	reply.FollowerTerm = rf.currentTerm
	reply.LogLength = -1

	// check that the previous log entries are correct
	relativePrevIndex := args.PrevLogIndex - rf.indexOffset

	if relativePrevIndex < 1 { // check snapshot
		if args.PrevLogTerm != rf.snapshotLastIncludedTerm ||
			args.PrevLogIndex != rf.snapshotLastIncludedIndex {
			reply.Success = false
			return
		}
	} else if len(rf.log) < relativePrevIndex {
		reply.LogLength = len(rf.log) + rf.indexOffset
		reply.Success = false
		return
	} else if relativePrevIndex > 0 && rf.log[relativePrevIndex - 1].CommandTerm != args.PrevLogTerm {
		conflictTerm := rf.log[relativePrevIndex - 1].CommandTerm

		reply.Success = false
		reply.ConflictingEntryTerm = conflictTerm
		reply.FirstIndexWithConflictingTerm = rf.findFirstIndexForTerm(conflictTerm)
		return
	}

	reply.Success = true

	rf.removeIncorrectLogEntries(args.Entries)

	lastNewEntryIndex := 0
	for _, entry := range args.Entries {
		newEntry := entry

		relativeIndex := entry.CommandIndex - rf.indexOffset
		if relativeIndex > 0 &&
			len(rf.log) < relativeIndex {
			rf.log = append(rf.log, newEntry)
			lastNewEntryIndex = newEntry.CommandIndex
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		if lastNewEntryIndex > 0 && lastNewEntryIndex < args.LeaderCommit {
			rf.commitIndex = lastNewEntryIndex
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	rf.persist()
}

func (rf *Raft) newLeaderWasElected(leaderTerm int) bool {
	return leaderTerm > rf.currentTerm ||
		(rf.state == "candidate" && leaderTerm == rf.currentTerm)
}

func (rf *Raft) findFirstIndexForTerm(term int) int {
	for i := 0; i < len(rf.log); i++ {
		if rf.log[i].CommandTerm == term {
			return rf.log[i].CommandIndex
		}
	}

	return 0
}

func (rf *Raft) removeIncorrectLogEntries(correctEntries []LogEntry) {
	for _, entry := range correctEntries {
		relativeLogIndex := entry.CommandIndex - rf.indexOffset - 1
		if relativeLogIndex >= 0 &&
			len(rf.log) > relativeLogIndex &&
			rf.log[relativeLogIndex].CommandTerm != entry.CommandTerm {
			rf.log = rf.log[:relativeLogIndex]
		}
	}
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(
	index int,
	snapshot []byte,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if index <= rf.indexOffset {
		return
	}

	lastSnapshotEntry := rf.log[index - rf.indexOffset - 1]

	rf.snapshot = snapshot
	rf.snapshotLastIncludedIndex = lastSnapshotEntry.CommandIndex
	rf.snapshotLastIncludedTerm = lastSnapshotEntry.CommandTerm

	rf.log = rf.log[index - rf.indexOffset:]
	rf.indexOffset = index

	rf.persist()
}

// RPC for the leader to send its Snapshot to a follower who has lagged too far behind
func (rf *Raft) InstallSnapshot(
	args *InstallSnapshotArgs,
	reply *InstallSnapshotReply,
) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if args.Term < rf.currentTerm {
		reply.CurrentTerm = rf.currentTerm
		return
	}

	if rf.snapshotAlreadyUpToDate(args.LastIncludedIndex) {
		return
	}

	rf.indexOffset = args.LastIncludedIndex
	rf.snapshot = args.Data
	rf.snapshotLastIncludedIndex = args.LastIncludedIndex
	rf.snapshotLastIncludedTerm = args.LastIncludedTerm

	rf.commitIndex = rf.indexOffset
	rf.lastApplied = rf.indexOffset

	rf.log = nil

	rf.persist()

	newMessage := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      rf.snapshot,
		SnapshotTerm:  rf.snapshotLastIncludedTerm,
		SnapshotIndex: rf.snapshotLastIncludedIndex,
	}

	rf.applyCh <- newMessage
}

func (rf *Raft) snapshotAlreadyUpToDate(lastIndexInLeaderSnapshot int) bool {
	return rf.snapshotLastIncludedIndex >= lastIndexInLeaderSnapshot
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(
	peers []*labrpc.ClientEnd,
	me int,
	persister *Persister,
	applyCh chan ApplyMsg,
) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh

	rf.readPersist(persister.ReadRaftState(), persister.ReadSnapshot())

	for i := 0 ; i < len(rf.peers) ; i++ {
		rf.nextIndex = append(rf.nextIndex, rf.indexOffset + len(rf.log) + 1)
		rf.matchIndex = append(rf.matchIndex, 0)
	}

	go rf.monitorCommittedEntries()
	go rf.monitorElectionStatus()

	return rf
}

// periodically check if there are new committed entries to send to the service
func (rf *Raft) monitorCommittedEntries() {
	const WaitInterval = 1 * time.Millisecond

	for !rf.killed() {
		rf.mu.Lock()

		if rf.commitIndex > rf.lastApplied && len(rf.log) + rf.indexOffset > rf.lastApplied {
			rf.lastApplied++

			rf.sendCommittedEntry(rf.lastApplied)
		}

		rf.mu.Unlock()

		time.Sleep(WaitInterval)
	}
}

func (rf *Raft) sendCommittedEntry(index int) {
	newMessage := ApplyMsg{
		CommandValid: true,
		CommandIndex: index,
		Command: rf.log[index - rf.indexOffset - 1].Command,
	}

	rf.applyCh <- newMessage
}

// periodically check if a leader election should be started
func (rf *Raft) monitorElectionStatus() {
	const BaseElectionTimeout = 300 * time.Millisecond

	for !rf.killed() {
		rand.Seed(time.Now().UnixNano())

		startTime := time.Now()
		time.Sleep(BaseElectionTimeout)

		rf.mu.Lock()
		if rf.lastRecieved.Before(startTime) && !rf.isLeader() {
			go rf.initiateElection()
		}
		rf.mu.Unlock()

		randTimeoutMs := ElectionTimeoutMin + (rand.Int63() % ElectionTimeoutMax)
		time.Sleep(time.Duration(randTimeoutMs) * time.Millisecond)
	}
}

// initiate an election by requesting votes from all servers
func (rf *Raft) initiateElection() {
	rf.mu.Lock()
	rf.convertToCandidate()
	rf.mu.Unlock()

	cond := sync.NewCond(&rf.mu)
	myVotes := 1
	totalVotes := 1

	for s, _ := range rf.peers {
		if s != rf.me {
			go func(server int) {
				voteGranted := rf.sendRequestVote(server)

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if voteGranted {
					myVotes++
				}
				totalVotes++
				cond.Broadcast()
			} (s)
		}
	}

	rf.mu.Lock()
	for rf.insufficientVotes(myVotes, totalVotes) {
		cond.Wait()
	}

	if rf.wonElection(myVotes) {
		rf.convertToLeader()

		rf.mu.Unlock()

		rf.sendHeartbeatsToFollowers()
	} else {
		rf.mu.Unlock()
	}
}

func (rf *Raft) convertToCandidate() {
	rf.state = "candidate"
	rf.currentTerm++
	rf.votedFor = rf.me
	rf.lastRecieved = time.Now()
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int) bool {
	rf.mu.Lock()

	lastLogIndex, lastLogTerm := rf.lastLogEntry()

	args := RequestVoteArgs{
		CandidateTerm: rf.currentTerm,
		CandidateId: rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm: lastLogTerm,
	}
	reply := RequestVoteReply{}

	rf.mu.Unlock()

	rf.peers[server].Call("Raft.RequestVote", &args, &reply)

	return reply.VoteGranted
}

func (rf *Raft) insufficientVotes(
	myVotes int,
	totalVotes int,
) bool {
	return float64(myVotes) <= float64(len(rf.peers)) / 2.0 && totalVotes != len(rf.peers)
}

func (rf *Raft) wonElection(votes int) bool  {
	return float64(votes) > float64(len(rf.peers)) / 2.0 && rf.state == "candidate"
}

func (rf *Raft) convertToLeader() {
	rf.state = "leader"
	rf.lastRecieved = time.Now()

	for i := 0 ; i < len(rf.peers) ; i++ {
		rf.nextIndex[i] = rf.indexOffset + len(rf.log) + 1
		rf.matchIndex[i] = 0
	}

	go rf.monitorCommitIndex()
}

// periodically check if new entries should be committed
func (rf *Raft) monitorCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	nextCommitIndex := rf.commitIndex + 1

	for !rf.killed() && rf.isLeader() {
		if len(rf.log) + rf.indexOffset >= nextCommitIndex {
			if rf.shouldCommitIndex(nextCommitIndex) {
				rf.commitIndex = nextCommitIndex
			}
			nextCommitIndex++
		} else {
			nextCommitIndex = rf.commitIndex + 1
		}

		rf.mu.Unlock()
		time.Sleep(BaseWaitInterval)
		rf.mu.Lock()
	}
}

// checks that there is consensus on the new commit index and the term matches
func (rf *Raft) shouldCommitIndex(newCommitIndex int) bool {
	numCommitted := 1.0

	for i, _ := range rf.peers {
		if rf.matchIndex[i] >= newCommitIndex {
			numCommitted++
		}
	}

	floatLen := float64(len(rf.peers))
	relativeCommitIndex := newCommitIndex - rf.indexOffset - 1
	return numCommitted > floatLen / 2.0 && rf.log[relativeCommitIndex].CommandTerm == rf.currentTerm
}

// periodically send heartbeats to all other servers in order to maintain leadership
func (rf *Raft) sendHeartbeatsToFollowers() {
	for s, _ := range rf.peers {
		if s != rf.me {
			go func(server int) {
				rf.mu.Lock()
				for !rf.killed() && rf.isLeader() {
					go rf.sendAppendEntry(server)

					rf.mu.Unlock()
					time.Sleep(HeartbeatInterval)
					rf.mu.Lock()
				}
				rf.mu.Unlock()
			} (s)
		}
	}
}

// send AppendEntry command to a follower
// serves as a heartbeat if there are no log entries in the command
func (rf *Raft) sendAppendEntry(server int) {
	rf.mu.Lock()
	if !rf.isLeader() {
		rf.mu.Unlock()
		return
	}

	originalTerm := rf.currentTerm

	args := AppendEntriesArgs{
		LeaderTerm: rf.currentTerm,
		LeaderId: rf.me,
		LeaderCommit: rf.commitIndex,
	}
	reply := AppendEntriesReply{}

	relativePrevLogIndex := rf.nextIndex[server] - rf.indexOffset - 1

	if relativePrevLogIndex <= 0 {
		args.PrevLogIndex = rf.snapshotLastIncludedIndex
		args.PrevLogTerm = rf.snapshotLastIncludedTerm
	} else if len(rf.log) > relativePrevLogIndex - 1 {
		args.PrevLogIndex = rf.log[relativePrevLogIndex - 1].CommandIndex
		args.PrevLogTerm = rf.log[relativePrevLogIndex - 1].CommandTerm
	}

	if relativePrevLogIndex >= 0 && len(rf.log) > relativePrevLogIndex {
		args.Entries = rf.log[relativePrevLogIndex:]
	}

	rf.mu.Unlock()

	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()

	if reply.FollowerTerm > rf.currentTerm {
		rf.convertToFollower(reply.FollowerTerm)
		rf.mu.Unlock()
		return
	}

	if reply.FollowerTerm != originalTerm {  // old RPC reply
		rf.mu.Unlock()
		return
	}

	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		rf.mu.Unlock()

	} else { // retry AppendEntries with lower nextIndex
		if reply.LogLength > 0 {
			if reply.LogLength < rf.snapshotLastIncludedIndex {
				go rf.sendInstallSnapshot(server)
			}

			rf.nextIndex[server] = reply.LogLength + 1
		} else {
			lastIndexForTerm := rf.findLastIndexForTerm(reply.ConflictingEntryTerm)
			if lastIndexForTerm == 0 {
				if reply.FirstIndexWithConflictingTerm == 0 {
					go rf.sendInstallSnapshot(server)
				}

				rf.nextIndex[server] = reply.FirstIndexWithConflictingTerm
			} else {
				rf.nextIndex[server] = lastIndexForTerm
			}
		}

		if rf.nextIndex[server] < rf.indexOffset + 1 {
			rf.nextIndex[server] = rf.indexOffset + 1
		}

		rf.mu.Unlock()

		rf.sendAppendEntry(server)
	}
}

func (rf *Raft) findLastIndexForTerm(term int) int {
	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].CommandTerm == term {
			return rf.log[i].CommandIndex
		}
	}

	return 0
}

func (rf *Raft) sendInstallSnapshot(server int) {
	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LastIncludedIndex: rf.snapshotLastIncludedIndex,
		LastIncludedTerm:  rf.snapshotLastIncludedTerm,
		Data:              rf.snapshot,
	}
	reply := InstallSnapshotReply{}

	rf.peers[server].Call("Raft.InstallSnapshot", &args, &reply)

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.CurrentTerm > rf.currentTerm {
		rf.convertToFollower(reply.CurrentTerm)
		return
	}
}