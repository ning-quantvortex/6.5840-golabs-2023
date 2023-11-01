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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()
	nPeers    int

	lastHeartBeatTime time.Time
	electionTimeOut   time.Duration // follower leader candidate
	lastActiveTime    time.Time     // heart beat last active time

	//选举
	term     int64
	role     Role
	leaderId int
	votedFor int

	//提交情况
	log         *RaftLog
	commitIndex int64
	lastApplied int64

	//状态机
	applyCond *sync.Cond
	applyChan chan ApplyMsg
}

type Role int

const (
	Leader Role = iota + 1
	Follower
	Candidate

	Undecided = -1
	None      = 0
)

func (m Role) String() string {
	switch m {
	case Leader:
		return "Leader"
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	}
	return "Unknown"
}

type LogType int

const (
	HeartBeatLogType   LogType = 1
	AppendEntryLogType LogType = 2

	DetectMatchIndexLogType LogType = 5
)

type LogEntry struct {
	Type     LogType
	LogTerm  int64
	LogIndex int64
	data     []byte
}

type RaftLog struct {
	Entries []*LogEntry

	NextIndexs  []int64
	MatchIndexs []int64
}

const (
	ElectionTimeout = 300 * time.Millisecond
	HeatBeatTimeout = 100 * time.Millisecond
)

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	return int(rf.term), rf.role == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

func (rf *Raft) lastLogTermAndLastLogIndex() (int64, int64) {
	if len(rf.log.Entries) == 0 {
		return 0, 0
	}
	logTerm := rf.log.Entries[len(rf.log.Entries)-1].LogTerm
	logIndex := rf.log.Entries[len(rf.log.Entries)-1].LogIndex
	return logTerm, logIndex
}

func (rf *Raft) logTerm(logIndex int64) int64 {
	return rf.log.Entries[logIndex].LogTerm
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
// when election starts, need to send the term and index of the last log.
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int64
	CandidateId  int
	LastLogIndex int64
	LastLogTerm  int64
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int64
	VoteGranted bool
}

type AppendEntriesArgs struct {
	LeaderId   int
	Term       int64 //leader currentTerm
	LogEntries []*LogEntry
}

type AppendEntriesReply struct {
	IsAccept     bool
	Term         int64
	NextLogTerm  int64
	NextLogIndex int64
	Msg          string
}

// Processing the voting request and deciding whether to vote:

// 1. A leader will only step down and become a follower when it receives a request with a higher term.
// If isolated, it will always believe it's the leader.
// 2. A follower, upon receiving a request with a term greater than its own,
// needs to update its term to avoid interference from the old leader.
// 3. Only when a valid request is received should the timeout timer be reset.
// 4. There will be only one leader for each term.
// 5. If a suddenly reconnected node starts an election and its term is the same as the current leader,
// one needs to check the "voteFor" to avoid casting duplicate votes.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	defer func() {
		DPrintf("args: %v, reply: %v", args, reply)
	}()
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	reply.VoteGranted = false
	DPrintf("id: %d received RequestVote, from id: %d term: %d to id: %d term: %d, role: %v voteFor: %v",
		rf.me, args.CandidateId, args.Term, rf.me, rf.term, rf.role, rf.votedFor)
	// shouldn't accept the smaller term
	if rf.term > args.Term {
		return
	}
	if args.Term > rf.term {
		rf.role = Follower //leader to follower
		rf.term = args.Term
		rf.votedFor = -1
		rf.leaderId = -1
	}
	// to avoid repeatly voting
	if rf.votedFor == Undecided || rf.votedFor == args.CandidateId {
		lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
		if lastLogIndex <= args.LastLogIndex && lastLogTerm <= args.LastLogTerm {
			rf.votedFor = args.CandidateId
			rf.leaderId = args.CandidateId
			reply.VoteGranted = true
			//为其他人投票，则重置自己的超时时间
			rf.lastActiveTime = time.Now()
			rf.electionTimeOut = randElectionTimeout()
		}
	}
	rf.persist()
}

// 只有follower、candidate能够接收到这个请求
// 如果收到term比自己大的AppendEntries请求，则表示发生过新一轮的选举，此时拒绝掉，等待超时选举
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("node[%d] handle AppendEntries, from node[%d] term[%d] to node[%d] term[%d], role=[%v]",
		rf.me, args.LeaderId, args.Term, rf.me, rf.term, rf.role)
	defer func() {
		DPrintf("node[%d] return AppendEntries, from node[%d] term[%d] to node[%d] term[%d], role=[%v]",
			rf.me, args.LeaderId, args.Term, rf.me, rf.term, rf.role)
	}()
	reply.Term = rf.term
	reply.IsAccept = false
	//拒绝旧leader请求
	if args.Term < rf.term {
		return
	}
	//发现一个更大的任期，转变成这个term的follower，leader、follower--> follower
	if args.Term > rf.term {
		rf.term = args.Term
		rf.role = Follower
		rf.votedFor = Undecided
		rf.leaderId = Undecided
		return
	}
	//接收对方的请求就是认定它为leader
	rf.leaderId = args.LeaderId
	DPrintf("node[%d] role[%v] received from node[%d], reset lastActiveTime[%v]", rf.me, rf.role, args.LeaderId, rf.lastActiveTime.UnixMilli())
	rf.lastActiveTime = time.Now()
	rf.persist()
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
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
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) heartBeatLoop() {
	for rf.killed() == false {

		time.Sleep(time.Millisecond * 1)
		func() {
			if rf.role != Leader {
				return
			}
			if time.Now().Sub(rf.lastHeartBeatTime) < 100 {
				return
			}
			maxTerm := rf.sendHeartBeat()
			rf.mu.Lock()
			if maxTerm > rf.term {
				//先更新时间，避免leader一进入follower就超时
				rf.term = maxTerm
				rf.lastActiveTime = time.Now()
				rf.role = Follower
				rf.votedFor = Undecided
				rf.leaderId = Undecided
			} else {
				rf.lastHeartBeatTime = time.Now()
				//rf.electionTimeOut = heartBeatTimeout()
			}
			rf.mu.Unlock()
		}()
	}
}
func (rf *Raft) electionLoop() {
	for rf.killed() == false {
		time.Sleep(time.Millisecond * 1)
		func() {
			//leader no operation
			if rf.role == Leader {
				return
			}
			elapses := time.Now().Sub(rf.lastActiveTime)
			electionTimeOut := randElectionTimeout()
			if elapses < electionTimeOut {
				//no time out no new election, only listen RequestVote and AppendEntries
				return
			}
			rf.mu.Lock()
			if rf.role == Follower {
				DPrintf("node[%d] term: %d Follower -> Candidate", rf.me, rf.term)
				rf.role = Candidate
			}
			DPrintf("become candidate... node[%v] term[%v] role[%v] elapses>=electionTimeOut[%v]", rf.me, rf.term, rf.role, elapses >= electionTimeOut)

			maxTerm, voteGranted := rf.startElection()

			rf.mu.Lock()
			//rf.electionTimeOut = randElectionTimeout()
			rf.lastActiveTime = time.Now()
			defer rf.mu.Unlock()
			DPrintf("node[%d] role[%v] maxTerm[%d] voteGranted[%d] nPeers[%d]", rf.me, rf.role, maxTerm, voteGranted, rf.nPeers)
			if rf.role != Candidate {
				return
			}
			if maxTerm > rf.term {
				rf.role = Follower
				rf.term = maxTerm
				rf.votedFor = Undecided
				rf.leaderId = Undecided
			} else if voteGranted > rf.nPeers/2 {
				rf.lastHeartBeatTime = time.Unix(0, 0)
				rf.role = Leader
				rf.leaderId = rf.me
			}
			rf.persist()
		}()
	}
}

func (rf *Raft) sendHeartBeat() int64 {

	logEntries := make([]*LogEntry, 0)
	logEntries = append(logEntries, &LogEntry{
		Type: HeartBeatLogType,
	})
	args := &AppendEntriesArgs{
		Term:       rf.term,
		LeaderId:   rf.me,
		LogEntries: logEntries,
	}
	type AppendEntriesResult struct {
		peerId int
		resp   *AppendEntriesReply
	}
	resultChan := make(chan *AppendEntriesResult, rf.nPeers-1)
	for i := 0; i < rf.nPeers; i++ {
		if rf.me == i {
			continue
		}
		go func(server int, args *AppendEntriesArgs) {
			reply := &AppendEntriesReply{}
			ok := rf.sendAppendEntries(server, args, reply)
			if ok {
				//发现更大的term则退回Follower
				resultChan <- &AppendEntriesResult{
					peerId: server,
					resp:   reply,
				}
			} else {
				resultChan <- &AppendEntriesResult{
					peerId: server,
					resp:   nil,
				}
			}
			DPrintf("node[%d] term[%v] role[%v] send heartbeat to node[%d], reply.Term>rf.term", rf.me, rf.term, rf.role.String(), server)
		}(i, args)
	}
	var maxTerm int64 = rf.term
	for {
		select {
		case result := <-resultChan:
			if result.resp != nil && result.resp.Term > maxTerm {
				maxTerm = result.resp.Term
			}
		}
	}
	return maxTerm
}

func (rf *Raft) startElection() (int64, int) {

	rf.role = Candidate
	rf.votedFor = rf.me
	rf.term++
	rf.persist()
	lastLogTerm, lastLogIndex := rf.lastLogTermAndLastLogIndex()
	rf.mu.Unlock()

	type RequestVoteResult struct {
		peerId int
		resp   *RequestVoteReply
	}
	voteChan := make(chan *RequestVoteResult, rf.nPeers-1)
	args := &RequestVoteArgs{
		Term:         rf.term,
		CandidateId:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	for i := 0; i < rf.nPeers; i++ {
		if rf.me == i {
			continue
		}
		go func(server int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(server, args, reply)
			if ok {
				voteChan <- &RequestVoteResult{
					peerId: server,
					resp:   reply,
				}
			} else {
				voteChan <- &RequestVoteResult{
					peerId: server,
					resp:   nil,
				}
			}
		}(i, args)
	}

	maxTerm := rf.term
	voteGranted := 1
	totalVote := 1
	for i := 0; i < rf.nPeers-1; i++ {
		select {
		case vote := <-voteChan:
			totalVote++
			if vote.resp != nil {
				if vote.resp.VoteGranted {
					voteGranted++
				}
				//出现更大term就退回follower
				if vote.resp.Term > maxTerm {
					maxTerm = vote.resp.Term
				}
			}
			if voteGranted > rf.nPeers/2 {
				return maxTerm, voteGranted
			}
		}
	}
	return maxTerm, voteGranted
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
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {

	rf := &Raft{
		mu:             sync.Mutex{},
		peers:          peers,
		persister:      persister,
		me:             me,
		nPeers:         len(peers),
		leaderId:       Undecided,
		term:           None,
		votedFor:       Undecided,
		role:           Follower,
		lastActiveTime: time.Now(),
		commitIndex:    0,
		lastApplied:    0,
		applyCond:      nil,
		applyChan:      applyCh,
	}
	DPrintf("starting new raft node, id: %d", me)

	rf.log = &RaftLog{
		Entries:     make([]*LogEntry, 0),
		NextIndexs:  make([]int64, len(rf.peers)),
		MatchIndexs: make([]int64, len(rf.peers)),
	}

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// start ticker goroutine to start elections
	go rf.electionLoop()
	go rf.heartBeatLoop()

	return rf
}

func randElectionTimeout() time.Duration {
	return ElectionTimeout + time.Duration(rand.Uint32())%ElectionTimeout
}

func heartBeatTimeout() time.Duration {
	return HeatBeatTimeout
}
