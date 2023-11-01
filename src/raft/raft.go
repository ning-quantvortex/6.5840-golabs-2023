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

type State string

const (
	Follower  State = "Follower"
	Candidate State = "Candidate"
	Leader    State = "Leader"
)

type LogEntry struct {
	Command interface{} // each entry contains command for state machine,
	Term    int         // and term when entry was received by leader(fisrt index is 1)
}

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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	applyCh   chan ApplyMsg
	applyCond *sync.Cond
	// Persistent state on all servers
	currentTerm int
	voteFor     int
	log         []LogEntry

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	// Volatile state on leaders
	nextIndex  []int
	matchIndex []int

	state    State
	leaderId int

	hbChan  chan bool
	eleChan chan bool
	lCond   *sync.Cond
	nlCond  *sync.Cond

	lastHeartBeatSentTime int64
	heartbeatDuration     int

	latestReceivedTime int64 // the last time received leader AppenedEentires RPC oe heart beat
	// or giving candidate RequestVote RPC voting time.

	lastElectionTime int64
	electionTimeout  int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.RLock()
	defer rf.mu.RUnlock()

	return rf.currentTerm, rf.state == Leader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
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

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // candidate’s term
	CandidateId  int // candidate requesting vote
	LastLogIndex int // index of candidate’s last log entry (§5.4)
	LastLogTerm  int // term of candidate’s last log entry (§5.4)
}

type AppendEntriesArgs struct {
	Term         int        // leader’s term
	LeaderId     int        // so follower can redirect clients
	PrevLogIndex int        // index of log entry immediately preceding new ones
	PrevLogTerm  int        // term of prevLogIndex entry
	Entries      []LogEntry // log entries to store (empty for heartbeat;may send more than one for efficiency)
	LeaderCommit int        // leader’s commitIndex
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool // true means candidate received vote
}

type AppendEntriesReply struct {
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reply false if term < CurrentTerm, otherwise continue a "voting process"
	if rf.currentTerm <= args.Term {

		// if one server's current term is smaller than other's, then it updates
		// it current term to the larger value
		if rf.currentTerm < args.Term {

			DPrintf("[RequestVote]: Id %d Term %d State %v\t||\targs's term %d is larger\n",
				rf.me, rf.currentTerm, rf.state, args.Term)

			rf.currentTerm = args.Term
			rf.voteFor = -1
			rf.state = Follower
			rf.persist()
		}

		// VoteFor is null or candidateId
		if rf.voteFor == -1 || rf.voteFor == args.CandidateId {

			// determine which of two Log is more "up-to-date" by comparing
			// the index and term of the last entries in the logs
			lastLogIndex := len(rf.log) - 1
			if lastLogIndex < 0 {
				DPrintf("[RequestVote]: Id %d Term %d State %v\t||\tinvalid lastLogIndex: %d\n",
					rf.me, rf.currentTerm, rf.state, lastLogIndex)
			}
			lastLogTerm := rf.log[lastLogIndex].Term

			DPrintf("[RequestVote]: Id %d Term %d State %v\t||\tlastLogIndex %d and lastLogTerm %d"+
				" while args's lastLogIndex %d lastLogTerm %d\n", rf.me, rf.currentTerm, rf.state,
				lastLogIndex, lastLogTerm, args.LastLogIndex, args.LastLogTerm)

			// If the logs have last entries with different terms, then the Log with the later term is more up-to-date;
			// otherwise, if the logs end with the same term, then whichever Log is longer is more up-to-date.
			// candidate is at least as up-to-date as receiver's Log
			if lastLogTerm < args.LastLogTerm ||
				(lastLogTerm == args.LastLogTerm && lastLogIndex <= args.LastLogIndex) {

				rf.voteFor = args.CandidateId
				// reset election timeout
				rf.resetElectionTimer()

				rf.state = Follower
				DPrintf("[RequestVote]: Id %d Term %d State %v\t||\tgrant vote for candidate %d\n",
					rf.me, rf.currentTerm, rf.state, args.CandidateId)

				rf.persist()

				reply.Term = rf.currentTerm
				reply.VoteGranted = true
				return
			}
		}
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
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

func (rf *Raft) noticeHeartBeat() {
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	rf.lastHeartBeatSentTime = time.Now().UnixNano()
	rf.mu.Unlock()

	rf.mu.Lock()
	index := len(rf.log) - 1
	nReplica := 1
	go rf.broadcastAppendEntries(index, rf.currentTerm, rf.commitIndex, nReplica, "Broadcast")
	rf.mu.Unlock()
}

func (rf *Raft) broadcastAppendEntries(index int, term int, commitIndex int, nReplica int, name string) {
}

func (rf *Raft) elect() {
	rf.mu.Lock()
	// set the state to candidate
	rf.state = Candidate
	rf.currentTerm++
	rf.voteFor = rf.me
	voteCount := 1
	rf.resetElectionTimer()
	rf.persist()
	DPrintf("ticker(): Id: %d, Term %d, State %v, start a new election", rf.me, rf.currentTerm, rf.state)
	rf.mu.Unlock()

	// send all peers RequestVote
	go func(voteCount *int, rf *Raft) {
		var wg sync.WaitGroup
		winThreshold := len(rf.peers)/2 + 1

		for i, _ := range rf.peers {
			// skip the candidate starts the vote
			if i == rf.me {
				continue
			}

			rf.mu.Lock()
			wg.Add(1)
			var lastLogIndex int
			var args RequestVoteArgs
			if len(rf.log) == 0 {
				lastLogIndex = len(rf.log)
				continue
			} else {
				lastLogIndex = len(rf.log) - 1
				args = RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
					LastLogIndex: lastLogIndex, LastLogTerm: rf.log[lastLogIndex].Term}
			}
			if lastLogIndex < 0 {
				DPrintf("elect(): Id %d Term %d State %v\t||\tinvalid lastLogIndex %d\n",
					rf.me, rf.currentTerm, rf.state, lastLogIndex)
			}
			DPrintf("elect(): Id %d Term %d State %v\t||\tissue RequestVote RPC"+
				" to peer %d\n", rf.me, rf.currentTerm, rf.state, i)
			rf.mu.Unlock()
			var reply RequestVoteReply

			// use seperate goroutine to send peer RequestVote RPC independently
			go func(i int, rf *Raft, args *RequestVoteArgs, reply *RequestVoteReply) {
				defer wg.Done()

				ok := rf.sendRequestVote(i, args, reply)

				if !ok {
					rf.mu.Lock()
					DPrintf("[StartElection]: Id %d Term %d State %v\t||\tsend RequestVote"+
						" Request to peer %d failed\n", rf.me, rf.currentTerm, rf.state, i)
					rf.mu.Unlock()
					return
				}

				rf.mu.Lock()
				if rf.currentTerm != args.Term {
					rf.mu.Unlock()
					return
				}
				rf.mu.Unlock()

				if !reply.VoteGranted {

					rf.mu.Lock()
					defer rf.mu.Unlock()
					DPrintf("[StartElection]: Id %d Term %d State %v\t||\tRequestVote is"+
						" rejected by peer %d\n", rf.me, rf.currentTerm, rf.state, i)

					// If RPC request or response contains T > CurrentTerm, set CurrentTerm = T,
					// convert to follower
					if rf.currentTerm < reply.Term {
						DPrintf("[StartElection]: Id %d Term %d State %v\t||\tless than"+
							" peer %d Term %d\n", rf.me, rf.currentTerm, rf.state, i, reply.Term)
						rf.currentTerm = reply.Term
						// reset voteFor
						rf.voteFor = -1
						rf.state = Follower
						rf.persist()
					}

				} else {
					// got peer's vote
					rf.mu.Lock()
					DPrintf("[StartElection]: Id %d Term %d State %v\t||\tpeer %d grants vote\n",
						rf.me, rf.currentTerm, rf.state, i)

					*voteCount += 1
					DPrintf("[StartElection]: Id %d Term %d State %v\t||\tnVotes %d\n",
						rf.me, rf.currentTerm, rf.state, *voteCount)

					// if got the majority's vote ，and the state is Candidate, swtich to leader
					if rf.state == Candidate && *voteCount >= winThreshold {

						DPrintf("[StartElection]: Id %d Term %d State %v\t||\twin election with nVotes %d\n",
							rf.me, rf.currentTerm, rf.state, *voteCount)

						// switch to leader state
						rf.state = Leader

						rf.leaderId = rf.me

						// when new leader start, initial nextIndex as the last postion of log
						for i := 0; i < len(rf.peers); i++ {
							rf.nextIndex[i] = len(rf.log)
							rf.matchIndex[i] = 0
						}
						// the hbChan is clocking until ticker() loop read the channel data,
						// so we need to send the hearbeat immediately
						// to avoid other peers send the useless vote be cause of the hear beat timeout
						go rf.broadcastHeartbeat()

						// win the election, persist the state
						rf.persist()
					}
					rf.mu.Unlock()
				}

			}(i, rf, &args, &reply)
		}
		// wait unsend RPC goroutine to finish
		wg.Wait()

	}(&voteCount, rf)
}

func (rf *Raft) broadcastHeartbeat() {
	// can't send the heart beat if is not a leader
	if _, isLeader := rf.GetState(); !isLeader {
		return
	}

	rf.mu.Lock()
	// update the hear beat sending time
	rf.lastHeartBeatSentTime = time.Now().UnixNano()
	rf.mu.Unlock()

	rf.mu.Lock()
	index := len(rf.log) - 1
	nReplica := 1
	go rf.broadcastAppendEntries(index, rf.currentTerm, rf.commitIndex, nReplica, "Broadcast")
	rf.mu.Unlock()
}

func (rf *Raft) resetElectionTimer() {
	rand.Seed(time.Now().UnixNano())
	rf.electionTimeout = rf.heartbeatDuration*5 + rand.Intn(300-150)
	rf.latestReceivedTime = time.Now().UnixNano()
}

func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here (2A)
		for {
			select {
			case <-rf.hbChan:
				rf.mu.Lock()
				DPrintf("ticker(): Id: %d, Term %d, State %v, the heart beat begins", rf.me, rf.currentTerm, rf.state)
				rf.mu.Unlock()
				go rf.noticeHeartBeat()
			case <-rf.eleChan:
				rf.mu.Lock()
				DPrintf("ticker(): Id: %d, Term %d, State %v, the election is timeout, start a new one", rf.me, rf.currentTerm, rf.state)
				rf.mu.Unlock()
				go rf.elect()
			}
		}
	}
}

func (rf *Raft) electionTimeoutLoop() {
	for {
		// if is leader, no need to check the time out.
		if _, isLeader := rf.GetState(); isLeader {
			rf.nlCond.L.Lock()
			// or we should use rf.mu.Lock()
			rf.nlCond.Wait()
			rf.nlCond.L.Unlock()
		} else {
			rf.mu.Lock()
			timeout := time.Now().UnixNano() - rf.lastElectionTime
			if int(timeout/int64(time.Millisecond)) >= rf.electionTimeout {
				DPrintf("electionTimeoutLoop(): Id: %d, Term %d, State %v, the election is time out, a new one is starting", rf.me, rf.currentTerm, rf.state)
				rf.eleChan <- true
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
}

func (rf *Raft) heartBeatLoop() {
	for {
		if _, isLeader := rf.GetState(); !isLeader {
			rf.lCond.L.Lock()
			// or we should use rf.mu.Lock()
			rf.lCond.Wait()
			rf.lCond.L.Unlock()
		} else {
			rf.mu.Lock()
			timeout := time.Now().UnixNano() - rf.latestReceivedTime
			if int(timeout/int64(time.Millisecond)) >= rf.heartbeatDuration {
				DPrintf("heartBeatLoop(): Id: %d, Term %d, State %v, the heart heart starts", rf.me, rf.currentTerm, rf.state)
				rf.hbChan <- true
			}
			rf.mu.Unlock()
		}
		time.Sleep(time.Millisecond * 10)
	}
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
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.state = Follower
	rf.leaderId = -1
	rf.applyCond = sync.NewCond(&rf.mu)
	rf.lCond = sync.NewCond(&rf.mu)
	rf.nlCond = sync.NewCond(&rf.mu)
	rf.heartbeatDuration = 120
	rf.resetElectionTimer()
	rf.eleChan = make(chan bool)
	rf.hbChan = make(chan bool)
	// inital the Raft instance
	rf.currentTerm = 0
	rf.voteFor = -1
	rf.commitIndex = 0
	rf.lastApplied = 0
	// print to the log when starting the new raft instance

	// start ticker goroutine to start elections
	// ticker listens heartbeat timeout and election timeout
	go rf.ticker()

	// start heartBeat loop
	go rf.heartBeatLoop()
	// start election loop
	go rf.electionTimeoutLoop()

	// initialize from state persisted before a crash
	rf.mu.Lock()
	rf.readPersist(persister.ReadRaftState())
	rf.mu.Unlock()
	return rf
}
