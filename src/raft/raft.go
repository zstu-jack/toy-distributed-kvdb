package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isleader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "labgob"

type RaftState int

const (
	Leader RaftState = iota
	Candidate
	Follower
)

var StateString map[RaftState]string = map[RaftState]string{
	Leader:    "Leader",
	Candidate: "Candidate",
	Follower:  "Follower",
}

//
// as each Raft peer becomes aware that successive log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	log   interface{}
	term  int
	index int
}

type AppendEntriesChan struct {
	Request  *AppendEntriesArgs
	Reply    *AppendEntriesReply
	DoneChan chan int
}
type RequestVoteChan struct {
	Request  *RequestVoteArgs
	Reply    *RequestVoteReply
	DoneChan chan int
}
type GetStateReply struct {
	Term     int
	IsLeader bool
}
type GetStateChan struct {
	DoneChan chan GetStateReply
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	raftState RaftState

	// persistent state
	currentTerm int        // init = 0
	votedFor    int        // set -1 when currentTerm changed.
	log         []LogEntry // first log' index = 1.

	// volatile state for all servers
	commitIndex int // init = 0;  highest index to be committed
	lastApplied int // init = 0;  highest index have been committed.

	// volatile state for leader
	nextIndex  []int // init = last log's index + 1	    index of next log entry to send to that server
	matchIndex []int // init = 0							highest log entry know to be replicated

	// for client.
	applyChan chan ApplyMsg // notify client the msg have been committed

	// used for simplify problem
	recvVotes              int
	appendEntriesChan      chan AppendEntriesChan
	requestVoteChan        chan RequestVoteChan
	getStateChan           chan GetStateChan
	appendEntriesReplyChan chan *AppendEntriesReply
	RequestVoteReplyChan   chan *RequestVoteReply
	endChan                chan int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	st := GetStateChan{DoneChan: make(chan GetStateReply, 1)}
	rf.getStateChan <- st
	reply := <-st.DoneChan
	// Your code here (2A).
	return reply.Term, reply.IsLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
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

//
// restore previously persisted state.
//
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

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int // log index preceding new ones
	PrevLogTerm       int
	Entries           []interface{}
	LeaderCommitIndex int // index of highest log entry known to be committed
}

type AppendEntriesReply struct {
	Term    int
	Success int
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted int
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	st := RequestVoteChan{Request: args, Reply: reply, DoneChan: make(chan int, 1)}
	rf.requestVoteChan <- st
	<-st.DoneChan
}
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	st := AppendEntriesChan{Request: args, Reply: reply, DoneChan: make(chan int, 1)}
	rf.appendEntriesChan <- st
	<-st.DoneChan
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	reply = &RequestVoteReply{}
	DPrintf("%v send request vote to %v request: %v\n", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("%v receive request vote from %v reply: %v\n", rf.me, server, reply)
	rf.RequestVoteReplyChan <- reply
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	reply = &AppendEntriesReply{}
	DPrintf("--->%v send request append entries to %v request: %v\n", rf.me, server, args)
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	DPrintf("--->%v receive request append entries from %v reply: %v\n", rf.me, server, reply)
	rf.appendEntriesReplyChan <- reply
	return ok
}

//
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
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	rf.endChan <- 1
}

func (rf *Raft) IncrementTerm() {
	rf.currentTerm = rf.currentTerm + 1
	rf.votedFor = -1
	rf.recvVotes = 0
}
func (rf *Raft) ModifyTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.recvVotes = 0
		rf.raftState = Follower
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.raftState = Follower

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 0)

	// volatile state for all servers
	rf.commitIndex = 0
	rf.lastApplied = 0

	// volatile state for leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = 0
		rf.matchIndex[i] = 0
	}

	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// self-used struct
	rf.recvVotes = 0
	rf.appendEntriesChan = make(chan AppendEntriesChan, 100)
	rf.requestVoteChan = make(chan RequestVoteChan, 100)
	rf.getStateChan = make(chan GetStateChan, 100)
	rf.appendEntriesReplyChan = make(chan *AppendEntriesReply, 100)
	rf.RequestVoteReplyChan = make(chan *RequestVoteReply, 100)
	rf.endChan = make(chan int, 1)

	// empty log
	rf.log = append(rf.log, LogEntry{term: 0, index: 0})

	rand.Seed(time.Now().UnixNano())

	go func(raft *Raft) {
		// now election timeout: [500-1000ms]
		// now heartbeat timeout: [150ms]
		timerElection := time.NewTimer(time.Duration(500+rand.Int31n(500)) * time.Millisecond)
		timerHeartBeat := time.NewTimer(time.Duration(150) * time.Millisecond)
		recvdHeartBeat := 0
		for {
			select {
			case requestAppendEntries := <-raft.appendEntriesChan:
				// Check HeartBeat
				if requestAppendEntries.Request.Term < rf.currentTerm {
					requestAppendEntries.Reply.Success = 0
					requestAppendEntries.Reply.Term = rf.currentTerm
				} else {
					if requestAppendEntries.Request.Term > rf.currentTerm {
						rf.ModifyTerm(requestAppendEntries.Request.Term)
					}
					requestAppendEntries.Reply.Success = 1
					recvdHeartBeat = 1
				}
				requestAppendEntries.DoneChan <- 1
				break
			case requestRequestVote := <-raft.requestVoteChan:
				if requestRequestVote.Request.Term < raft.currentTerm {
					requestRequestVote.Reply.VoteGranted = 0
					requestRequestVote.Reply.Term = raft.currentTerm
				} else if requestRequestVote.Request.Term == raft.currentTerm {
					if rf.votedFor == -1 {
						requestRequestVote.Reply.VoteGranted = 1
						rf.votedFor = requestRequestVote.Request.CandidateId
					} else {
						requestRequestVote.Reply.VoteGranted = 0
					}
					requestRequestVote.Reply.Term = raft.currentTerm
				} else {
					rf.ModifyTerm(requestRequestVote.Request.Term)
					requestRequestVote.Reply.VoteGranted = 1
					rf.votedFor = requestRequestVote.Request.CandidateId
				}
				requestRequestVote.DoneChan <- 1
				break
			case requestGetState := <-raft.getStateChan:
				reply := GetStateReply{IsLeader: raft.raftState == Leader, Term: raft.currentTerm}
				requestGetState.DoneChan <- reply
				break
			case <-timerElection.C:
				if recvdHeartBeat == 0 && rf.raftState != Leader {
					DPrintf("%v start election state=%v at term=%v\n", rf.me, StateString[rf.raftState], rf.currentTerm)
					rf.IncrementTerm()
					rf.raftState = Candidate
					// add for self
					rf.recvVotes = 1
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me, LastLogTerm: rf.log[len(rf.log)-1].term, LastLogIndex: rf.log[len(rf.log)-1].index}
							go rf.sendRequestVote(i, args, nil)
						}
					}
				}
				timerElection = time.NewTimer(time.Duration(500+rand.Int31n(500)) * time.Millisecond)
				recvdHeartBeat = 0
				break
			case <-timerHeartBeat.C:
				// send heartbeat if current node is leader
				if rf.raftState == Leader {
					for i := 0; i < len(rf.peers); i++ {
						if i != rf.me {
							args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
							go rf.sendAppendEntries(i, args, nil)
						}
					}
				}
				timerHeartBeat = time.NewTimer(time.Duration(150) * time.Millisecond)
				break
			//rpc reply
			case reply := <-rf.appendEntriesReplyChan:
				if reply.Success == 0 {
				}
				rf.ModifyTerm(reply.Term)
				break
			case reply := <-rf.RequestVoteReplyChan:
				if reply.VoteGranted == 0 {
					rf.ModifyTerm(reply.Term)
				} else {
					rf.recvVotes = rf.recvVotes + 1
					if rf.recvVotes >= (len(rf.peers)+1)/2 {
						DPrintf("%v become leader\n", rf.me)
						rf.raftState = Leader
						rf.recvVotes = 0
						rf.votedFor = -1
					}
				}
			case <-rf.endChan:
				goto END
			}
		}
	END:
	}(rf)

	return rf
}
