package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (Index, Term, isleader)
//   start agreement on a new Log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the Log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"labgob"
	"math/rand"
	"strconv"
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
	ChanSize = 100
)

var StateString = map[RaftState]string{
	Leader:    "Leader",
	Candidate: "Candidate",
	Follower:  "Follower",
}

//
// as each Raft peer becomes aware that successive Log Entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed Log entry.
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
	Log   interface{}
	Term  int
	Index int
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
type ClientCommandResult struct {
	Index    int
	Term     int
	IsLeader int
}

func (result *ClientCommandResult) ToString() string {
	return "Index=" + strconv.Itoa(result.Index) + ", Term=" + strconv.Itoa(result.Term) + ", IsLeader=" + strconv.Itoa(result.IsLeader)
}

type ClientCommandChan struct {
	ClientCommandResultChan chan ClientCommandResult
	Command                 interface{}
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's Index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	raftState RaftState

	// persistent state
	currentTerm int        // init = 0
	votedFor    int        // set -1 when currentTerm changed.
	log         []LogEntry // first Log' Index = 1.

	// volatile state for all servers
	commitIndex int // init = 0;  highest Index to be committed
	lastApplied int // init = 0;  highest Index have been committed.

	// volatile state for leader
	nextIndex  []int // init = last Log's Index + 1	    Index of next Log entry to send to that server
	matchIndex []int // init = 0							highest Log entry know to be replicated

	// for client.
	applyChan chan ApplyMsg // notify client the msg have been committed

	// used for simplify problem
	clientCommandChan      chan ClientCommandChan
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
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	DPrintf("!!!!!!!!!!!!!!!!!!!!!!!!Write Persist!!!!!!!!!!!!!!!!!!!!!   node(%v)  term=%v,voteFor=%v,logsize=%v lastlog=%v", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.log[len(rf.log)-1])

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
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
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("read persist error")
	}
	DPrintf("!!!!!!!!!!!!!!!!!!!!!!!!Read Persist!!!!!!!!!!!!!!!!!!!!!   node(%v)  term=%v,voteFor=%v,logsize=%v lastLog=%v", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.log[len(rf.log)-1])
}

type AppendEntriesArgs struct {
	Term              int
	LeaderId          int
	PrevLogIndex      int // Log Index preceding new ones
	PrevLogTerm       int
	Entries           []LogEntry
	LeaderCommitIndex int // Index of highest Log entry known to be committed
}

func (args *AppendEntriesArgs) ToString() string {
	firstEntryIndex := -1
	if len(args.Entries) > 0 {
		firstEntryIndex = args.Entries[0].Index
	}
	return "Term=" + strconv.Itoa(args.Term) + ", LeaderId=" + strconv.Itoa(args.LeaderId) + ", PrevLogIndex=" + strconv.Itoa(args.PrevLogIndex) +
		", PrevLogTerm=" + strconv.Itoa(args.PrevLogTerm) + ", LeaderCommitIndex=" + strconv.Itoa(args.LeaderCommitIndex) +
		", entries' size=" + strconv.Itoa(len(args.Entries)) + ", first entry Index=" + strconv.Itoa(firstEntryIndex)
}

type AppendEntriesReply struct {
	Term      int
	Success   int
	Who       int // TODO: remove the field
	NextIndex int // TODO: remove the field
}

func (reply *AppendEntriesReply) ToString() string {
	return "Term=" + strconv.Itoa(reply.Term) + ", Success=" + strconv.Itoa(reply.Success) + ", NextIndex=" + strconv.Itoa(reply.NextIndex)
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

func (args *RequestVoteArgs) ToString() string {
	return "Term=" + strconv.Itoa(args.Term) + ", CandidateId=" + strconv.Itoa(args.CandidateId) + ", LastLogIndex=" + strconv.Itoa(args.LastLogIndex) +
		", LastLogTerm=" + strconv.Itoa(args.LastLogTerm)
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

func (reply *RequestVoteReply) ToString() string {
	return "Term=" + strconv.Itoa(reply.Term) + ", voteGranted=" + strconv.Itoa(reply.VoteGranted)
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
	DPrintf("!!!>(%v) Received Request Append Entries: (%v)", rf.me, args.ToString())
	st := AppendEntriesChan{Request: args, Reply: reply, DoneChan: make(chan int, 1)}
	rf.appendEntriesChan <- st
	<-st.DoneChan
}

//
// example code to send a RequestVote RPC to a server.
// server is the Index of the target server in rf.peers[].
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
	DPrintf("(%v) send request vote to %v request: (%v)\n", rf.me, server, args.ToString())
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf("(%v) receive request vote reply from %v reply: (%v)\n", rf.me, server, reply.ToString())
	rf.RequestVoteReplyChan <- reply
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	reply = &AppendEntriesReply{}
	DPrintf("--->(%v) send request append entries to %v request: (%v)\n", rf.me, server, args.ToString())
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	DPrintf("--->(%v) receive request append entries reply from %v reply: (%v)\n", rf.me, server, reply.ToString())
	rf.appendEntriesReplyChan <- reply
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's Log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft Log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the Index that the command will appear at
// if it's ever committed. the second return value is the current
// Term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {

	// Your code here (2B).
	DPrintf("(%v)Received a client Command at Term (%v) state=%v", rf.me, rf.currentTerm, StateString[rf.raftState])
	clientCommand := ClientCommandChan{Command: command, ClientCommandResultChan: make(chan ClientCommandResult, 1)}
	rf.clientCommandChan <- clientCommand
	commandResult := <-clientCommand.ClientCommandResultChan
	isLeader := false
	if commandResult.IsLeader > 0 {
		isLeader = true
	}
	DPrintf("(%v)Reply to Client at Term %v reply=%v", rf.me, rf.currentTerm, commandResult.ToString())
	return commandResult.Index, commandResult.Term, isLeader
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
	rf.persist()
}
func (rf *Raft) ModifyTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		rf.recvVotes = 0
		rf.raftState = Follower
		rf.persist()
	}
}

func (rf *Raft) DoSendAppendEntries(To int) {
	lastLogIndexForI := rf.nextIndex[To] - 1
	theIndexInLeader := 0
	for i := 0; i < len(rf.log); i++ {
		if lastLogIndexForI == rf.log[i].Index {
			theIndexInLeader = i
			break
		}
	}

	// FIXME ? once entries'size > 0. invalid memory address or nil pointer derefrence
	//args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.Log[lastLogIndexForI].(LogEntry).Index,
	//	PrevLogTerm: rf.Log[lastLogIndexForI].(LogEntry).Term, Entries: rf.Log[theIndexInLeader+1:], LeaderCommitIndex: lastLogIndex}
	rf.mu.Lock()
	entries := make([]LogEntry, len(rf.log[theIndexInLeader+1:]))
	copy(entries, rf.log[theIndexInLeader+1:])
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.log[lastLogIndexForI].Index,
		PrevLogTerm: rf.log[lastLogIndexForI].Term, Entries: entries, LeaderCommitIndex: rf.commitIndex}
	rf.mu.Unlock()

	go func(i int, args *AppendEntriesArgs) {
		rf.sendAppendEntries(i, args, nil)
	}(To, args)
}
func (rf *Raft) DoSendRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		args := &RequestVoteArgs{Term: rf.currentTerm, CandidateId: rf.me,
			LastLogTerm: rf.log[len(rf.log)-1].Term, LastLogIndex: rf.log[len(rf.log)-1].Index}
		go func(i int, args *RequestVoteArgs) {
			rf.sendRequestVote(i, args, nil)
		}(i, args)
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
		rf.nextIndex[i] = 1
		rf.matchIndex[i] = 0
	}

	rf.applyChan = applyCh

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// self-used struct
	rf.recvVotes = 0
	rf.appendEntriesChan = make(chan AppendEntriesChan, ChanSize)
	rf.requestVoteChan = make(chan RequestVoteChan, ChanSize)
	rf.getStateChan = make(chan GetStateChan, ChanSize)
	rf.appendEntriesReplyChan = make(chan *AppendEntriesReply, ChanSize)
	rf.RequestVoteReplyChan = make(chan *RequestVoteReply, ChanSize)
	rf.endChan = make(chan int, 1)
	rf.clientCommandChan = make(chan ClientCommandChan, ChanSize)

	// empty Log
	// BUG: no need to append empty log IF log is not empty
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})
	}

	rand.Seed(time.Now().UnixNano())

	ElectionTimeOut := int32(250)
	HeartBeatTimeOut := 100

	go func(raft *Raft) {
		// now election timeout: [500-1000ms]
		// now heartbeat timeout: [150ms]
		timerElection := time.NewTimer(time.Duration(ElectionTimeOut+rand.Int31n(ElectionTimeOut)) * time.Millisecond)
		timerHeartBeat := time.NewTimer(time.Duration(HeartBeatTimeOut) * time.Millisecond)
		recvdHeartBeat := 0
		for {
			select {
			// --------------------------------------------  rpc call from leader ------------------------------------------------------------------
			case requestAppendEntries := <-raft.appendEntriesChan:
				// Check HeartBeat
				rf.raftState = Follower
				recvdHeartBeat = 1

				requestAppendEntries.Reply.Who = rf.me
				if requestAppendEntries.Request.Term < rf.currentTerm {
					DPrintf("AppendError Caused By Term Mismatch")
					requestAppendEntries.Reply.Success = 0
					requestAppendEntries.Reply.Term = rf.currentTerm
				} else {
					if requestAppendEntries.Request.Term > rf.currentTerm {
						rf.ModifyTerm(requestAppendEntries.Request.Term)
					}
					prevLogIndex := requestAppendEntries.Request.PrevLogIndex
					prevLogTerm := requestAppendEntries.Request.PrevLogTerm
					entries := requestAppendEntries.Request.Entries
					requestAppendEntries.Reply.Success = 0

					for i := len(rf.log) - 1; i >= 0; i-- {
						if rf.log[i].Index == prevLogIndex && rf.log[i].Term == prevLogTerm {
							requestAppendEntries.Reply.Success = 1
							rf.log = rf.log[0 : i+1]
							// rf.Log = append(rf.Log, entries)  // BUG: error here. append a alice as interface{} (single element)
							rf.log = append(rf.log, entries...)
							rf.persist()

							if requestAppendEntries.Request.LeaderCommitIndex > rf.commitIndex {
								if requestAppendEntries.Request.LeaderCommitIndex > rf.log[len(rf.log)-1].Index {
									rf.commitIndex = rf.log[len(rf.log)-1].Index
								} else {
									rf.commitIndex = requestAppendEntries.Request.LeaderCommitIndex
								}
							}
							requestAppendEntries.Reply.NextIndex = rf.log[len(rf.log)-1].Index + 1

							// TODO: replicate code. Index in the Log struct
							offsetApplied := rf.lastApplied - rf.log[0].Index
							offsetCommitted := rf.commitIndex - rf.log[0].Index
							for i := offsetApplied + 1; i <= offsetCommitted; i++ {
								DPrintf("Log Apply ================= index=%v len(log)=%v, Log=%v", i, len(rf.log), rf.log[i].Log)
								msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: rf.log[i].Index}
								rf.applyChan <- msg
								rf.lastApplied = i
							}
						}
					}
				}
				requestAppendEntries.DoneChan <- 1
				break
			case requestRequestVote := <-raft.requestVoteChan:
				// BUG: check log before we vote for the request node.
				if requestRequestVote.Request.Term < raft.currentTerm {
					requestRequestVote.Reply.VoteGranted = 0
					requestRequestVote.Reply.Term = raft.currentTerm
				} else {

					if requestRequestVote.Request.Term > raft.currentTerm {
						rf.ModifyTerm(requestRequestVote.Request.Term)
					}

					newer := false
					if requestRequestVote.Request.LastLogTerm == rf.log[len(rf.log)-1].Term {
						newer = requestRequestVote.Request.LastLogIndex >= rf.log[len(rf.log)-1].Index
					} else {
						newer = requestRequestVote.Request.LastLogTerm > rf.log[len(rf.log)-1].Term
					}

					if rf.votedFor == -1 && newer {
						requestRequestVote.Reply.VoteGranted = 1
						rf.votedFor = requestRequestVote.Request.CandidateId
						rf.raftState = Follower
						rf.persist()
					} else {
						requestRequestVote.Reply.VoteGranted = 0
					}
					requestRequestVote.Reply.Term = raft.currentTerm

				}

				requestRequestVote.DoneChan <- 1
				break
			case requestGetState := <-raft.getStateChan:
				reply := GetStateReply{IsLeader: raft.raftState == Leader, Term: raft.currentTerm}
				requestGetState.DoneChan <- reply
				break
			// --------------------------------------------  timeout ------------------------------------------------------------------
			case <-timerElection.C:
				if recvdHeartBeat == 0 && rf.raftState != Leader {
					DPrintf("%v start election state=%v at Term=%v\n", rf.me, StateString[rf.raftState], rf.currentTerm)
					rf.IncrementTerm()
					rf.raftState = Candidate
					rf.recvVotes = 1 // vote for self.
					rf.DoSendRequestVote()
				}
				timerElection = time.NewTimer(time.Duration(ElectionTimeOut+rand.Int31n(ElectionTimeOut)) * time.Millisecond)
				recvdHeartBeat = 0
				break
			case <-timerHeartBeat.C:
				// send heartbeat if current node is leader
				if rf.raftState == Leader {
					for i := 0; i < len(rf.peers); i++ {
						if i == rf.me {
							continue
						}
						rf.DoSendAppendEntries(i)
					}
				}
				timerHeartBeat = time.NewTimer(time.Duration(HeartBeatTimeOut) * time.Millisecond)
				break
			// --------------------------------------------  rpc reply ------------------------------------------------------------------
			case reply := <-rf.appendEntriesReplyChan:
				if reply.Success == 0 {
				}
				rf.ModifyTerm(reply.Term)
				if rf.raftState == Leader {
					if reply.Success == 0 {
						rf.nextIndex[reply.Who] = rf.nextIndex[reply.Who] - 1
						if rf.nextIndex[reply.Who] < rf.log[0].Index+1 {
							// Nothing to send.
							rf.nextIndex[reply.Who] = rf.log[0].Index + 1
						} else {
							// Send Again
							rf.DoSendAppendEntries(reply.Who)
						}
					} else {
						// update next
						rf.nextIndex[reply.Who] = reply.NextIndex
						rf.matchIndex[reply.Who] = reply.NextIndex - 1

						offsetApplied := rf.lastApplied - rf.log[0].Index
						offsetCommitted := 0

						for i := len(rf.log) - 1; i >= 0; i-- {
							nReplicated := 0
							if rf.log[i].Term == rf.currentTerm {
								for j := 0; j < len(rf.peers); j++ {
									DPrintf("node(%v) matchIndex=%v, LogIndex=%v", j, rf.matchIndex[j], rf.log[i].Index)
									if rf.matchIndex[j] >= rf.log[i].Index {
										nReplicated++
									}
								}
								DPrintf("--------------------------apply?-------------------------- who=%v nReplicated=%v index=%v term=%v lastApplied=%v committedIndex=%v", rf.me, nReplicated, rf.log[i].Index, rf.log[i].Term, rf.lastApplied, rf.log[i].Index)
								if nReplicated >= (len(rf.peers)+1)/2 {
									offsetCommitted = i
									break
								}
							}
						}
						for i := offsetApplied + 1; i <= offsetCommitted; i++ {
							rf.commitIndex = rf.log[i].Index
							DPrintf("Log Apply ================= index=%v len(log)=%v, Log=%v", i, len(rf.log), rf.log[i].Log)
							applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: rf.log[i].Index}
							rf.applyChan <- applyMsg
							rf.lastApplied = rf.log[i].Index
						}
					}
				}
				break
			case reply := <-rf.RequestVoteReplyChan:
				if reply.VoteGranted == 0 {
					rf.ModifyTerm(reply.Term)
				} else {
					rf.recvVotes = rf.recvVotes + 1
					if rf.recvVotes >= (len(rf.peers)+1)/2 {
						DPrintf("%v become leader at Term %v\n", rf.me, rf.currentTerm)
						rf.raftState = Leader
						rf.recvVotes = 0
						rf.votedFor = -1
						rf.persist()

						rf.nextIndex = make([]int, len(rf.peers))
						rf.matchIndex = make([]int, len(rf.peers))
						for i := range rf.peers {
							rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1 // BUG: initialize the nextIndex/matchIndex once the node become leader.
							rf.matchIndex[i] = 0
						}

						for i := 0; i < len(rf.peers); i++ {
							if i == rf.me {
								continue
							}
							rf.DoSendAppendEntries(i)
						}
					}
				}
			// --------------------------------------------  client command ------------------------------------------------------------------
			case clientCommand := <-rf.clientCommandChan:
				if rf.raftState != Leader {
					// Received a command But not a leader
					result := ClientCommandResult{Index: 0, Term: rf.currentTerm, IsLeader: 0}
					clientCommand.ClientCommandResultChan <- result
				} else {
					// A leader received a command
					// Reply to Client
					lastLogIndex := rf.log[len(rf.log)-1].Index
					rf.log = append(rf.log, LogEntry{Log: clientCommand.Command, Term: rf.currentTerm, Index: lastLogIndex + 1})
					rf.nextIndex[rf.me] = rf.log[len(rf.log)-1].Index + 1
					rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
					rf.persist()

					result := ClientCommandResult{Index: lastLogIndex + 1, Term: rf.currentTerm, IsLeader: 1}
					clientCommand.ClientCommandResultChan <- result
					// Notify all followers.
					for i := 0; i < len(rf.peers); i++ {
						if i == me {
							continue
						}
						rf.DoSendAppendEntries(i)
					}
					// BUG: rf.commitIndex = lastLogIndex:
					// CommitIndex Change when/after having majority's agreement
					// AppliedIndex: Send To Client
				}
			case <-rf.endChan:
				goto END
			}
		}
	END:
	}(rf)

	return rf
}
