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
	ChanSize = 2000
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
	recvVotes              int
	clientCommandChan      chan ClientCommandChan
	appendEntriesChan      chan AppendEntriesChan
	requestVoteChan        chan RequestVoteChan
	getStateChan           chan GetStateChan
	appendEntriesReplyChan chan *AppendEntriesReply
	RequestVoteReplyChan   chan *RequestVoteReply
	endChan                chan int
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
	ReqTerm   int
	Term      int
	Success   int
	Who       int
	NextIndex int
}

func (reply *AppendEntriesReply) ToString() string {
	return "Term=" + strconv.Itoa(reply.Term) + ", Success=" + strconv.Itoa(reply.Success) + ", NextIndex=" + strconv.Itoa(reply.NextIndex)
}

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

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	ReqTerm     int
	Term        int
	VoteGranted int
}

func (reply *RequestVoteReply) ToString() string {
	return "Term=" + strconv.Itoa(reply.Term) + ", voteGranted=" + strconv.Itoa(reply.VoteGranted)
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	// DPrintf("!!!!!!!!!!!!!!!!!!!!!!!!Write Persist!!!!!!!!!!!!!!!!!!!!!   node(%v)  term=%v,voteFor=%v,logsize=%v lastlog=%v", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.log[len(rf.log)-1])

	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	rf.mu.Lock()
	defer rf.mu.Unlock()

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	if d.Decode(&rf.currentTerm) != nil ||
		d.Decode(&rf.votedFor) != nil ||
		d.Decode(&rf.log) != nil {
		panic("read persist error")
	}
	DPrintf("!!!!!!!!!!!!!!!!!!!!!!!!Read Persist!!!!!!!!!!!!!!!!!!!!!   node(%v)  term=%v,voteFor=%v,logsize=%v lastLog=%v", rf.me, rf.currentTerm, rf.votedFor, len(rf.log), rf.log[len(rf.log)-1])
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	DPrintf("(%v) request vote from (%v)  (%v)", rf.me, args.CandidateId, args.ToString())
	st := RequestVoteChan{Request: args, Reply: reply, DoneChan: make(chan int, 1)}
	rf.requestVoteChan <- st
	<-st.DoneChan
}
func (rf *Raft) RequestAppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf("(%v) append entry from (%v)  (%v)", rf.me, args.LeaderId, args.ToString())
	st := AppendEntriesChan{Request: args, Reply: reply, DoneChan: make(chan int, 1)}
	rf.appendEntriesChan <- st
	<-st.DoneChan
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
	DPrintf("(%v) request vote to   (%v)   (%v)\n", rf.me, server, args.ToString())
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
		//panic("rpc error send request vote")
		return ok
	}
	DPrintf("(%v) receive vote from (%v)   (%v)\n", rf.me, server, reply.ToString())
	rf.RequestVoteReplyChan <- reply
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	reply = &AppendEntriesReply{}
	DPrintf("(%v) append entry to   (%v)   (%v)\n", rf.me, server, args.ToString())
	ok := rf.peers[server].Call("Raft.RequestAppendEntries", args, reply)
	if !ok {
		//panic("rpc error send append entries")
		return ok
	}
	DPrintf("(%v) recv app entry from (%v)  (%v)\n", rf.me, server, reply.ToString())
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
	DPrintf("(%v) Received a client Command at Term (%v) state=%v", rf.me, rf.currentTerm, StateString[rf.raftState])
	clientCommand := ClientCommandChan{Command: command, ClientCommandResultChan: make(chan ClientCommandResult, 1)}
	rf.clientCommandChan <- clientCommand
	commandResult := <-clientCommand.ClientCommandResultChan
	isLeader := false
	if commandResult.IsLeader > 0 {
		isLeader = true
	}
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

func (rf *Raft) ModifyTerm(term int) {
	if term > rf.currentTerm {
		rf.currentTerm = term
		rf.votedFor = -1
		// rf.recvVotes = 0
		rf.raftState = Follower
		rf.persist()
	}
}

func (rf *Raft) DoSendAppendEntries(To int) {
	if rf.raftState != Leader {
		panic("not a leader")
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	offset := rf.nextIndex[To] - rf.log[0].Index
	if offset > len(rf.log) {
		offset = len(rf.log)
	}

	entries := make([]LogEntry, len(rf.log[offset:]))
	copy(entries, rf.log[offset:])
	args := &AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me, PrevLogIndex: rf.log[offset-1].Index,
		PrevLogTerm: rf.log[offset-1].Term, Entries: entries, LeaderCommitIndex: rf.commitIndex}

	go func(i int, args *AppendEntriesArgs) {
		rf.sendAppendEntries(i, args, nil)
	}(To, args)
}
func (rf *Raft) DoSendRequestVote() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.raftState != Candidate {
			panic("not a candidate")
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

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	// BUG: no need to append empty log IF log is not empty
	if len(rf.log) == 0 {
		rf.log = append(rf.log, LogEntry{Term: 0, Index: 0})
	}

	// volatile state for leader
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))
	for i := 0; i < len(peers); i++ {
		rf.nextIndex[i] = rf.log[len(rf.log)-1].Index
		rf.matchIndex[i] = 0
	}

	rf.applyChan = applyCh

	// self-used struct
	rf.recvVotes = 0
	rf.appendEntriesChan = make(chan AppendEntriesChan, ChanSize)
	rf.requestVoteChan = make(chan RequestVoteChan, ChanSize)
	rf.getStateChan = make(chan GetStateChan, ChanSize)
	rf.appendEntriesReplyChan = make(chan *AppendEntriesReply, ChanSize)
	rf.RequestVoteReplyChan = make(chan *RequestVoteReply, ChanSize)
	rf.endChan = make(chan int, ChanSize)
	rf.clientCommandChan = make(chan ClientCommandChan, ChanSize)

	rand.Seed(time.Now().UnixNano())

	// now election timeout: [500-1000ms]
	// now heartbeat timeout: [150ms]
	ElectionTimeOut := int32(500)
	HeartBeatTimeOut := 120

	go func(raft *Raft) {
		timerElection := time.NewTimer(time.Duration(ElectionTimeOut+rand.Int31n(ElectionTimeOut)) * time.Millisecond)
		timerHeartBeat := time.NewTimer(time.Duration(HeartBeatTimeOut) * time.Millisecond)
		heartBeat := 0
		for {
			select {
			// --------------------------------------------  rpc call from leader ------------------------------------------------------------------
			case requestAppendEntries := <-raft.appendEntriesChan:
				reply := requestAppendEntries.Reply
				request := requestAppendEntries.Request

				reply.Who = rf.me
				reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
				reply.Success = 0
				reply.ReqTerm = request.Term

				if request.Term < rf.currentTerm {
					reply.Term = rf.currentTerm
					requestAppendEntries.DoneChan <- 1
					break
				}

				// Check HeartBeat And Term
				heartBeat = 1
				if request.Term > rf.currentTerm {
					rf.ModifyTerm(request.Term)
				}
				rf.raftState = Follower
				reply.Term = rf.currentTerm

				prevLogIndex := request.PrevLogIndex
				prevLogTerm := request.PrevLogTerm
				entries := request.Entries
				// BUG: CHECK length for log & Leader'PrevLogIndex
				if request.PrevLogIndex > rf.log[len(rf.log)-1].Index {
					requestAppendEntries.DoneChan <- 1
					break
				}

				offset := prevLogIndex - rf.log[0].Index
				// BUG: case : check log term. decrease one each time...
				// FIND FIRST LOG that has same index & term with leader.
				term := rf.log[offset].Term // prevLog's Term.
				if prevLogTerm != term {
					for i := offset - 1; i >= 0; i-- {
						if rf.log[i].Term != term { // optimization: jump over a segment which has same term
							reply.NextIndex = rf.log[i].Index + 1
							break
						}
					}
					requestAppendEntries.DoneChan <- 1
					break
				}

				rf.log = rf.log[:offset+1]
				rf.log = append(rf.log, entries...)
				rf.persist()
				reply.Success = 1
				reply.NextIndex = rf.log[len(rf.log)-1].Index + 1

				if request.LeaderCommitIndex > rf.commitIndex {
					last := rf.log[len(rf.log)-1].Index
					if request.LeaderCommitIndex > last {
						rf.commitIndex = last
					} else {
						rf.commitIndex = request.LeaderCommitIndex
					}

					// TODO: replicate code.
					offsetApplied := rf.lastApplied - rf.log[0].Index
					offsetCommitted := rf.commitIndex - rf.log[0].Index
					for i := offsetApplied + 1; i <= offsetCommitted; i++ {
						DPrintf("================================== (%v) Log Apply : index=%v term=%v len(log)=%v, Log=%v", rf.me, rf.log[i].Index, rf.log[i].Term, len(rf.log), rf.log[i].Log)
						msg := ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: rf.log[i].Index}
						rf.applyChan <- msg
						rf.lastApplied = rf.log[i].Index
					}
				}

				requestAppendEntries.DoneChan <- 1
				break
			case requestRequestVote := <-raft.requestVoteChan:
				req := requestRequestVote.Request
				rsp := requestRequestVote.Reply

				rsp.ReqTerm = req.Term
				// Check Term & state
				rsp.VoteGranted = 0
				if req.Term < raft.currentTerm {
					rsp.Term = raft.currentTerm
					requestRequestVote.DoneChan <- 1
					break
				}
				if req.Term > raft.currentTerm {
					rf.ModifyTerm(req.Term)
				}
				rsp.Term = raft.currentTerm

				// BUG: check log before we vote for the request node.
				// only leader can append entries.
				newer := false
				if req.LastLogTerm == rf.log[len(rf.log)-1].Term {
					newer = req.LastLogIndex >= rf.log[len(rf.log)-1].Index
				} else {
					newer = req.LastLogTerm > rf.log[len(rf.log)-1].Term
				}

				if (rf.votedFor == -1 || rf.votedFor == req.CandidateId) && newer {
					rsp.VoteGranted = 1
					rf.votedFor = req.CandidateId
					rf.raftState = Follower
					rf.persist()
					// BUG: reset timer.
					timerElection.Stop()
					timerElection.Reset(time.Duration(ElectionTimeOut+rand.Int31n(ElectionTimeOut)) * time.Millisecond)
				}

				requestRequestVote.DoneChan <- 1
				break
			case requestGetState := <-raft.getStateChan:
				reply := GetStateReply{IsLeader: raft.raftState == Leader, Term: raft.currentTerm}
				requestGetState.DoneChan <- reply
				break
			// --------------------------------------------  timeout ------------------------------------------------------------------
			case <-timerElection.C:
				if heartBeat == 0 && rf.raftState != Leader {
					rf.currentTerm = rf.currentTerm + 1
					DPrintf("(%v) start election state=%v at Term=%v\n", rf.me, StateString[rf.raftState], rf.currentTerm)
					rf.raftState = Candidate
					rf.recvVotes = 1 // vote for self.
					rf.votedFor = rf.me
					rf.persist()
					rf.DoSendRequestVote()
				}
				timerElection.Stop()
				timerElection.Reset(time.Duration(ElectionTimeOut+rand.Int31n(ElectionTimeOut)) * time.Millisecond)
				heartBeat = 0
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
				timerHeartBeat.Stop()
				timerHeartBeat.Reset(time.Duration(HeartBeatTimeOut) * time.Millisecond)
			// --------------------------------------------  rpc reply ------------------------------------------------------------------
			case reply := <-rf.appendEntriesReplyChan:

				if reply.ReqTerm != rf.currentTerm {
					break
				}
				if reply.Term > rf.currentTerm {
					rf.ModifyTerm(reply.Term)
					break
				}

				if rf.raftState != Leader {
					break
				}

				// BUG???: only deal with same term request's reply.
				// denpending on success or not success
				//if reply.ReqTerm != rf.currentTerm {
				//	break
				//}

				if reply.Success == 0 {
					rf.nextIndex[reply.Who] = reply.NextIndex
					rf.DoSendAppendEntries(reply.Who) // Send Again
					break
				}
				// update next
				rf.nextIndex[reply.Who] = reply.NextIndex
				rf.matchIndex[reply.Who] = reply.NextIndex - 1

				offsetApplied := rf.lastApplied - rf.log[0].Index
				offsetCommitted := rf.commitIndex - rf.log[0].Index

				for i := offsetCommitted + 1; i < len(rf.log); i++ {
					nReplicated := 1
					if rf.log[i].Term == rf.currentTerm { // Leader Send Append-> Other Election -> Leader Term Changed -> Leader Recv Reply   (would not commit)
						//if rf.log[i].Term == reply.ReqTerm {
						for j := 0; j < len(rf.peers); j++ {
							if j == rf.me {
								continue
							}
							if rf.matchIndex[j] >= rf.log[i].Index {
								nReplicated++
							}
						}
						if nReplicated > len(rf.peers)/2 {
							offsetCommitted = i
						}
					}
				}
				for i := offsetApplied + 1; i <= offsetCommitted; i++ {
					rf.commitIndex = rf.log[i].Index
					DPrintf(" ==================================(%v) Log Apply index=%v term=%v, len(log)=%v, Log=%v", rf.me, rf.log[i].Index, rf.log[i].Term, len(rf.log), rf.log[i].Log)
					applyMsg := ApplyMsg{CommandValid: true, Command: rf.log[i].Log, CommandIndex: rf.log[i].Index}
					rf.applyChan <- applyMsg
					rf.lastApplied = rf.log[i].Index
				}

			case reply := <-rf.RequestVoteReplyChan:

				if reply.Term > rf.currentTerm {
					rf.ModifyTerm(reply.Term)
					break
				}

				if rf.raftState != Candidate {
					break
				}

				if reply.ReqTerm != rf.currentTerm {
					break //BUG: only consider current term's reply
				}

				if reply.VoteGranted == 0 {
					break
				}
				rf.recvVotes = rf.recvVotes + 1
				if rf.recvVotes > len(rf.peers)/2 {
					DPrintf("%v become leader at Term %v\n", rf.me, rf.currentTerm)
					rf.raftState = Leader
					rf.recvVotes = 0
					//rf.votedFor = -1
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

			// --------------------------------------------  client command ------------------------------------------------------------------
			case clientCommand := <-rf.clientCommandChan:
				if rf.raftState != Leader {
					result := ClientCommandResult{Index: 0, Term: rf.currentTerm, IsLeader: 0}
					clientCommand.ClientCommandResultChan <- result
				} else {
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
					// BUG:
					// CommitIndex Change when/after having majority's agreement
					// AppliedIndex: Send To Client
				}
			case <-rf.endChan:
				//fmt.Println("    =================================================KILLED ================================================= ")
				//DPrintf("================================================================================================== KILLED =================================================")
				//goto END
			}
		}
		//END:
	}(rf)

	// tester call RPC --> Message Chan --> Kill() ---> tester Wait for rpc reply forever ???
	// Start / GetState /

	// should SyncDealing RPC RequestVote/RequestAppendEntries .???? not needed, since rpc's inherently uncertain

	// !!! votedFor modification time: in case of multiple leaders. !!!

	return rf
}
