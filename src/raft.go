//
// raft.go
// =======
// Write your code in this file
// We will use the original version of all other
// files for testing
//

package raft

//
// API
// ===
// This is an outline of the API that your raft implementation should
// expose.
//
// rf = NewPeer(...)
//   Create a new Raft server.
//
// rf.PutCommand(command interface{}) (index, term, isleader)
//   PutCommand agreement on a new log entry
//
// rf.GetState() (me, term, isLeader)
//   Ask a Raft peer for "me", its current term, and whether it thinks it
//   is a leader
//
// ApplyCommand
//   Each time a new entry is committed to the log, each Raft peer
//   should send an ApplyCommand to the service (e.g. tester) on the
//   same server, via the applyCh channel passed to NewPeer()
//

import (
	"fmt"
	"github.com/cmu440/rpc"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"
)

// Set to false to disable debug logs completely
// Make sure to set kEnableDebugLogs to false before submitting
const kEnableDebugLogs = false

// Set to true to log to stdout instead of file
const kLogToStdout = true

// Change this to output logs to a different directory
const kLogOutputDir = "./raftlogs/"

// least possible election timeout time
const electionBase = 400

// max election timeout range
const electionRange = 200

// heartbeat interval
const heartBeat = 100

// dummy
const buffer = 5000

// ApplyCommand
// ========
//
// As each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyCommand to the service (or
// tester) on the same server, via the applyCh passed to NewPeer()
type ApplyCommand struct {
	Index   int
	Command interface{}
}

type TermCommand struct {
	Term    int
	Command interface{}
}

type raftState int

const (
	Follower raftState = iota
	Candidate
	Leader
)

// Raft struct
// ===========
//
// A Go object implementing a single Raft peer
type Raft struct {
	mux   sync.Mutex       // Lock to protect shared access to this peer's state
	peers []*rpc.ClientEnd // RPC end points of all peers
	me    int              // this peer's index into peers[]
	// You are expected to create reasonably clear log files before asking a
	// debugging question on Piazza or OH. Use of this logger is optional, and
	// you are free to remove it completely.
	logger *log.Logger // We provide you with a separate logger per peer.

	// Your data here (2A, 2B).
	// Look at the Raft paper's Figure 2 for a description of what
	// state a Raft peer should maintain
	applyChan         chan ApplyCommand
	nextIndex         []int
	matchIndex        []int
	commitIndex       int
	lastApplied       int
	log               []TermCommand
	totalVotes        int
	heartBeatInterval time.Duration
	state             raftState
	term              int
	votedFor          int
	signalChan        chan time.Time
}

// GetState()
// ==========
// Gets the current state of the raft instance
// Return "me", current term and whether this peer
// believes it is the leader
func (rf *Raft) GetState() (int, int, bool) {
	var me int
	var term int
	var isleader bool
	rf.mux.Lock()
	defer rf.mux.Unlock()
	// Your code here (2A)
	if rf.state == Leader {
		isleader = true
	} else {
		isleader = false
	}
	me = rf.me
	term = rf.term
	return me, term, isleader
}

// RequestVoteArgs
// ===============
//
// # Example RequestVote RPC arguments structure
//
// Please note
// ===========
// Field names must start with capital letters!
// RequestVoteArgs struct with the same elements
// as the one in the paper
type RequestVoteArgs struct {
	// Your data here (2A, 2B)
	Term         int
	Candidate    int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// ================
//
// Example RequestVote RPC reply structure.
//
// Please note
// ===========
// Field names must start with capital letters!
// RequestVoteReply struct with the same elements
// as the one in the paper
type RequestVoteReply struct {
	// Your data here (2A)
	Term int
	Vote bool
}

type AppendEntriesArgs struct {
	HeartBeat    bool
	Leader       int
	Term         int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	Entries      []TermCommand
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

// This is a helper function that checks whether a candidateLog is as up to date
// as the voter log by comparing the terms and indices
func uptodate(candidTerm int, candidInd int, receiverTerm int, receiverInd int) bool {
	if candidTerm > receiverTerm {
		return true
	}
	if candidTerm < receiverTerm {
		return false
	}
	if candidInd >= receiverInd {
		return true
	} else {
		return false
	}
}

// RequestVote
// ===========
//
// Example RequestVote RPC handler
// RequestVote RPC that hanldes a request vote call
// Will update term if current term is out of date, in addition it will check
// whether the candidate should be voted for by checking if the server voted for
// anything in this term and also checks whether the candidate logs are up to date,
// if they are then we grant vote and don't grant otherwise
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B)
	rf.mux.Lock()
	receiverTerm := -1
	if len(rf.log) == 0 {
		receiverTerm = -1
	} else {
		receiverTerm = rf.log[len(rf.log)-1].Term
	}
	receiverInd := len(rf.log) - 1
	//candidate out of date
	if args.Term < rf.term {
		reply.Vote = false
		rf.logger.Printf("Server %d did not grant vote to %d \n", rf.me, args.Candidate)
	} else if args.Term == rf.term {
		//checks whether we voted before and whether the candidate logs are up to date
		if (rf.votedFor == -1 || rf.votedFor == args.Candidate) && uptodate(args.LastLogTerm, args.LastLogIndex, receiverTerm, receiverInd) {
			reply.Vote = true
			rf.votedFor = args.Candidate
			rf.logger.Printf("Server %d granted vote to %d on term %d \n", rf.me, args.Candidate, rf.term)
			rf.signalChan <- time.Now()

		} else {
			reply.Vote = false
			rf.logger.Printf("Server %d did not grant vote to %d on term %d \n", rf.me, args.Candidate, rf.term)
		}
	} else {
		//Checks whether candidate logs are up to date since our server is outdated
		if uptodate(args.LastLogTerm, args.LastLogIndex, receiverTerm, receiverInd) {
			reply.Vote = true
			rf.votedFor = args.Candidate
			rf.state = Follower
			rf.logger.Printf("Server %d voted for %d and became a follower on with terms %d %d \n", rf.me, args.Candidate, rf.term, args.Term)
			rf.signalChan <- time.Now()
		} else {
			reply.Vote = false
			rf.votedFor = -1
		}
	}
	rf.term = max(rf.term, args.Term)
	reply.Term = max(rf.term, args.Term)
	rf.mux.Unlock()

}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

// AppendEntries RPC call will handle any incoming appendEntries RPC to the
// server. If the term is greater in the argument, then the current server will
// revert back to follower state. Otherwise it will check the prevLogIndex to check
// whether or not it should send back a success message
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	rf.logger.Printf("Server %d has log: %v ", rf.me, rf.log)
	rf.logger.Printf("Server %d has log length %d", rf.me, len(rf.log))
	rf.signalChan <- time.Now()
	//rf.logger.Printf("Server %d received heartbeat from %d\n", rf.me, args.Leader)
	//Candidate received a RPC call from current leader, step down
	if args.Term == rf.term && rf.state == Candidate {
		reply.Term = args.Term
		rf.term = args.Term
		rf.logger.Printf("Server %d became a follower \n", rf.me)
		rf.state = Follower
	}
	//term is out of date, step down
	if args.Term > rf.term {
		reply.Term = args.Term
		rf.term = args.Term
		rf.logger.Printf("Server %d became a follower \n", rf.me)
		rf.state = Follower
		rf.votedFor = -1
	} else {
		reply.Term = rf.term
	}
	reply.Success = false
	//reply false if incoming term if lesser
	if args.Term < rf.term {
		reply.Success = false
		return
	}
	//prevLogIndex is not valid index in current log
	if len(rf.log) <= args.PrevLogIndex {
		reply.Success = false
		return
	}
	//ensures the terms are the same and prevLogIndex
	if args.PrevLogIndex != -1 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	}
	//Appends the entries onto log at the index prevLogInd + 1
	// if successful
	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	reply.Success = true
	rf.logger.Printf("Leader commit : %d and current commit is: %d", args.LeaderCommit, rf.commitIndex)
	// if leader commit is greater than current commit and we succeed, then we should commit all the way up
	// until leader commit or log length
	if args.LeaderCommit > rf.commitIndex {
		upper := min(len(rf.log)-1, args.LeaderCommit)
		for i := rf.commitIndex + 1; i <= upper; i++ {
			rf.applyChan <- ApplyCommand{Command: rf.log[i].Command, Index: i + 1}
			rf.logger.Printf("Just commited %v  at index %d on server %d", rf.log[i].Command, i+1, rf.me)
			rf.commitIndex = i
		}
	}

}

// sendRequestVote
// ===============
//
// # Example code to send a RequestVote RPC to a server
//
// server int -- index of the target server in
// rf.peers[]
//
// args *RequestVoteArgs -- RPC arguments in args
//
// reply *RequestVoteReply -- RPC reply
//
// The types of args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers)
//
// The rpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost
//
// # Call() sends a request and waits for a reply
//
// If a reply arrives within a timeout interval, Call() returns true;
// otherwise Call() returns false
//
// # Thus Call() may not return for a while
//
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply
//
// Call() is guaranteed to return (perhaps after a delay)
// *except* if the handler function on the server side does not return
//
// Thus there
// is no need to implement your own timeouts around Call()
//
// Please look at the comments and documentation in ../rpc/rpc.go
// for more details
//
// If you are having trouble getting RPC to work, check that you have
// capitalized all field names in the struct passed over RPC, and
// that the caller passes the address of the reply struct with "&",
// not the struct itself
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// PutCommand
// =====
//
// The service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log
//
// # If this server is not the leader, return false
//
// # Otherwise start the agreement and return immediately
//
// There is no guarantee that this command will ever be committed to
// the Raft log, since the leader may fail or lose an election
//
// The first return value is the index that the command will appear at
// if it is ever committed
//
// # The second return value is the current term
//
// The third return value is true if this server believes it is
// the leader
func (rf *Raft) PutCommand(command interface{}) (int, int, bool) {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if rf.state != Leader {
		return -1, -1, false
	}
	rf.logger.Printf("Inputted new command: %v\n", command)
	newCommand := TermCommand{Command: command, Term: rf.term}
	//Appends the command to the end of leader log
	rf.log = append(rf.log, newCommand)
	//Update both nextIndex for leader and matchIndex
	rf.nextIndex[rf.me] = len(rf.log)
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.logger.Printf("Index of command should be %d \n", len(rf.log))

	//this is to account for the fact that the logs are 1 based
	currLen := len(rf.log)
	return currLen, rf.term, true
}

// Stop
// ====
//
// The tester calls Stop() when a Raft instance will not
// be needed again
//
// You are not required to do anything
// in Stop(), but it might be convenient to (for example)
// turn off debug output from this instance
func (rf *Raft) Stop() {
	rf.logger.SetOutput(ioutil.Discard)

}

// This function checks whether the candidate is still in
// candiate state after its election timeo to ensure that
// it does not go on further
func (rf *Raft) checkRightState(state raftState) bool {
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if rf.state != state {
		return false
	} else {
		return true
	}
}

// Generates a random timeout between 400 to
// 600 ms
func generateTimeOut() time.Duration {
	timeout := electionBase + rand.Intn(electionRange)
	duration := time.Millisecond * time.Duration(timeout)
	return duration
}

// Function to send a heartbeat to a specific server and
// computes the response accordingly
func (rf *Raft) sendHeartbeat(server int) {
	rf.mux.Lock()
	args := &AppendEntriesArgs{HeartBeat: true,
		Leader: rf.me,
		Term:   rf.term}
	rf.mux.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if ok {
		if reply.Term > rf.term {
			rf.logger.Printf("Server %d became a follower \n", rf.me)
			rf.term = reply.Term
			rf.votedFor = -1
			rf.state = Follower
			rf.signalChan <- time.Now()
		}
	}
}

func max(a int, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

// This function broadcasts the appendEntries RPC
// call to all servers in a parallel fashion
func (rf *Raft) broadCast() {
	rf.mux.Lock()
	peers := len(rf.peers)
	for i := 0; i < peers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendLogEntries(i)
	}
	rf.mux.Unlock()
}

// This function broadcasts heartbeats
// call to all servers in a parallel fashion
func (rf *Raft) broadCastHeartBeat() {
	rf.mux.Lock()
	peers := len(rf.peers)
	for i := 0; i < peers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendHeartbeat(i)
	}
	rf.mux.Unlock()
}

// The leader routine, it will first check if there
// is some N that is greater than the commitedIndex
// and N has been matched on a majority of server, if
// so then it will commit the entries in the log up to N
// It will also broadcast appendEntries RPC to each of the
// servers
func (rf *Raft) leaderRoutine() {
	rf.mux.Lock()
	majority := (len(rf.peers) / 2) + 1
	end := rf.commitIndex
	rf.matchIndex[rf.me] = len(rf.log) - 1
	rf.logger.Printf("Current matchIndex for leader: %v", rf.matchIndex)
	rf.logger.Printf("Leader term: %d", rf.term)
	//Check for each index whether it has been replicated on a majority
	for i := rf.commitIndex + 1; i < len(rf.log); i++ {
		count := 0
		for server := range rf.peers {
			if rf.log[i].Term == rf.term && rf.matchIndex[server] >= i {
				rf.logger.Printf("Count updated %d", rf.me)
				count++
			}
			if rf.log[i].Term != rf.term {
				rf.logger.Printf("Terms don't match %d", rf.me)
			}
		}
		//replicated on a majority
		if count >= majority {
			end = i
		}
	}
	//Possible new commitIndex due to replication on majority of servers
	for i := rf.commitIndex + 1; i <= end; i++ {
		rf.logger.Printf("Trying to commit at server %d", rf.me)
		rf.applyChan <- ApplyCommand{Command: rf.log[i].Command, Index: i + 1}
		rf.logger.Printf("Just commited %v  at index %d on server %d", rf.log[i].Command, i+1, rf.me)
		rf.lastApplied = i
		rf.commitIndex = i
	}
	//rf.broadCastHeartBeat()
	rf.mux.Unlock()
	rf.broadCast()
	time.Sleep(rf.heartBeatInterval)

}

// This sends an appendEntry RPC with the leader's
// log to some server, the server will either accept
// or reject the log, in the former case then we would
// change nextIndex and matchIndex accordingly and in
// the second case we would back off our nextIndex
// for the server. In this implementation, both appendEntries
// and heartbeats are combined together as really one RPC call
func (rf *Raft) sendLogEntries(server int) {
	rf.mux.Lock()
	serverLogIndex := rf.nextIndex[server] - 1
	var prevLogTerm int
	//edge cases to check if current log is empty
	if serverLogIndex < 0 {
		prevLogTerm = -1
	} else {
		prevLogTerm = rf.log[serverLogIndex].Term
	}
	logLength := len(rf.log)
	args := &AppendEntriesArgs{HeartBeat: false,
		Leader:       rf.me,
		Term:         rf.term,
		PrevLogIndex: serverLogIndex,
		PrevLogTerm:  prevLogTerm,
		LeaderCommit: rf.commitIndex,
	}
	if rf.nextIndex[server] <= logLength {
		args.Entries = rf.log[rf.nextIndex[server]:logLength]
		//	rf.logger.Print(args.Entries)
	}
	rf.logger.Printf("Sending AppendEntries with prevInd %d and prevTerm %d to server %d \n", serverLogIndex, prevLogTerm, server)
	rf.mux.Unlock()
	reply := &AppendEntriesReply{}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if ok {
		//server successfully replicated, update nextInd and matchInd for server
		if reply.Success {
			rf.nextIndex[server] = min(len(rf.log), rf.nextIndex[server]+len(args.Entries))
			rf.matchIndex[server] = max(rf.matchIndex[server], serverLogIndex+len(args.Entries))
			rf.logger.Printf("Server %d successfully appended entries, new nextIndex is %d and new matchIndex is %d \n", server, rf.nextIndex[server], rf.matchIndex[server])
		} else {
			//term out of date
			if reply.Term > rf.term {
				rf.term = reply.Term
				rf.votedFor = -1
				rf.state = Follower
				rf.signalChan <- time.Now()
			} else {
				//backoff nextInd since server was unsuccessful
				// in replicating log
				if rf.nextIndex[server] == 0 {
					rf.nextIndex[server] = 0
				} else {
					rf.nextIndex[server]--
				}
			}
		}
	}
}

// This function sends a vote request RPC to some server
// The server can choose to either vote or not vote for
// the candidate, for each vote we increment the total
// votes of the candidate.
func (rf *Raft) sendVoteRequest(server int) {
	rf.mux.Lock()
	lastTerm := 0
	if len(rf.log) == 0 {
		lastTerm = -1
	} else {
		lastTerm = rf.log[len(rf.log)-1].Term
	}
	args := &RequestVoteArgs{Term: rf.term,
		Candidate: rf.me, LastLogIndex: len(rf.log) - 1, LastLogTerm: lastTerm}
	rf.mux.Unlock()
	reply := &RequestVoteReply{}
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mux.Lock()
	defer rf.mux.Unlock()
	if ok {
		//current term is out of date
		if reply.Term > rf.term {
			rf.term = reply.Term
			rf.votedFor = -1
			rf.logger.Printf("Server %d became a follower \n", rf.me)
			rf.state = Follower
			return
		} else {
			//received vote
			if reply.Vote {
				rf.totalVotes++
			}
		}
	}

}

// This function broadcasts a voteRequest RPC
// to all possible servers in a parallel fashion
func (rf *Raft) beginElection() {
	rf.mux.Lock()
	servers := len(rf.peers)
	for i := 0; i < servers; i++ {
		if i == rf.me {
			continue
		} else {
			go rf.sendVoteRequest(i)
		}
	}
	rf.mux.Unlock()
}

// The candidate routine, it will first have a timeout
// and will send out all voteRequests, if not enough votes
// were gathered or the state changed after the timeout, then
// we revert back to follower state and wait for reelection
func (rf *Raft) candidateRoutine() {
	timeout := generateTimeOut()
	rf.mux.Lock()
	servers := len(rf.peers)
	majority := servers/2 + 1
	rf.mux.Unlock()
	//begin election
	rf.beginElection()
	time.Sleep(timeout)
	//checks if there has been a state change
	if !rf.checkRightState(Candidate) {
		return
	}

	rf.mux.Lock()
	defer rf.mux.Unlock()
	//check if have majority of votes
	if rf.totalVotes >= majority { //Is leader s
		rf.logger.Printf("Server %d became leader \n", rf.me)
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.matchIndex[i] = -1
			rf.nextIndex[i] = len(rf.log)
		}
		rf.state = Leader
		return
	} else {
		//revert to follower state if not enough votes
		rf.state = Follower
		return
	}

}

// The follower routine, this simply has a ticker that
// waits a random time generated by the election timeout
// and we either reset the ticker if we receive some valid
// RPC call from the leader or candidate and if it times out then
// we become the candidate
func (rf *Raft) followerRoutine() {
	duration := generateTimeOut()
	for {
	RESTART:
		ticker := time.NewTicker(duration)
		start := time.Now()
	SELECT:
		select {
		case receiveTime := <-rf.signalChan:
			//recevied some RPC call during timeoutphase
			// so we reset
			if start.Before(receiveTime) {
				goto RESTART

				//the received RPC call was actually
				// before the timeout began so ignore
			} else {
				goto SELECT
			}
			// timed out
		case <-ticker.C:
			rf.mux.Lock()
			rf.logger.Printf("Server %d timed out to become candidates \n", rf.me)
			//fmt.Println(now)
			rf.totalVotes = 1
			rf.votedFor = rf.me
			rf.term++
			rf.state = Candidate
			rf.mux.Unlock()
			return

		}
	}

}

// The main routine for each raft instance, depending on the state it is
// in after each check, we will execute the corresponding routine
// for that state, the states are leader, follower, candidate
func (rf *Raft) mainRoutine() {
	for {
		rf.mux.Lock()
		status := rf.state
		rf.logger.Printf("Server %d has log %v:", rf.me, rf.log)
		//me := rf.me
		rf.mux.Unlock()
		switch status {
		case Candidate:
			//	fmt.Printf("%d is currently a candidate \n", me)
			rf.candidateRoutine()
		case Leader:
			//	fmt.Printf("%d is currently a leader \n", me)
			rf.leaderRoutine()
		case Follower:
			//	fmt.Printf("%d is currently a follower \n", me)
			rf.followerRoutine()
		}
	}
}

// NewPeer
// ====
//
// # The service or tester wants to create a Raft server
//
// The port numbers of all the Raft servers (including this one)
// are in peers[]
//
// This server's port is peers[me]
//
// All the servers' peers[] arrays have the same order
//
// applyCh
// =======
//
// applyCh is a channel on which the tester or service expects
// Raft to send ApplyCommand messages
//
// NewPeer() must return quickly, so it should start Goroutines
// for any long-running work
func NewPeer(peers []*rpc.ClientEnd, me int, applyCh chan ApplyCommand) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.me = me

	if kEnableDebugLogs {
		peerName := peers[me].String()
		logPrefix := fmt.Sprintf("%s ", peerName)
		if kLogToStdout {
			rf.logger = log.New(os.Stdout, peerName, log.Lmicroseconds|log.Lshortfile)
		} else {
			err := os.MkdirAll(kLogOutputDir, os.ModePerm)
			if err != nil {
				panic(err.Error())
			}
			logOutputFile, err := os.OpenFile(fmt.Sprintf("%s/%s.txt", kLogOutputDir, logPrefix), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
			if err != nil {
				panic(err.Error())
			}
			rf.logger = log.New(logOutputFile, logPrefix, log.Lmicroseconds|log.Lshortfile)
		}
		//rf.logger.Println("logger initialized")
	} else {
		rf.logger = log.New(ioutil.Discard, "", 0)
	}
	//initialization code
	rf.log = make([]TermCommand, 0)
	rf.commitIndex = -1
	rf.lastApplied = -1
	rf.term = 1
	rf.totalVotes = 0
	rf.votedFor = -1
	rf.state = Follower
	rf.applyChan = applyCh
	rf.heartBeatInterval = time.Duration(heartBeat * time.Millisecond)
	rf.signalChan = make(chan time.Time, buffer)
	go rf.mainRoutine()
	return rf
}
