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

	//	"6.824/labgob"
	"6.824/labrpc"
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

const (
	Leader = iota
	Candidate
	Follower
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm  int            // lastest term server has seen
	votedFor     int            // vote for candidate in current term
	logOperation []LogOperation // command operation
	currentRole  int            // state leader, follower, candidate

	lastApplied int
	leaderId    int

	electionTimeout time.Time
}

// func (rf Raft) String() string{
// 	return fmt.Sprintf("%d %d %d", rf.currentTerm, rf.lastApplied, )
// }

type LogOperation struct {
	Term int
	// command string
	// value string
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.currentRole == Leader
	return term, isleader
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
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	CurrentTerm int
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Trường hợp this peer là leader, nhận request vote của candidate thì chuyện gì xảy ra?
	// Nếu this peer là leader nhưng nhận request vote của candidate thì term của
	// this peer sẽ phải lớn hơn term của candidate => RequestVote của candidate failed
	// Trường hợp term của this peer nhỏ hơn term của candidate => RequestVote của candidate hợp lệ => this peer từ leader hoặc candidate thành Follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term

		// hạ bậc role if this peer has stale term
		rf.currentRole = Follower
		rf.votedFor = -1
	}

	var lastTerm = 0
	// check log validate. Only access when logOperation's length > 0
	if len(rf.logOperation) > 0 {
		lastTerm = rf.logOperation[rf.lastApplied].Term
	}

	log := (lastTerm > 0 && args.LastLogTerm >= lastTerm && args.LastLogIndex >= rf.lastApplied) ||
		(lastTerm == 0 && args.LastLogIndex == rf.lastApplied)

	// DPrintf("[%d] log=%v votedFor=%v currentTerm=%v", rf.me, log, rf.votedFor, rf.currentTerm)

	// chưa vote cho ai trong term
	// candidate's log is at least as up-to-date as this peer log
	// Case: term 3 and this peer is foller.
	// Do VotedFor chỉ cập nhật khi RequestVote đc gọi nên làm sao có thể phân biệt giữa VotedFor của current term và VotedFor new term?
	// Phân biệt nhau dựa vào log index và term trong log. Sau khi đã vote thì this peer sẽ ghi nhận new term = 4
	// nếu nó nhận requestVote khác cũng là term 3 nhưng vì this peer có term là 4 mà 3 < 4 => bị failed ở điều kiện check đầu tiên

	// TODO: kiểm tra case term 1 failed, bắt đầu term 2. Truy vấn vào array logOperation có thể lỗi do term 1 ko thành công nên ko có log
	// TODO: nhận n request nhưng có term giống nhau. Ví dụ term 3 lần 1 votedFor 0, term 3 lần 2 phải reject vì đã voted rồi
	if rf.currentTerm == args.Term && log && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.CurrentTerm = args.Term
		rf.currentTerm = args.Term
		DPrintf("[%d] vote for [%d] within term %d", rf.me, rf.votedFor, args.Term)
	} else {
		DPrintf("current term %d - [%d] reject vote for [%d] within term %d", rf.currentTerm, rf.me, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.CurrentTerm = rf.currentTerm
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex string
	PreLogTerm   string
	Entries      []LogOperation
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	currentTerm := rf.currentTerm

	// the period of election
	if args.Term > currentTerm {
		rf.currentRole = Follower
		currentTerm = args.Term
	}

	if currentTerm == args.Term {
		// reset timeout
		if len(args.Entries) == 0 {
			reset := time.Now()
			DPrintf("[%d] reset timeout from [%d] within term %d - time %v", rf.me, args.LeaderId, args.Term, reset)
			rf.electionTimeout = reset
		}
		rf.leaderId = args.LeaderId
		rf.currentTerm = currentTerm
		reply.Term = rf.currentTerm
		reply.Success = true
	} else {
		reply.Success = false
		reply.Term = currentTerm
	}
}

// leader send periodic heartbeats with AppendEntriArgs no carry log entries
func (rf *Raft) sendHeartbeat() bool {
	type Result struct {
		serverId  int
		okNetwork bool
		reply     AppendEntriesReply
	}
	isLeader := true
	// before send a heart beat should check this peer still be a leader
	// DPrintf("[%d] prepare sending heart beat", rf.me)
	rf.mu.Lock()
	if rf.currentRole == Follower || rf.currentRole == Candidate {
		rf.mu.Unlock()
		isLeader = false
		return isLeader
	}
	currentTerm := rf.currentTerm
	args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, Entries: []LogOperation{}}
	rf.mu.Unlock()
	// replyChan := make(chan Result, 15)
	// var wg sync.WaitGroup
	for i := range rf.peers {
		if i != rf.me {
			// wg.Add(1)
			go func(i int) {
				// defer wg.Done()
				reply := AppendEntriesReply{}
				ok := false
				// for !notOk {
				// DPrintf("[%d] before serverid=%d", rf.me, i)
				ok = rf.sendAppendEntries(i, &args, &reply)
				// DPrintf("[%d] after serverid=%d ok=%v", rf.me, i, ok)
				// }
				// replyChan <- Result{i, ok, reply}
				if !ok {
					// DPrintf("[%d] send heartbeat to [%d] but network failed", rf.me, i)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				if !reply.Success && reply.Term > currentTerm {
					rf.currentTerm = reply.Term
					rf.currentRole = Follower
					isLeader = false
					// DPrintf("[%d] send heartbeat but got new term %d [currentRole=%v]", rf.me, rf.currentTerm, rf.currentRole)
				}
			}(i)
		}
	}

	// go func() {
	// 	wg.Wait()
	// 	close(replyChan)
	// }()

	// for result := range replyChan {
	// 	// update term if this peer has a stale term
	// 	// If a candidate or leader discovers that its term is out of date, it immediately reverts to follower state
	// 	if result.okNetwork {
	// 		rf.mu.Lock()
	// 		if !result.reply.Success && result.reply.Term > currentTerm {
	// 			rf.currentTerm = result.reply.Term
	// 			rf.currentRole = Follower
	// 			isLeader = false
	// 			DPrintf("[%d] send heartbeat but got new term %d [currentRole=%v]", rf.me, rf.currentTerm, rf.currentRole)
	// 		}
	// 		rf.mu.Unlock()
	// 	} else {
	// 		DPrintf("[%d] send heartbeat to [%d] but network failed", rf.me, result.serverId)
	// 	}
	// }
	// DPrintf("[%d] sent all heartbeat", rf.me)
	return isLeader
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

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// prepare and sending out requestvote
func (rf *Raft) startElection() {
	type Result struct {
		serverId int
		ok       bool
		reply    RequestVoteReply
	}
	rf.mu.Lock()
	rf.votedFor = rf.me
	rf.currentRole = Candidate
	rf.currentTerm++
	lastTerm := 0
	rf.electionTimeout = time.Now()
	if len(rf.logOperation) > 0 {
		lastTerm = rf.logOperation[rf.lastApplied].Term
	}
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		LastLogIndex: rf.lastApplied,
		LastLogTerm:  lastTerm,
		CandidateId:  rf.me,
	}
	rf.mu.Unlock()
	majority := 1
	doneVote := false
	DPrintf("[%d] starting an election at term %d", rf.me, args.Term)
	// var wg sync.WaitGroup
	// resultChannel := make(chan Result, 1)
	for i := range rf.peers {
		if i != rf.me {
			// wg.Add(1)
			go func(i int, args RequestVoteArgs) {
				// defer wg.Done()
				DPrintf("[%d] sending request vote to [%d] at term %d", args.CandidateId, i, args.Term)
				reply := RequestVoteReply{}
				ok := false
				// for !ok {
				ok = rf.sendRequestVote(i, &args, &reply)
				if !ok {
					return
				}

				// }
				// resultChannel <- Result{i, ok, reply}
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm < reply.CurrentTerm {
					rf.currentTerm = reply.CurrentTerm
					rf.currentRole = Follower
				} else if reply.VoteGranted && rf.currentTerm == reply.CurrentTerm {
					// count majority of the servers in cluster
					// tally the votes
					DPrintf("[%d] receiver the vote [%d] at term %d", rf.me, i, rf.currentTerm)
					majority++
					if doneVote || majority <= len(rf.peers)/2 {
						// goto Final
						return
					}
					// if doneVote || majority >= len(rf.peers)/2+1 {
					rf.currentRole = Leader
					doneVote = true
					go rf.heartbeat(rf.currentTerm)
					// }
				}
				// Final:
			}(i, args)
		}
	}

	// rf.mu.Unlock()

	// go func() {
	// 	wg.Wait()
	// 	close(resultChannel)
	// }()

	// for result := range resultChannel {
	// 	if result.ok {
	// 		rf.mu.Lock()
	// 		if rf.currentTerm < result.reply.CurrentTerm {
	// 			rf.currentTerm = result.reply.CurrentTerm
	// 			rf.currentRole = Follower
	// 		} else if result.reply.VoteGranted && rf.currentTerm == result.reply.CurrentTerm {
	// 			// count majority of the servers in cluster
	// 			// tally the votes
	// 			DPrintf("[%d] receiver the vote [%d] at term %d", rf.me, result.serverId, rf.currentTerm)
	// 			majority++
	// 			if doneVote || majority <= len(rf.peers)/2 {
	// 				// goto Final
	// 				rf.mu.Unlock()
	// 				continue
	// 			}
	// 			// if doneVote || majority >= len(rf.peers)/2+1 {
	// 			rf.currentRole = Leader
	// 			doneVote = true
	// 			go rf.heartbeat(rf.currentTerm)
	// 			// }
	// 		}
	// 		// Final:
	// 		rf.mu.Unlock()
	// 	}

	// if reply.VoteGranted {
	// 	// count majority of the servers in cluster
	// 	majority++
	// 	if majority >= len(rf.peers)/2+1 {
	// 		rf.currentRole = Leader
	// 		if !isCallHearBeat {
	// 			go rf.heartbeat()
	// 			isCallHearBeat = true
	// 		}
	// 	}
	// } else {
	// 	rf.mu.Lock()
	// 	if rf.currentTerm < reply.CurrentTerm {
	// 		rf.currentTerm = reply.CurrentTerm
	// 		rf.currentRole = Follower
	// 	}
	// 	rf.mu.Unlock()
	// }
	// }

	// reply := new(RequestVoteReply)
	// for reply != nil || reply.VoteGranted {
	// 	r := <-replyChannel
	// 	reply = &r
	// 	rf.mu.Lock()
	// 	if rf.currentTerm < r.CurrentTerm {
	// 		rf.currentTerm = r.CurrentTerm
	// 		rf.currentRole = Follower
	// 	}
	// 	rf.mu.Unlock()
	// }

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
func (rf *Raft) ticker() {
	shiftMin := (rf.me + 2) * 3
	// shiftMin := 0
	timeInterval := time.Duration(rand.Intn(300)+300+shiftMin) * time.Millisecond
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(timeInterval)
		// check election timeout if zero
		if !rf.killed() {
			rf.mu.Lock()
			afterElectionTimeout := time.Since(rf.electionTimeout) > timeInterval
			DPrintf("[%d] [timeout=%v] to check election %v", rf.me, afterElectionTimeout, time.Now())
			afterElectionTimeout = afterElectionTimeout && rf.currentRole != Leader
			rf.mu.Unlock()
			if afterElectionTimeout {
				// starts a new election
				rf.startElection()
			}
		}
	}
}

func (rf *Raft) heartbeat(term int) {
	timeInterval := time.Duration(115 * time.Millisecond)
	isLeader := true
	DPrintf("[%d] is leader within term %d", rf.me, term)
	for !rf.killed() && isLeader {
		isLeader = rf.sendHeartbeat()
		if isLeader {
			time.Sleep(timeInterval)
		}
	}
	DPrintf("[%d] KILL heart beat", rf.me)
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
	rf.votedFor = -1
	rf.currentRole = Follower
	rf.currentTerm = 0
	rf.lastApplied = 0
	rf.logOperation = make([]LogOperation, 0)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
