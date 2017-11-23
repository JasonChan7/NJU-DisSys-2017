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

import "sync"
import "labrpc"
import "time"
import "fmt"
import "math/rand"
// import "sync/atomic"

// import "bytes"
// import "encoding/gob"

type ServerState string
const (
	FOLLOWER  ServerState = "Follower"
	CANDIDATE ServerState = "Candidate"
	LEADER    ServerState = "Leader"
	HEARTBEAT_INTERVAL = 100
)
type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}
//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	votedFor		int
	votedGot		int
	currentTerm		int
	state			ServerState

	electionTimer *time.Timer
	voteChan	chan struct{} // 成功投票的信号
	appendChan	chan struct{} // 成功更新 log 的信号

	log		[]LogEntry
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// var term int
	// var isleader bool
	// Your code here.
	return rf.currentTerm, rf.state == LEADER
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}


type AppendEntriesArgs struct {
	Term 			int
	LeaderId		int
	PrevLogIndex	int
	PrevLogTerm		int
	// Entries			[]LogEntry
	// LeaderCommit	int
}

type AppendEntriesReply struct {
	Term		int
	Success		bool
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int
	VoteGranted		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		// fmt.Println("ae2U")
		rf.UpdateTo(FOLLOWER)
		// fmt.Println("ae2D")
		reply.Success = true
	} else {
		// fmt.Println("ae3U")
		rf.UpdateTo(FOLLOWER)
		// fmt.Println("ae3D")
		reply.Success = true
	}
	// rf.appendChan <- struct{}{}
	if reply.Success {
		go func() { rf.appendChan <- struct{}{} }()
	}
	// rf.mu.Unlock()
}
//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Printf("server %d receive RequestVote from server %d\n", rf.me, args.CandidateId)
	fmt.Printf("rf Term %d, args Term %d\n", rf.currentTerm, args.Term)
	if args.Term < rf.currentTerm {
		// fmt.Println("1")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	} else if args.Term > rf.currentTerm {
		// fmt.Println("rv2")
		rf.UpdateTo(FOLLOWER)
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.votedFor == -1 {
		// fmt.Println("rv3")
		rf.UpdateTo(FOLLOWER)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		// fmt.Println("4")
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}
	// fmt.Printf("rf Term %d, args Term %d\n", rf.currentTerm, args.Term)
	if reply.VoteGranted == true {
		fmt.Println("Granted")
		// rf.voteChan <- struct{}{}
		go func() { rf.voteChan <- struct{}{} }()
	}
	// rf.mu.Unlock()
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) SendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true


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

	// Your initialization code here.
	rf.state = FOLLOWER
	rf.votedFor = -1 // default for no server, server id start from 0
	rf.voteChan = make(chan struct{})
	rf.appendChan = make(chan struct{})
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Loop()
	return rf
}

func GetElectionTimeout() time.Duration {
	return (200 + time.Duration(rand.Intn(300))) * time.Millisecond
}

func (rf *Raft) Loop() {
	rf.electionTimer = time.NewTimer(GetElectionTimeout())
	for {
		currentState := rf.state
		switch currentState {
		case FOLLOWER:
			select {
			// 成功投票
			case <-rf.voteChan:
				fmt.Printf("server %d recevied voteChan\n", rf.me)
				rf.electionTimer.Reset(GetElectionTimeout())
			// 收到 log 更新的消息
			case <-rf.appendChan:
				fmt.Printf("server %d recevied appendChan\n", rf.me)
				rf.electionTimer.Reset(GetElectionTimeout())
			// 超时消息
			case <-rf.electionTimer.C:
				rf.mu.Lock()
				rf.UpdateTo(CANDIDATE)
				rf.mu.Unlock()
				rf.electionTimer.Reset(GetElectionTimeout())
				rf.StartElection()
			}
		case CANDIDATE:
			// rf.mu.Lock()
			select {
			// discovers current leader or new term
			case <-rf.appendChan:
				rf.UpdateTo(FOLLOWER)
			// times out, new election
			case <-rf.electionTimer.C:
				// rf.electionTimer.Reset(randElectionDuration())
				fmt.Printf("Warning: election timeout, restart\n")
				rf.StartElection()
			// check for wheather receives votes from majority
			default:
				if rf.votedGot > len(rf.peers)/2 {
					rf.UpdateTo(LEADER)
				}
			}
			// rf.mu.Unlock()
		case LEADER:
			select {
			// dicovers server with higher term
			case <-rf.appendChan:
				fmt.Println("???")
				rf.UpdateTo(FOLLOWER)
			default:
				fmt.Printf("current server %d send heart-beats\n", rf.me)
				rf.StartAppend()
				time.Sleep(HEARTBEAT_INTERVAL*time.Millisecond)
			}
		}
	}
}

func (rf *Raft) UpdateTo(state ServerState) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	currState := rf.state
	// fmt.Printf("rf is in state %s, will update to state %s\n", currState, state)
	if currState == state {
		// fmt.Printf("Warning: server %d current state is already %s, neednt trans to state %s\n", rf.me, currState, state)
		return
	}
	switch state {
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.votedFor = -1
		rf.votedGot = 0
	case CANDIDATE:
		rf.state = CANDIDATE
	case LEADER:
		rf.state = LEADER
	}
	fmt.Printf("In term %d: Server %d update from %s to %s\n", rf.currentTerm, rf.me, currState, state)
	// rf.mu.Unlock()
}

// only candidate state server call this func
func (rf *Raft) StartElection() {
	if rf.state != CANDIDATE {
		fmt.Printf("Warning: current state is %s not candidate, cannot start election\n", rf.state)
		return
	}
	fmt.Printf("server %d start election\n", rf.me)
	rf.currentTerm += 1
	rf.votedFor = rf.me
	rf.votedGot = 1
	rf.electionTimer.Reset(GetElectionTimeout())
	// send RequestVote RPC to all other servers
	lastIndex, lastTerm := rf.GetLastLogInfo()
	reqVoteArgs := RequestVoteArgs {
		Term:			rf.currentTerm,
		CandidateId:	rf.me,
		LastLogIndex:	lastIndex,
		LastLogTerm:	lastTerm,
	}
	// fmt.Printf("current server number is %d\n", len(rf.peers))
	// replies := make([]RequestVoteReply, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				if rf.state == CANDIDATE && rf.sendRequestVote(server, &reqVoteArgs, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.VoteGranted == true {
						rf.votedGot += 1
					} else {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.UpdateTo(FOLLOWER)
						}
					}
				} else {
					fmt.Printf("Server %d send vote req failed.\n", rf.me)
				}
			}(i)
			// if rf.state == CANDIDATE && rf.sendRequestVote(i, &reqVoteArgs, &replies[i]) {
			// 	rf.mu.Lock()
			// 	defer rf.mu.Unlock()
			// 	if replies[i].VoteGranted {
			// 		rf.votedGot += 1
			// 	} else if replies[i].Term > rf.currentTerm {
			// 		rf.currentTerm = replies[i].Term
			// 		rf.UpdateTo(FOLLOWER)
			// 	}
			// 	// rf.mu.Unlock()
			// } else {
			// 	fmt.Printf("server %d now is in state %s, send RequestVote failed\n", rf.me, rf.state)
			// }
		}
	}
}

func (rf *Raft) GetLastLogInfo() (int, int) {
	if len(rf.log) > 0 {
		entry := rf.log[len(rf.log)-1]
		return entry.Index, entry.Term
	}
	// fmt.Printf("Warning: server %d doesnt have any log\n", rf.me)
	return 0, 0
}

func (rf *Raft) StartAppend() {
	lastIndex, lastTerm := rf.GetLastLogInfo()
	args := AppendEntriesArgs {
		Term:			rf.currentTerm,
		LeaderId:		rf.me,
		PrevLogIndex:	lastIndex,
		PrevLogTerm:	lastTerm,
	}
	// replies := make([]AppendEntriesReply, len(rf.peers))
	for i := range rf.peers {
		if i != rf.me && rf.state == LEADER {
			// if rf.SendAppendEntries(i, &args, &replies[i]) {
			// 	rf.mu.Lock()
			// 	if replies[i].Success != true {
			// 		if replies[i].Term > rf.currentTerm {
			// 			rf.currentTerm = replies[i].Term
			// 			rf.UpdateTo(FOLLOWER)
			// 		}
			// 	}
			// 	rf.mu.Unlock()
			// }
			go func(server int) {
				var reply AppendEntriesReply
				if rf.state == LEADER && rf.SendAppendEntries(server, &args, &reply) {
					rf.mu.Lock()
					defer rf.mu.Unlock()
					if reply.Success != true {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.UpdateTo(FOLLOWER)
						}
					}
				}
			}(i)
		}
	}
}