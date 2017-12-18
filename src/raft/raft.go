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
import "sync/atomic"

// import "bytes"
// import "encoding/gob"

type ServerState string
const (
	// FOLLOWER  ServerState = "Follower"
	// CANDIDATE ServerState = "Candidate"
	// LEADER    ServerState = "Leader"
	HEARTBEAT_INTERVAL = 100
	FOLLOWER = iota
	CANDIDATE
	LEADER
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
	currentTerm		int32
	state			int32

	electionTimer *time.Timer
	voteChan	chan struct{} // 成功投票的信号
	appendChan	chan struct{} // 成功更新 log 的信号

	// log consensus
	log				[]LogEntry	
	commitIndex		int			// 大部分 server 达成一致的 log
	lastApplied		int			// 本 server 已经执行的 log 最后下标
	nextIndex		[]int		// 每个 server 上复制 LogEntry 的起点
	matchIndex		[]int		// 每个 server 上已经和 leader 一致的最高 index，理想情况下 matchIndex[i]=nextIndex[i]+1
	applyChan		chan ApplyMsg
}
// atomic operations
func (rf *Raft) GetTerm() int32 {
	return atomic.LoadInt32(&rf.currentTerm)
}

func (rf *Raft) IncrementTerm() {
	atomic.AddInt32(&rf.currentTerm, 1)
}

func (rf *Raft) IsState(state int32) bool {
	return atomic.LoadInt32(&rf.state) == state
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here.
	term = int(rf.GetTerm())
	isleader = rf.IsState(LEADER)
	return term, isleader
	// return rf.currentTerm, rf.state == LEADER
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
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
}


type AppendEntriesArgs struct {
	Term 			int32
	LeaderId		int
	PrevLogIndex	int			// leader 认为 follower 最后一个匹配的位置
	PrevLogTerm		int			// PrevLogIndex 位置的 LogEntry 的 Term
	Entries			[]LogEntry	// leader 的 log[PrevLogIndex+1:]
	LeaderCommit	int			// leader 的 commitIndex
}

type AppendEntriesReply struct {
	Term		int32
	Success		bool
	NextTrial	int
}

//
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term			int32
	CandidateId		int
	LastLogIndex	int
	LastLogTerm		int
}

//
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term			int32
	VoteGranted		bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	inform := func() {
		go func() {
			rf.appendChan <- struct{}{}
		}()
	}
	rf.mu.Lock()
	defer inform()
	defer rf.mu.Unlock()
	// 
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		// should return？？？
		return
	} else if args.Term > rf.currentTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		// fmt.Println("ae2U")
		rf.UpdateTo(FOLLOWER)
		// fmt.Println("ae2D")
	} else {
		// fmt.Println("ae3U")
		// rf.UpdateTo(FOLLOWER)
		// fmt.Println("ae3D")
		reply.Success = true
	}

	// log consensus
	// 当前 server 的最后一个日志的 index 小于 leader 认为它应处于的位置（不匹配）
	if args.PrevLogIndex > rf.GetLastLogIndex() {
		reply.Success = false
		reply.NextTrial = rf.GetLastLogIndex()+1
		return
	}
	// not in the same term
	if args.PrevLogTerm != rf.log[args.PrevLogIndex].Term {
		reply.Success = false
		// 获取应处于的时间片id
		badTerm := rf.log[args.PrevLogIndex].Term
		i := args.PrevLogIndex
		for ; rf.log[i].Term == badTerm; i-- {
			// 找到最后一个处于应处于的时间片的 LogEntry 的 index
		}
		reply.NextTrial = i+1
		return
	}
	conflictIdx := -1
	if rf.GetLastLogIndex() < args.PrevLogIndex+len(args.Entries) {
		conflictIdx = args.PrevLogIndex+1
	} else {
		for idx:=0; idx<len(args.Entries); idx++ {
			// 依次检查每个位置上的 Term 是否一致
			if rf.log[idx+args.PrevLogIndex+1].Term != args.Entries[idx].Term {
				conflictIdx = idx+args.PrevLogIndex+1
				break
			}
		}
	}
	if conflictIdx != -1 {
		// ...是什么？？？
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)
	}

	// commit
	if args.LeaderCommit > rf.commitIndex {
		// rf.commitIndex = max(rf.GetLastLogIndex(), args.LeaderCommit)
		if args.LeaderCommit < rf.GetLastLogIndex() {
			rf.commitIndex = rf.GetLastLogIndex()
		} else {
			rf.commitIndex = args.LeaderCommit
		}
	}

	// rf.appendChan <- struct{}{}
	// if reply.Success {
	// 	go func() { rf.appendChan <- struct{}{} }()
	// }
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
		rf.currentTerm = args.Term
		rf.UpdateTo(FOLLOWER)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else if rf.votedFor == -1 {
		// fmt.Println("rv3")
		// rf.UpdateTo(FOLLOWER)
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
	} else {
		// fmt.Println("4")
		reply.VoteGranted = false
		// reply.Term = rf.currentTerm
	}
	// fmt.Printf("rf Term %d, args Term %d\n", rf.currentTerm, args.Term)
	// log consensus
	rfLastLogTerm := rf.log[rf.GetLastLogIndex()].Term
	if rfLastLogTerm > args.LastLogTerm {
		// 自己处于更新的时间片
		reply.VoteGranted = false
	} else if rfLastLogTerm == args.LastLogTerm {
		// 自己的日志的 index 更新
		if rf.GetLastLogIndex() > args.LastLogIndex {
			reply.VoteGranted = false
		}
	}

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

	term, isLeader = rf.GetState()
	if isLeader == true {
		rf.mu.Lock()
		index = len(rf.log)
		rf.log = append(rf.log, LogEntry{term, index, command})
		rf.mu.Unlock()
	}

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

	// log consensus
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.applyChan = applyCh
	rf.log = make([]LogEntry, 1)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	go rf.Loop()
	return rf
}

func GetElectionTimeout() time.Duration {
	return (400 + time.Duration(rand.Intn(100))) * time.Millisecond
}

func (rf *Raft) Loop() {
	rf.electionTimer = time.NewTimer(GetElectionTimeout())
	for {
		// atomic
		// currentState := rf.state
		switch atomic.LoadInt32(&rf.state) {
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
				// rf.electionTimer.Reset(GetElectionTimeout())
				// rf.StartElection()
			}
		case CANDIDATE:
			rf.mu.Lock()
			select {
			// discovers current leader or new term
			case <-rf.appendChan:
				rf.UpdateTo(FOLLOWER)
			// times out, new election
			case <-rf.electionTimer.C:
				rf.electionTimer.Reset(GetElectionTimeout())
				fmt.Printf("Warning: election timeout, restart\n")
				rf.StartElection()
			// check for wheather receives votes from majority
			default:
				if rf.votedGot > len(rf.peers)/2 {
					rf.UpdateTo(LEADER)
				}
			}
			rf.mu.Unlock()
		case LEADER:
			// select {
			// // dicovers server with higher term
			// case <-rf.appendChan:
			// 	fmt.Println("???")
			// 	rf.UpdateTo(FOLLOWER)
			// default:
			fmt.Printf("current server %d send heart-beats\n", rf.me)
			rf.StartAppend()
			rf.UpdateCommitIndex()
			time.Sleep(HEARTBEAT_INTERVAL*time.Millisecond)
			// }
		}
		go rf.ApplyLog()
	}
}

func (rf *Raft) UpdateTo(state int32) {
	// rf.mu.Lock()
	// defer rf.mu.Unlock()
	// currState := rf.state
	// fmt.Printf("rf is in state %s, will update to state %s\n", currState, state)
	if rf.IsState(state) {
		// fmt.Printf("Warning: server %d current state is already %s, neednt trans to state %s\n", rf.me, currState, state)
		return
	}
	stateDesc := []string{"FOLLOWER", "CANDIDATE", "LEADER"}
	preState := rf.state
	switch state {
	case FOLLOWER:
		rf.state = FOLLOWER
		rf.votedFor = -1
		// rf.votedGot = 0
	case CANDIDATE:
		rf.state = CANDIDATE
		rf.StartElection()
	case LEADER:
		// log consensus
		for i, _ := range rf.peers {
			rf.nextIndex[i] = rf.GetLastLogIndex()+1
			rf.matchIndex[i] = 0
		}
		rf.state = LEADER
	default:
		fmt.Printf("Warning: invalid state %d, do nothing.\n", state)
	}
	fmt.Printf("In term %d: Server %d transfer from %s to %s\n",
		rf.currentTerm, rf.me, stateDesc[preState], stateDesc[rf.state])
	// rf.mu.Unlock()
}

// only candidate state server call this func
func (rf *Raft) StartElection() {
	// if rf.state != CANDIDATE {
	// 	fmt.Printf("Warning: current state is %s not candidate, cannot start election\n", rf.state)
	// 	return
	// }
	// fmt.Printf("server %d start election\n", rf.me)
	// rf.currentTerm += 1
	// rf.votedFor = rf.me
	// rf.votedGot = 1
	// rf.electionTimer.Reset(GetElectionTimeout())
	// // send RequestVote RPC to all other servers
	// lastIndex := rf.GetLastLogIndex()
	// lastTerm := rf.log[lastIndex].Term
	// reqVoteArgs := RequestVoteArgs {
	// 	Term:			rf.currentTerm,
	// 	CandidateId:	rf.me,
	// 	LastLogIndex:	lastIndex,
	// 	LastLogTerm:	lastTerm,
	// }
	// fmt.Printf("current server number is %d\n", len(rf.peers))
	// replies := make([]RequestVoteReply, len(rf.peers))
	rf.IncrementTerm()
	rf.votedFor = rf.me
	rf.votedGot = 1
	rf.electionTimer.Reset(GetElectionTimeout())
	var args RequestVoteArgs
	args.Term = rf.currentTerm
	args.CandidateId = rf.me
	args.LastLogIndex = rf.GetLastLogIndex()
	args.LastLogTerm = rf.log[args.LastLogIndex].Term
	for i, _ := range rf.peers {
		if i != rf.me {
			go func(server int) {
				var reply RequestVoteReply
				if rf.IsState(CANDIDATE) && rf.sendRequestVote(server, &args, &reply) {
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

func (rf *Raft) GetLastLogIndex() int {
	return len(rf.log)-1
}
 
func (rf *Raft) StartAppend() {
	sendAppendEntriesTo := func(server int) bool {
		// 该函数返回 true 代表需要重试，否则返回false
		var args AppendEntriesArgs
		rf.mu.Lock()
		args.Term = rf.currentTerm
		args.LeaderId = rf.me
		args.LeaderCommit = rf.commitIndex
		fmt.Printf("append server is %d\n", server)
		args.PrevLogIndex = rf.nextIndex[server]-1
		//!!! args.PrevLogIndex may be -1
		fmt.Printf("rf.nextIndex[server] is %d\n", rf.nextIndex[server])
		args.PrevLogTerm = rf.log[args.PrevLogIndex].Term
		if rf.GetLastLogIndex() >= rf.nextIndex[server] {
			args.Entries = rf.log[rf.nextIndex[server]:]
		}
		rf.mu.Unlock()

		var reply AppendEntriesReply

		// 发送是并行的
		if rf.IsState(LEADER) && rf.SendAppendEntries(server, &args, &reply) {
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Success == true {
				rf.nextIndex[server] += len(args.Entries)
				rf.matchIndex[server] = rf.nextIndex[server] - 1
			} else {
				if rf.state != LEADER {
					return false
				}
				if reply.Term > rf.currentTerm {
					// term 不匹配
					rf.currentTerm = reply.Term
					rf.UpdateTo(FOLLOWER)
				} else {
					// log 不匹配
					rf.nextIndex[server] = reply.NextTrial
					return true
				}
			}
		}
		return false
	}
	for i, _ := range rf.peers {
		if i == rf.me {
			continue
		}
		go func(server int) {
			for {
				if sendAppendEntriesTo(server) == false {
					break
				}
			}
		}(i)
	}
}
// log consensus
func (rf *Raft) UpdateCommitIndex() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	for i := rf.GetLastLogIndex(); i > rf.commitIndex; i-- {
		matchedCount := 1
		for j, matched := range rf.matchIndex {
			if j == rf.me {
				continue
			}
			if matched > rf.commitIndex {
				matchedCount += 1
			}
		}
		if matchedCount > len(rf.peers)/2 {
			rf.commitIndex = i
			break
		}
	}
}
func (rf *Raft) ApplyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.commitIndex > rf.lastApplied {
		for i := rf.lastApplied+1; i <= rf.commitIndex; i++ {
			var msg ApplyMsg
			msg.Index = i
			msg.Command = rf.log[i].Command
			rf.applyChan <- msg
		}
	}
}