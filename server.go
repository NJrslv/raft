package raft

import (
	"log"
	"raft/proto"
	"sync"
	"time"
)

type State int

const (
	Leader State = iota
	Follower
	Candidate
)

type Server struct {
	// Persistent state on all servers
	currentTerm int32
	votedFor    int32
	log         Log

	// Volatile state on all servers
	commitIndex int32
	lastApplied int32

	// Volatile state on leaders
	nextIndex  []int32
	matchIndex []int32

	// Internal
	currentLeader int32
	state         State
	fsm           *StateMachineKV

	config  *ServerConfig
	mu      sync.RWMutex
	rpc     *RPCserver
	storage *Storage
	Peers   *Peers

	// applyNotifyCh is used to notify when there are commands available to apply.
	// true if command was replicated
	applyNotifyCh chan bool // for all servers
	// electNotifyCh is used to notify the start of an election process.
	electNotifyCh chan struct{} // for candidate
	// clientCmdIn is used to send commands from client to leader.
	clientCmdIn chan CommandKV // for leader
	// clientCmdOut is used to send command from leader to client.
	clientCmdOut chan ClientResponse // for leader
	// appendEntriesNotifyCh is used to notify leader when to send AppendEntries rpc.
	appendEntriesNotifyCh chan struct{} // for leader

	done       chan struct{}
	stopServer chan struct{}
}

type ClientResponse struct {
	Msg Response
	Ok  bool
}

func (s *Server) Start() {
	logInfo("Starting server")
	s.convertToFollower(-1)
	go s.rpc.start(s.GetAddress())
	s.Peers.DialPeers()
	go s.run()
}

func (s *Server) Stop() {
	logInfo("Stopping server")
	s.rpc.stop()
	s.Peers.CloseConn()
	s.done <- struct{}{}
}

func (s *Server) run() {
	s.PersistToStorage()
	for {
		select {
		case <-s.done:
			s.stopServer <- struct{}{}
			logInfo("Stopping process")
			return
		default:
		}

		switch s.state {
		case Follower:
			s.runFollower()
		case Candidate:
			s.runCandidate()
		case Leader:
			s.runLeader()
		}
	}
}

func (s *Server) runFollower() {
	s.LoadFromStorage()
	logRaft(Follower, StateChanged, s.config.Id, s.currentTerm)

	electionTimer := time.NewTimer(randElectionTimeout())
	defer electionTimer.Stop()

	for s.state == Follower {
		select {
		case <-electionTimer.C:
			logRaft(Follower, ElectionTimeout, s.config.Id, s.currentTerm)
			s.state = Candidate
			s.electNotifyCh <- struct{}{}
		case <-s.applyNotifyCh:
			logRaft(Follower, Apply, s.config.Id, s.currentTerm)
			s.applyCommitted()
		case in := <-s.rpc.RequestVoteInCh:
			logRaft(Follower, ReceiveRequestVote, s.config.Id, s.currentTerm)
			out := proto.RequestVoteResponse{VoteGranted: false}
			// Reply false if term < currentTerm AND
			// If votedFor is null or candidateId, and candidate’s log
			// is at least as up-to-date as receiver’s log, grant vote.
			if s.currentTerm <= in.Term && s.votedFor == -1 && in.CandidateId != -1 {
				s.currentTerm = in.Term
				if s.log.isLogUpToDateWith(in.LastLogIndex, in.LastLogTerm) {
					out.VoteGranted = true
					s.votedFor = in.CandidateId
					electionTimer.Reset(randElectionTimeout())
				}
				s.PersistToStorage()
			}
			out.Term = s.currentTerm
			s.rpc.RequestVoteOutCh <- &out
		case in := <-s.rpc.AppendEntriesInCh:
			logRaft(Follower, ReceiveAppendEntries, s.config.Id, s.currentTerm)
			var out proto.AppendEntriesResponse
			// Reply false if term < currentTerm.
			if in.Term < s.currentTerm {
				out.Term = s.currentTerm
				out.Success = false
				return
			}
			// Reply false if log does not contain an entry at prevLogIndex
			// whose term matches prevLogTerm. Also help the leader bring us
			// up to date quickly (By skipping the indexes in the same term).
			if !s.log.isPrevLogTermTheSame(in.PrevLogIndex, in.PrevLogTerm) {
				if in.PrevLogIndex >= s.log.size() {
					out.ConflictIndex = s.log.size()
					out.ConflictTerm = -1
				} else {
					out.ConflictTerm = s.log[in.PrevLogIndex].Term

					var i int32
					for i = in.PrevLogIndex - 1; i >= 0; i-- {
						if s.log[i].Term != out.ConflictTerm {
							break
						}
					}
					out.ConflictIndex = i + 1
				}
				out.Term = s.currentTerm
				out.Success = false
				s.rpc.AppendEntriesOutCh <- &out
				return
			}

			s.currentTerm = in.Term
			s.votedFor = -1
			electionTimer.Reset(randElectionTimeout())

			// If an existing entry conflicts with a new one (same index but
			// different terms), delete the existing entry and all that follow it.
			// Append any new entries not already in the log.
			logPosInsertFrom := in.PrevLogIndex + 1
			appendEntriesPos := 0
			for logPosInsertFrom < s.log.size() && appendEntriesPos < len(in.Entries) {
				if s.log[logPosInsertFrom].Term != in.Entries[appendEntriesPos].Term {
					break
				}
				logPosInsertFrom++
				appendEntriesPos++
			}
			// if appendEntriesPos == len(in.Entries) then our log is up-to-date.
			if appendEntriesPos < len(in.Entries) {
				s.log = s.log[:logPosInsertFrom]                        // delete
				s.log = append(s.log, in.Entries[appendEntriesPos:]...) // insert
			}
			// fsm.apply(command)
			if in.CommitIndex > s.commitIndex {
				s.commitIndex = min(in.CommitIndex, s.log.size())
				s.applyNotifyCh <- false // false ~ cmd is not replicated (we don't know)
			}

			s.PersistToStorage()

			out.Term = s.currentTerm
			out.Success = true
			s.rpc.AppendEntriesOutCh <- &out
		case <-s.stopServer:
			return
		}
	}
}

func debug(line int) {
	log.Printf("\n----------- HERE ----------- %d\n", line)
}

func (s *Server) runCandidate() {
	s.LoadFromStorage()
	logRaft(Candidate, StateChanged, s.config.Id, s.currentTerm)

	s.currentTerm++
	s.votedFor = s.config.Id
	votesReceived := 1

	electionTimer := time.NewTimer(randElectionTimeout())
	defer electionTimer.Stop()

	for s.state == Candidate {
		select {
		case <-electionTimer.C:
			logRaft(Candidate, ElectionTimeout, s.config.Id, s.currentTerm)
			// Start new election
			electionTimer.Reset(randElectionTimeout())
			s.electNotifyCh <- struct{}{}
		case in := <-s.rpc.AppendEntriesInCh:
			logRaft(Candidate, ReceiveAppendEntries, s.config.Id, s.currentTerm)
			if in.Term >= s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.AppendEntriesInCh <- in
			} else {
				s.rpc.AppendEntriesOutCh <- s.rejectAppendEntriesMsg()
			}
		case in := <-s.rpc.RequestVoteInCh:
			logRaft(Candidate, ReceiveRequestVote, s.config.Id, s.currentTerm)
			if in.Term >= s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.RequestVoteInCh <- in
			} else {
				s.rpc.RequestVoteOutCh <- s.rejectRequestVoteMsg()
			}
		case <-s.electNotifyCh:
			logRaft(Candidate, ElectionStart, s.config.Id, s.currentTerm)
			logRaft(Candidate, SendRequestVote, s.config.Id, s.currentTerm)
			var wg sync.WaitGroup
			for _, peer := range *s.Peers {
				wg.Add(1)
				go func(p Peer) {
					defer wg.Done()
					s.mu.RLock()
					lastLogIndex, lastLogTerm := s.log.getLastLogIndexTerm()
					currentTerm := s.currentTerm
					s.mu.RUnlock()

					in := proto.RequestVoteRequest{
						Term:         currentTerm,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
						CandidateId:  s.config.Id,
					}

					if out, err := p.RequestVote(randReqVoteTimeout(), &in); err == nil {
						s.mu.Lock()
						defer s.mu.Unlock()

						if out.Term > currentTerm {
							s.convertToFollower(out.Term)
							return
						}

						if out.VoteGranted {
							votesReceived++
							if s.isMajorityCount(votesReceived) {
								logRaft(Candidate, ElectionWin, s.config.Id, s.currentTerm)
								s.state = Leader
								return
							}
						}
					}
				}(*peer)
			}
			wg.Wait()
			logRaft(Candidate, ElectionStop, s.config.Id, s.currentTerm)
			s.PersistToStorage()
		case <-s.applyNotifyCh:
			logRaft(Candidate, Apply, s.config.Id, s.currentTerm)
			s.applyCommitted()
		case <-s.stopServer:
			return
		}
	}
}

func (s *Server) runLeader() {
	s.LoadFromStorage()
	logRaft(Leader, StateChanged, s.config.Id, s.currentTerm)

	heartbeatTimer := time.NewTimer(randHeartbeatTimeout())
	defer heartbeatTimer.Stop()

	for s.state == Leader {
		select {
		case cmd := <-s.clientCmdIn:
			logRaft(Leader, ReceiveClientCmd, s.config.Id, s.currentTerm)
			heartbeatTimer.Reset(randHeartbeatTimeout())
			s.sendAppendEntries(false, cmd, s.currentTerm, s.commitIndex)
		case <-heartbeatTimer.C:
			logRaft(Leader, HeartbeatTimeout, s.config.Id, s.currentTerm)
			heartbeatTimer.Reset(randHeartbeatTimeout())
			s.sendAppendEntries(true, CommandKV{}, s.currentTerm, s.commitIndex)
		case isReplicated := <-s.applyNotifyCh:
			logRaft(Leader, Apply, s.config.Id, s.currentTerm)
			if isReplicated {
				applyResults := s.applyCommitted()
				for _, applyResult := range applyResults {
					logInfo("Successfully responded to client")
					s.clientCmdOut <- ClientResponse{
						Msg: applyResult,
						Ok:  true,
					}
				}
			} else {
				logInfo("Failed to respond to client")
				s.clientCmdOut <- ClientResponse{Msg: "", Ok: false}
			}
		case in := <-s.rpc.RequestVoteInCh:
			logRaft(Leader, ReceiveRequestVote, s.config.Id, s.currentTerm)
			if in.Term > s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.RequestVoteInCh <- in
			} else {
				logRaft(Leader, RejectRequestVote, s.config.Id, s.currentTerm)
				s.rpc.RequestVoteOutCh <- s.rejectRequestVoteMsg()
			}
		case in := <-s.rpc.AppendEntriesInCh:
			logRaft(Leader, ReceiveAppendEntries, s.config.Id, s.currentTerm)
			if in.Term == s.currentTerm {
				logError("2 leaders in the same term")
			}

			if in.Term > s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.AppendEntriesInCh <- in
			} else {
				logRaft(Leader, RejectAppendEntries, s.config.Id, s.currentTerm)
				s.rpc.AppendEntriesOutCh <- s.rejectAppendEntriesMsg()
			}
		case <-s.stopServer:
			return
		}
	}
}

type Response string

// Submit attempts to execute a command and replicate it.
func (s *Server) Submit(command CommandKV) ClientResponse {
	log.Printf("SERVER: Submit received by %v", s.config.Id)
	if s.state == Leader {
		s.clientCmdIn <- command
		return <-s.clientCmdOut
	}
	return ClientResponse{Msg: "", Ok: false}
}

func NewServer(id int32, db *map[string]string) *Server {
	return &Server{
		currentTerm: -1,
		votedFor:    -1,
		log:         Log{},

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  make([]int32, len(Cluster)),
		matchIndex: make([]int32, len(Cluster)),

		currentLeader: -1,
		state:         Follower,
		fsm:           NewStateMachineKV(db),

		config:  NewConfig(id),
		rpc:     NewRPCserver(1),
		storage: NewStorage(id),
		Peers:   NewPeers(id),

		applyNotifyCh:         make(chan bool, 1),
		electNotifyCh:         make(chan struct{}, 1),
		clientCmdIn:           make(chan CommandKV, 1),
		clientCmdOut:          make(chan ClientResponse, 1),
		appendEntriesNotifyCh: make(chan struct{}, 1),
		done:                  make(chan struct{}, 1),
	}
}

func (s *Server) convertToFollower(newTerm int32) {
	s.currentTerm = newTerm
	s.votedFor = -1
	s.state = Follower
	s.PersistToStorage()
}

func (s *Server) applyCommitted() []Response {
	var responses []Response // we can allocate [] of size commitIndex - lastApplied
	for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		responses = append(responses, s.fsm.Apply(stringToCommandKV(s.log[i].CommandName)))
		s.lastApplied++
	}
	return responses
}

func (s *Server) rejectRequestVoteMsg() *proto.RequestVoteResponse {
	return &proto.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}
}

func (s *Server) rejectAppendEntriesMsg() *proto.AppendEntriesResponse {
	currLogIndex, currLogTerm := s.log.getLastLogIndexTerm()
	return &proto.AppendEntriesResponse{
		Term:          s.currentTerm,
		Success:       false,
		ConflictIndex: currLogIndex,
		ConflictTerm:  currLogTerm,
	}
}

func (s *Server) sendAppendEntries(isHeartBeat bool, cmd CommandKV, currentTerm int32, commitIndex int32) {
	logRaft(Leader, SendAppendEntries, s.config.Id, s.currentTerm)
	if !isHeartBeat {
		s.log = append(s.log, &proto.LogEntry{
			Index:       s.log.size(),
			Term:        s.currentTerm,
			CommandName: commandKVToString(&cmd),
		})
		s.PersistToStorage()
	}

	var wg sync.WaitGroup
	for _, peer := range *s.Peers {
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			// retry if we have to
			for s.sendAppendEntry(isHeartBeat, p, currentTerm, commitIndex) {
			}
		}(*peer)
	}
	wg.Wait()
}

// sendAppendEntry returns true if we need to retry
func (s *Server) sendAppendEntry(isHeartBeat bool, p Peer, currentTerm int32, commitIndex int32) bool {
	s.mu.RLock()
	nextIndex := s.nextIndex[p.id]
	prevLogIndex := nextIndex - 1
	prevLogTerm := int32(-1)
	if prevLogIndex >= 0 {
		prevLogTerm = s.log[prevLogIndex].Term
	}

	entries := Log{}
	if !isHeartBeat {
		entries = s.log[nextIndex:]
	}
	s.mu.RUnlock()

	in := proto.AppendEntriesRequest{
		Term:         currentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderId:     s.config.Id,
		Entries:      entries,
	}

	isReplicated := false
	if out, err := p.AppendEntries(randAppendEntriesTimeout(), &in); err == nil {
		s.mu.Lock()
		defer s.mu.Unlock()

		if out.Term > currentTerm {
			s.convertToFollower(out.Term)
			return false
		}

		if currentTerm == out.Term {
			if out.Success {
				// update nextIndex and matchIndex
				s.nextIndex[p.id] = nextIndex + entries.size()
				s.matchIndex[p.id] = s.nextIndex[p.id] - 1
				// If there exists an N such that N > commitIndex,
				// a majority of matchIndex[N] ≥ N,
				// and log[N].term == currentTerm:
				// set commitIndex = N
				for N := commitIndex + 1; N < s.log.size(); N++ {
					if s.log[N].Term == currentTerm {
						serverCount := 1
						for _, peer := range *s.Peers {
							if s.matchIndex[peer.id] >= N {
								serverCount++
							}
						}

						if s.isMajorityCount(serverCount) {
							logRaft(Leader, AdvanceCommitIndex, s.config.Id, s.currentTerm)
							s.commitIndex = N
						}
					}
				}

				if s.commitIndex != commitIndex {
					isReplicated = true
				}
			} else {
				s.nextIndex[p.id] = out.ConflictIndex
				if out.ConflictTerm != -1 {
					lastIndexOfTerm := -1
					for i := len(s.log) - 1; i >= 0; i-- {
						if s.log[i].Term == out.ConflictTerm {
							lastIndexOfTerm = i
							break
						}
					}

					if lastIndexOfTerm != -1 {
						s.nextIndex[p.id] = int32(lastIndexOfTerm + 1)
					} else {
						s.nextIndex[p.id] = out.ConflictIndex
					}
				}
				return true
			}
		}
	}
	if !isHeartBeat {
		s.applyNotifyCh <- isReplicated
	}
	return false
}

func (s *Server) isMajorityCount(count int) bool {
	return 2*count > len(*s.Peers)+1
}

func (s *Server) PersistToStorage() {
	err := s.storage.Save(NewPersistentState(s.currentTerm, s.votedFor, s.log))
	if err != nil {
		logError(err.Error())
	}
}

func (s *Server) LoadFromStorage() {
	state, err := s.storage.Load()
	if err != nil {
		logError(err.Error())
	} else {
		s.currentTerm = state.CurrentTerm
		s.votedFor = state.VotedFor
		s.log = state.Log
	}
}

func (s *Server) IsLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == Leader
}

func (s *Server) GetLeaderAddress() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return Cluster[s.currentLeader].Address
}

func (s *Server) GetAddress() string {
	return s.config.Address
}

func (s *Server) GetMyId() int32 {
	return s.config.Id
}
