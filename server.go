package raft

import (
	"fmt"
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
	state State
	fsm   *StateMachineKV

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

	done       chan struct{}
	stopServer chan struct{}
}

type ClientResponse struct {
	Msg Response
	Ok  bool
}

func (s *Server) Start() {
	logInfo("Starting server")

	go s.rpc.start(s.GetAddress())
	s.Peers.DialPeers()
	go s.run()
}

func (s *Server) Stop() {
	logInfo("Stopping server")

	s.Peers.CloseConn()
	s.rpc.stop()
	s.done <- struct{}{}
}

func (s *Server) run() {
	for {
		select {
		case <-s.done:
			s.stopServer <- struct{}{}
			logInfo("Stopping process")
			return
		default:
		}

		switch s.GetState() {
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
	logRaft(StateChanged, s.GetDebugInfoLock())

	electionTicker := time.NewTicker(randomTimeout(DefaultElectionTimeout))
	defer electionTicker.Stop()

	for s.GetState() == Follower {
		s.mu.RLock()
		currentTerm := s.currentTerm
		votedFor := s.votedFor
		s.mu.RUnlock()

		select {
		case <-electionTicker.C:
			logRaft(ElectionTimeout, s.GetDebugInfoLock())

			s.mu.Lock()
			s.state = Candidate
			s.mu.Unlock()

			s.electNotifyCh <- struct{}{}
			return

		case <-s.applyNotifyCh:
			logRaft(Apply, s.GetDebugInfoLock())
			s.applyCommitted()

		case in := <-s.rpc.RequestVoteInCh:
			logRaft(ReceiveRequestVote, s.GetDebugInfoLock())

			out := proto.RequestVoteResponse{VoteGranted: false}
			// Reply false if term < currentTerm AND
			// If votedFor is null or candidateId, and candidate’s log
			// is at least as up-to-date as receiver’s log, grant vote.
			if currentTerm <= in.Term && votedFor == -1 && in.CandidateId != -1 {
				s.mu.Lock()

				s.currentTerm = in.Term
				if s.log.isLogUpToDateWith(in.LastLogIndex, in.LastLogTerm) {
					out.VoteGranted = true
					s.votedFor = in.CandidateId
					electionTicker.Reset(randomTimeout(DefaultElectionTimeout))
				}

				s.mu.Unlock()
				s.PersistToStorage()
			}

			out.Term = currentTerm
			s.rpc.RequestVoteOutCh <- &out

		case in := <-s.rpc.AppendEntriesInCh:
			logRaft(ReceiveAppendEntries, s.GetDebugInfoLock())

			var out proto.AppendEntriesResponse

			// Reply false if term < currentTerm.
			if in.Term < currentTerm {
				out.Term = currentTerm
				out.Success = false
				return
			}

			// Reply false if log does not contain an entry at prevLogIndex
			// whose term matches prevLogTerm. Also help the leader bring us
			// up to date quickly (By skipping the indexes in the same term).
			s.mu.Lock()
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
			electionTicker.Reset(randomTimeout(DefaultElectionTimeout))

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

			s.mu.Unlock()
			s.PersistToStorage()

			out.Term = currentTerm
			out.Success = true
			s.rpc.AppendEntriesOutCh <- &out

		case <-s.electNotifyCh:
			//  Ignore since we are not the candidate

		case <-s.stopServer:
			return
		}
	}
}

func (s *Server) runCandidate() {
	s.mu.Lock()
	s.votedFor = s.config.Id
	s.currentTerm++
	s.mu.Unlock()

	votesReceived := 1

	logRaft(StateChanged, s.GetDebugInfoLock())

	electionTicker := time.NewTicker(randomTimeout(DefaultElectionTimeout))

	for s.GetState() == Candidate {
		currentTerm := s.GetTerm()

		select {
		case <-electionTicker.C:
			logRaft(ElectionTimeout, s.GetDebugInfoLock())

			s.electNotifyCh <- struct{}{}

		case in := <-s.rpc.AppendEntriesInCh:
			logRaft(ReceiveAppendEntries, s.GetDebugInfoLock())

			if in.Term >= currentTerm {
				s.mu.Lock()
				s.convertToFollower(in.Term)
				s.mu.Unlock()
				// push to the follower
				s.rpc.AppendEntriesInCh <- in
				return
			}

			logRaft(RejectAppendEntries, s.GetDebugInfoLock())
			s.rpc.AppendEntriesOutCh <- s.rejectAppendEntriesMsg()

		case in := <-s.rpc.RequestVoteInCh:
			logRaft(ReceiveRequestVote, s.GetDebugInfoLock())

			if in.Term > currentTerm {
				s.mu.Lock()
				s.convertToFollower(in.Term)
				s.mu.Unlock()
				// push to the follower
				s.rpc.RequestVoteInCh <- in
				return
			}

			logRaft(RejectRequestVote, s.GetDebugInfoLock())
			s.rpc.RequestVoteOutCh <- s.rejectRequestVoteMsg()

		case <-s.electNotifyCh:
			logRaft(SendRequestVote, s.GetDebugInfoLock())

			var wg sync.WaitGroup
			for _, peer := range *s.Peers {
				wg.Add(1)
				go func(p Peer) {
					defer wg.Done()
					s.mu.RLock()
					lastLogIndex, lastLogTerm := s.log.getLastLogIndexTerm()

					in := proto.RequestVoteRequest{
						Term:         currentTerm,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
						CandidateId:  s.config.Id,
					}
					s.mu.RUnlock()

					if out, err := p.RequestVote(DefaultRequestVoteTimeout, &in); err == nil {
						s.mu.Lock()
						defer s.mu.Unlock()

						if out.Term > currentTerm {
							s.convertToFollower(out.Term)
							return
						}

						if out.VoteGranted {
							votesReceived++
							if s.isMajorityCount(votesReceived) {
								logRaft(ElectionWin, s.GetDebugInfoLockFree())
								s.state = Leader
								return
							}
						}
					}
				}(*peer)
			}
			wg.Wait()
			s.PersistToStorage()
			return

		case <-s.applyNotifyCh:
			logRaft(Apply, s.GetDebugInfoLock())
			s.applyCommitted()

		case <-s.stopServer:
			return
		}
	}
}

func (s *Server) runLeader() {
	logRaft(StateChanged, s.GetDebugInfoLock())

	heartbeatTicker := time.NewTicker(randomTimeout(DefaultHeartbeatTimeout))

	stopListenClient, stopSendHeartBeats := make(chan struct{}, 1), make(chan struct{}, 1)
	go s.listenClient(stopListenClient, heartbeatTicker)
	go s.sendHeartBeats(stopSendHeartBeats, heartbeatTicker)

	for s.GetState() == Leader {
		currentTerm := s.GetTerm()

		select {
		case isReplicated := <-s.applyNotifyCh:
			logRaft(Apply, s.GetDebugInfoLock())

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
			logRaft(ReceiveRequestVote, s.GetDebugInfoLock())

			if in.Term > currentTerm {
				s.mu.Lock()
				s.convertToFollower(in.Term)
				s.mu.Unlock()
				// push to the follower
				s.rpc.RequestVoteInCh <- in
				return
			}

			logRaft(RejectRequestVote, s.GetDebugInfoLock())
			s.rpc.RequestVoteOutCh <- s.rejectRequestVoteMsg()

		case in := <-s.rpc.AppendEntriesInCh:
			logRaft(ReceiveAppendEntries, s.GetDebugInfoLock())

			if in.Term == currentTerm {
				logError("2 leaders in the same term")
			}

			if in.Term > currentTerm {
				s.mu.Lock()
				s.convertToFollower(in.Term)
				s.mu.Unlock()
				// push to the follower
				s.rpc.AppendEntriesInCh <- in
				return
			}

			logRaft(RejectAppendEntries, s.GetDebugInfoLock())
			s.rpc.AppendEntriesOutCh <- s.rejectAppendEntriesMsg()

		case <-s.electNotifyCh:
			//  Ignore since we are not the candidate

		case <-s.stopServer:
			stopListenClient <- struct{}{}
			stopSendHeartBeats <- struct{}{}
			return
		}
	}
}

func (s *Server) listenClient(done chan struct{}, heartbeatTicker *time.Ticker) {
	for {
		select {
		case cmd := <-s.clientCmdIn:
			logRaft(ReceiveClientCmd, s.GetDebugInfoLock())

			heartbeatTicker.Reset(randomTimeout(DefaultHeartbeatTimeout))

			s.mu.RLock()
			currentTerm := s.currentTerm
			commitIndex := s.commitIndex
			s.mu.RUnlock()
			s.sendAppendEntries(false, cmd, currentTerm, commitIndex)

		case <-done:
			return
		}
	}
}

func (s *Server) sendHeartBeats(done chan struct{}, heartbeatTicker *time.Ticker) {
	for {
		select {
		case <-heartbeatTicker.C:
			logRaft(HeartbeatTimeout, s.GetDebugInfoLock())

			heartbeatTicker.Reset(randomTimeout(DefaultHeartbeatTimeout))

			s.mu.RLock()
			currentTerm := s.currentTerm
			commitIndex := s.commitIndex
			s.mu.RUnlock()
			s.sendAppendEntries(true, CommandKV{}, currentTerm, commitIndex)

		case <-done:
			return
		}
	}
}

type Response string

// Submit attempts to execute a command and replicate it.
func (s *Server) Submit(command CommandKV) ClientResponse {
	logRaft(SubmitReceived, s.GetDebugInfoLock())

	if s.isLeader() {
		s.clientCmdIn <- command
		return <-s.clientCmdOut
	}

	return ClientResponse{Msg: "", Ok: false}
}

func NewServer(id int32, db *map[string]string) *Server {
	s := &Server{
		currentTerm: -1,
		votedFor:    -1,
		log:         Log{},

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  make([]int32, len(Cluster)),
		matchIndex: make([]int32, len(Cluster)),

		state: Follower,
		fsm:   NewStateMachineKV(db),

		config:  NewConfig(id),
		rpc:     NewRPCserver(1),
		storage: NewStorage(id),
		Peers:   NewPeers(id),

		applyNotifyCh: make(chan bool, 1),
		electNotifyCh: make(chan struct{}, 1),
		clientCmdIn:   make(chan CommandKV, 1),
		clientCmdOut:  make(chan ClientResponse, 1),
		done:          make(chan struct{}, 1),
	}

	// if storage is empty => it is first load of the server => term = -1
	if state, err := s.storage.Load(); err != nil {
		s.convertToFollower(-1)
	} else {
		s.currentTerm = state.CurrentTerm
		s.votedFor = state.VotedFor
		s.log = state.Log
	}

	return s
}

func (s *Server) convertToFollower(newTerm int32) {
	s.currentTerm = newTerm
	s.votedFor = -1
	s.state = Follower
}

func (s *Server) applyCommitted() []Response {
	s.mu.Lock()
	defer s.mu.Unlock()

	var responses []Response // we can allocate [] of size commitIndex - lastApplied
	for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
		responses = append(responses, s.fsm.Apply(stringToCommandKV(s.log[i].CommandName)))
		s.lastApplied++
	}
	return responses
}

func (s *Server) rejectRequestVoteMsg() *proto.RequestVoteResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return &proto.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}
}

func (s *Server) rejectAppendEntriesMsg() *proto.AppendEntriesResponse {
	s.mu.RLock()
	defer s.mu.RUnlock()
	currLogIndex, currLogTerm := s.log.getLastLogIndexTerm()
	return &proto.AppendEntriesResponse{
		Term:          s.currentTerm,
		Success:       false,
		ConflictIndex: currLogIndex,
		ConflictTerm:  currLogTerm,
	}
}

func (s *Server) sendAppendEntries(isHeartBeat bool, cmd CommandKV, currentTerm int32, commitIndex int32) {
	logRaft(SendAppendEntries, s.GetDebugInfoLock())
	if !isHeartBeat {
		s.mu.Lock()
		s.log = append(s.log, &proto.LogEntry{
			Index:       s.log.size(),
			Term:        s.currentTerm,
			CommandName: commandKVToString(&cmd),
		})
		s.mu.Unlock()
	}

	s.PersistToStorage()

	isReplicated := false

	var wg sync.WaitGroup
	for _, peer := range *s.Peers {
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			// retry if we have to
			for s.sendAppendEntry(isHeartBeat, p, currentTerm, commitIndex, &isReplicated) {
			}
		}(*peer)
	}
	wg.Wait()

	if !isHeartBeat {
		s.applyNotifyCh <- isReplicated
	}
}

// sendAppendEntry returns true if we need to retry
func (s *Server) sendAppendEntry(
	isHeartBeat bool,
	p Peer,
	currentTerm int32,
	commitIndex int32,
	isReplicated *bool,
) bool {
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

	in := proto.AppendEntriesRequest{
		Term:         currentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderId:     s.config.Id,
		Entries:      entries,
	}
	s.mu.RUnlock()

	if out, err := p.AppendEntries(DefaultAppendEntriesTimeout, &in); err == nil {
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
							logRaft(AdvanceCommitIndex, s.GetDebugInfoLockFree())
							s.commitIndex = N
						}
					}
				}

				if s.commitIndex != commitIndex {
					*isReplicated = true
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
	return false
}

func (s *Server) isMajorityCount(count int) bool {
	return 2*count > len(*s.Peers)+1
}

func (s *Server) PersistToStorage() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	err := s.storage.Save(NewPersistentState(s.currentTerm, s.votedFor, s.log))
	if err != nil {
		logError(err.Error())
	}
}

func (s *Server) LoadFromStorage() {
	s.mu.RLock()
	defer s.mu.RUnlock()

	state, err := s.storage.Load()
	if err != nil {
		logError(err.Error())
	} else {
		s.currentTerm = state.CurrentTerm
		s.votedFor = state.VotedFor
		s.log = state.Log
	}
}

func (s *Server) isLeader() bool {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state == Leader
}

func (s *Server) GetState() State {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.state
}

func (s *Server) GetTerm() int32 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentTerm
}

func (s *Server) GetAddress() string {
	return s.config.Address
}

func (s *Server) GetDebugInfoLock() string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return fmt.Sprintf("Id: %d, State: %s, Term: %v, CommitedIndex: %v ", s.config.Id, stateToStr(s.state), s.currentTerm, s.commitIndex)
}

func (s *Server) GetDebugInfoLockFree() string {
	return fmt.Sprintf("Id: %d, State: %s, Term: %v, CommitedIndex: %v ", s.config.Id, stateToStr(s.state), s.currentTerm, s.commitIndex)
}
