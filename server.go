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
	peers   *Peers

	// applyNotifyCh is used to notify when there are commands available to apply.
	applyNotifyCh chan struct{} // for all servers
	// electNotifyCh is used to notify the start of an election process.
	electNotifyCh chan struct{} // for candidate
	// clientCmdIn is used to send commands from client to leader.
	clientCmdIn chan CommandKV // for leader
	// clientCmdOut is used to send command from leader to client.
	clientCmdOut chan Response // for leader
	// appendEntriesNotifyCh is used to notify leader when to send AppendEntries rpc.
	appendEntriesNotifyCh chan struct{} // for leader
}

func (s *Server) StartServer() {
	defer s.peers.CloseConn()
	s.convertToFollower(-1)
	go s.run()
	// TODO somehow wait here, create done chan
}

func (s *Server) run() {
	for {
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
	log.Printf("SERVER#%d: Follower state", s.config.ID)

	electionTimer := time.NewTimer(randElectionTimeout())
	defer electionTimer.Stop()

	for s.state == Follower {
		select {
		case <-electionTimer.C:
			log.Printf("Follower#%d election time out", s.config.ID)
			s.state = Candidate
			s.electNotifyCh <- struct{}{}
		case <-s.applyNotifyCh:
			log.Printf("Follower#%d apply()", s.config.ID)
			s.applyCommitted()
		case in := <-s.rpc.RequestVoteInCh:
			log.Printf("Follower#%d receive RequestVote", s.config.ID)
			out := proto.RequestVoteResponse{Term: s.currentTerm, VoteGranted: false}
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
			s.rpc.RequestVoteOutCh <- &out
		case in := <-s.rpc.AppendEntriesInCh:
			log.Printf("Follower#%d receive AppendEntries", s.config.ID)
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
				s.applyNotifyCh <- struct{}{}
			}

			s.PersistToStorage()

			out.Term = s.currentTerm
			out.Success = true
			s.rpc.AppendEntriesOutCh <- &out
		}
	}
}

func (s *Server) runCandidate() {
	log.Printf("SERVER#%d: Candidate state", s.config.ID)

	s.currentTerm++
	s.votedFor = s.config.ID
	votesReceived := 1

	electionTimer := time.NewTimer(randElectionTimeout())
	defer electionTimer.Stop()

	for s.state == Candidate {
		select {
		case <-electionTimer.C:
			log.Printf("Candidate#%d election time out", s.config.ID)
			// Start new election
			electionTimer.Reset(randElectionTimeout())
			s.electNotifyCh <- struct{}{}
		case in := <-s.rpc.AppendEntriesInCh:
			log.Printf("Candidate#%d receive AppendEntries", s.config.ID)
			if in.Term >= s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.AppendEntriesInCh <- in
			} else {
				s.rpc.AppendEntriesOutCh <- s.rejectAppendEntriesMsg()
			}
		case in := <-s.rpc.RequestVoteInCh:
			log.Printf("Candidate#%d receive RequestVote", s.config.ID)
			if in.Term > s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.RequestVoteInCh <- in
			} else {
				s.rpc.RequestVoteOutCh <- s.rejectRequestVoteMsg()
			}
		case <-s.electNotifyCh:
			log.Printf("Candidate#%d start election", s.config.ID)
			var wg sync.WaitGroup
			for _, peer := range *s.peers {
				wg.Add(1)
				go func(p Peer) {
					defer wg.Done()
					s.mu.RLock()
					lastLogIndex, lastLogTerm := s.log.getLastLogIndexTerm()
					s.mu.RUnlock()

					in := proto.RequestVoteRequest{
						Term:         s.currentTerm,
						LastLogIndex: lastLogIndex,
						LastLogTerm:  lastLogTerm,
						CandidateId:  s.config.ID,
					}
					if out, err := p.RequestVote(randReqVoteTimeout(), &in); err != nil {
						if out.Term > s.currentTerm {
							s.convertToFollower(out.Term)
							return
						}

						if out.VoteGranted {
							votesReceived++
							if s.isMajorityCount(votesReceived) {
								s.state = Leader
								return
							}
						}
					}
				}(peer)
			}
			wg.Wait()
			s.PersistToStorage()
		case <-s.applyNotifyCh:
			log.Printf("Candidate#%d apply()", s.config.ID)
			s.applyCommitted()
		}
	}
}

func (s *Server) runLeader() {
	log.Printf("SERVER#%d: Leader state", s.config.ID)

	heartbeatTimer := time.NewTimer(randHeartbeatTimeout())
	defer heartbeatTimer.Stop()

	s.mu.RLock()
	currentTerm := s.currentTerm
	commitIndex := s.commitIndex
	s.mu.RUnlock()

	for s.state == Leader {
		select {
		case cmd := <-s.clientCmdIn:
			log.Printf("Leader#%d received cmd from client", s.config.ID)
			heartbeatTimer.Reset(randHeartbeatTimeout())
			s.sendAppendEntries(false, cmd, currentTerm, commitIndex)
		case <-heartbeatTimer.C:
			log.Printf("Leader#%d heartbeat timeout", s.config.ID)
			heartbeatTimer.Reset(randHeartbeatTimeout())
			s.sendAppendEntries(true, CommandKV{}, currentTerm, commitIndex)
		case <-s.applyNotifyCh:
			log.Printf("Leader#%d apply()", s.config.ID)
			applyResults := s.applyCommitted()
			for _, applyResult := range applyResults {
				s.clientCmdOut <- applyResult
			}
		case in := <-s.rpc.RequestVoteInCh:
			log.Printf("Leader#%d received RequestVote", s.config.ID)
			if in.Term > s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.RequestVoteInCh <- in
			} else {
				s.rpc.RequestVoteOutCh <- s.rejectRequestVoteMsg()
			}
		case in := <-s.rpc.AppendEntriesInCh:
			log.Printf("Leader#%d received AppendEntries", s.config.ID)
			if in.Term == s.currentTerm {
				log.Fatal("SERVER(FATAL): 2 leaders in the same term")
			}

			if in.Term > s.currentTerm {
				s.convertToFollower(in.Term)
				// push to the follower
				s.rpc.AppendEntriesInCh <- in
			} else {
				s.rpc.AppendEntriesOutCh <- s.rejectAppendEntriesMsg()
			}
		}
	}
}

type Response string

// Submit attempts to execute a command and replicate it.
func (s *Server) Submit(command CommandKV) (Response, bool) {
	// TODO TODO.txt(6):  create command Id, in order to apply it once
	// TODO TODO.txt(11): we enter in this func <=> we are in the leader state => reconsider 'ok' value
	log.Printf("SERVER: Submit received by %v", s.config.ID)
	if s.state == Leader {
		s.clientCmdIn <- command
		return <-s.clientCmdOut, true
	}
	return "", false
}

func NewServer(id int32, db *map[string]string) *Server {
	return &Server{
		currentTerm: -1,
		votedFor:    -1,
		log:         NewLog(),

		commitIndex:   -1,
		lastApplied:   -1,
		nextIndex:     make([]int32, len(Cluster)),
		matchIndex:    make([]int32, len(Cluster)),
		currentLeader: -1,

		fsm:     NewStateMachineKV(db),
		config:  NewConfig(id),
		rpc:     NewRPCserver(1),
		storage: NewStorage(id),
		peers:   NewPeers(id),

		applyNotifyCh: make(chan struct{}),
		electNotifyCh: make(chan struct{}),
		clientCmdIn:   make(chan CommandKV),
		clientCmdOut:  make(chan Response),
	}
}

// No need to reset election timeout.
// State change triggers run{NewServerState}(),
// which creates a new timer.
func (s *Server) convertToFollower(newTerm int32) {
	s.currentTerm = newTerm
	s.votedFor = -1
	s.state = Follower
	s.PersistToStorage()
}

func (s *Server) applyCommitted() []Response {
	var responses []Response // TODO or we can allocate [] of size commitIndex - lastApplied
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
	s.mu.RLock()
	currLogIndex, currLogTerm := s.log.getLastLogIndexTerm()
	s.mu.RUnlock()

	return &proto.AppendEntriesResponse{
		Term:          s.currentTerm,
		Success:       false,
		ConflictIndex: currLogIndex,
		ConflictTerm:  currLogTerm,
	}
}

func (s *Server) sendAppendEntries(isHeartBeat bool, cmd CommandKV, currentTerm int32, commitIndex int32) {
	if !isHeartBeat {
		// TODO lock
		s.log = append(s.log, &proto.LogEntry{
			Index:       s.log.size(),
			Term:        s.currentTerm,
			CommandName: commandKVToString(&cmd),
		})
		s.PersistToStorage()
	}

	var wg sync.WaitGroup
	for _, peer := range *s.peers {
		wg.Add(1)
		go func(p Peer) {
			defer wg.Done()
			// retry if we have to
			for s.sendAppendEntry(isHeartBeat, p, currentTerm, commitIndex) {
			}
		}(peer)
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
	in := proto.AppendEntriesRequest{
		Term:         currentTerm,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		CommitIndex:  commitIndex,
		LeaderId:     s.config.ID,
		Entries:      entries,
	}
	s.mu.RUnlock()

	if out, err := p.AppendEntries(randAppendEntriesTimeout(), &in); err != nil {
		// TODO lock
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
						for _, peer := range *s.peers {
							if s.matchIndex[peer.id] >= N {
								serverCount++
							}
						}

						if s.isMajorityCount(serverCount) {
							s.commitIndex = N
						}
					}
				}

				if s.commitIndex != commitIndex {
					s.applyNotifyCh <- struct{}{}
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
	return 2*count > len(*s.peers)+1
}

func (s *Server) PersistToStorage() {
	s.storage.Save(NewPersistentState(s.currentTerm, s.votedFor, &s.log))
}

func (s *Server) IsLeader() bool {
	return s.state == Leader
}

func (s *Server) GetLeaderAddress() string {
	return Cluster[s.currentLeader].Address
}
