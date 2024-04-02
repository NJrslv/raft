package raft

import (
	"log"
	"raft/proto"
	"sync"
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
	mu      sync.Mutex
	rpc     *RPCserver
	storage *Storage
	peers   *Peers

	applyNotifyCh chan struct{}
}

func (s *Server) StartServer() {
	defer s.peers.CloseConn()
	s.state = Follower
	go s.run()
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

	electionTimer := GenerateElectionTimer()
	defer electionTimer.Stop()

	for s.state == Follower {
		select {
		case <-electionTimer.C:
			log.Printf("Follower#%d election time out", s.config.ID)

			s.state = Candidate
		case <-s.applyNotifyCh:
			log.Printf("Follower#%d Apply()", s.config.ID)

			for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
				s.fsm.Apply(parseCommandKV(s.log[i].CommandName))
				s.lastApplied++
			}
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
					electionTimer = GenerateElectionTimer()
				}
				s.PersistToStorage()
			}
			s.rpc.RequestVoteOutCh <- &out
		case in := <-s.rpc.AppendEntriesInCh:
			log.Printf("Follower#%d receive AppendEntries", s.config.ID)

			var out proto.AppendEntriesResponse
			// Reply false if term < currentTerm OR log doesn’t contain
			// an entry at prevLogIndex whose term matches prevLogTerm.
			// Also help the leader bring us up to date quickly (By
			// skipping the terms). As described in $5.3 of the paper.
			if in.Term < s.currentTerm || !s.log.isPrevLogTermTheSame(in.PrevLogIndex, in.PrevLogTerm) {
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
				break
			}

			s.currentTerm = in.Term
			s.votedFor = -1
			electionTimer = GenerateElectionTimer()

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
	for s.state == Candidate {
		select {
		case <-s.applyNotifyCh:
			log.Printf("Candidate#%d Apply()", s.config.ID)
			for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
				s.fsm.Apply(parseCommandKV(s.log[i].CommandName))
			}
		}
	}
}

func (s *Server) runLeader() {
	log.Printf("SERVER#%d: Leader state", s.config.ID)
	for s.state == Candidate {
		select {
		case <-s.applyNotifyCh:
			log.Printf("Leader#%d Apply()", s.config.ID)
			for i := s.lastApplied + 1; i <= s.commitIndex; i++ {
				s.fsm.Apply(parseCommandKV(s.log[i].CommandName))
			}
		}
	}
}

// Attempts to execute a command and replicate it. The function will return the
// result when the command has been successfully committed or an error has occurred

type Response string

// coomitIndex may be >>>> executed command bec of concurrency

func (s *Server) Submit(command interface{}) (Response, bool) {
	// reogrganize the system
	// put there in client chan command to apply

	s.mu.Lock()
	defer s.mu.Unlock()

	log.Printf("SERVER: Submit received by %v", s.config.ID)
	if s.state == Leader {
		// append to log
		// append entries to peers
		// commit
	}
	return "", false
}

func NewServer(id int32, db *map[string]string) *Server {
	nextIndex, matchIndex := make([]int32, len(Cluster)), make([]int32, len(Cluster))
	for i := 0; i < len(Cluster); i++ {
		nextIndex[i], matchIndex[i] = -1, -1
	}
	return &Server{
		currentTerm: 0,
		votedFor:    -1,
		log:         NewLog(),

		commitIndex: -1,
		lastApplied: -1,

		nextIndex:  nextIndex,
		matchIndex: matchIndex,

		currentLeader: -1,
		fsm:           NewStateMachineKV(db),
		config:        NewConfig(id),
		rpc:           NewRPCserver(1),
		storage:       NewStorage(id),
		peers:         NewPeers(),
	}
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
