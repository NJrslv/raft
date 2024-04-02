package raft

import (
	"math/rand"
	"time"
)

// Define default heartbeat interval and election timeout
const (
	DefaultHeartbeatInterval  = 50 * time.Millisecond
	DefaultElectionTimeout    = 150 * time.Millisecond
	DefaultRequestVoteTimeout = DefaultElectionTimeout / (ClusterSize - 1)

	// DeltaElectionTimeout is used to randomize the election timeout
	// within a range [defaultTimeout - delta, defaultTimeout + delta]
	DeltaElectionTimeout    = 20 * time.Millisecond
	DeltaRequestVoteTimeout = DeltaElectionTimeout / (ClusterSize - 1)

	MinElectionTimeout    = DefaultElectionTimeout - DeltaElectionTimeout
	MinRequestVoteTimeout = DefaultRequestVoteTimeout - DeltaRequestVoteTimeout
)

func randElectionTimer() *time.Timer {
	timeoutOffset := rand.Intn(int(2 * DeltaElectionTimeout))
	return time.NewTimer(MinElectionTimeout + time.Duration(timeoutOffset))
}

func randReqVoteTimeout() time.Duration {
	timeoutOffset := rand.Intn(int(2 * DeltaRequestVoteTimeout))
	return MinRequestVoteTimeout + time.Duration(timeoutOffset)
}
