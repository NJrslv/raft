package raft

import (
	"math/rand"
	"time"
)

const (
	DefaultHeartbeatTimeout     = 50 * time.Millisecond
	DefaultElectionTimeout      = 150 * time.Millisecond
	DefaultRequestVoteTimeout   = DefaultElectionTimeout / (ClusterSize - 1)
	DefaultAppendEntriesTimeout = DefaultHeartbeatTimeout / (ClusterSize - 1)

	// Delta timeouts are used to randomize the timeout
	// within a range [defaultTimeout - delta, defaultTimeout + delta]

	DeltaHeartbeatTimeout     = 5 * time.Millisecond
	DeltaElectionTimeout      = 20 * time.Millisecond
	DeltaRequestVoteTimeout   = DeltaElectionTimeout / (ClusterSize - 1)
	DeltaAppendEntriesTimeout = DeltaHeartbeatTimeout / (ClusterSize - 1)

	MinHeartbeatInterval    = DefaultHeartbeatTimeout - DeltaHeartbeatTimeout
	MinElectionTimeout      = DefaultElectionTimeout - DeltaElectionTimeout
	MinRequestVoteTimeout   = DefaultRequestVoteTimeout - DeltaRequestVoteTimeout
	MinAppendEntriesTimeout = DefaultAppendEntriesTimeout - DeltaAppendEntriesTimeout
)

func randHeartbeatTimeout() time.Duration {
	timeoutOffset := rand.Intn(int(2 * DeltaHeartbeatTimeout))
	return MinHeartbeatInterval + time.Duration(timeoutOffset)
}

func randElectionTimeout() time.Duration {
	timeoutOffset := rand.Intn(int(2 * DeltaElectionTimeout))
	return MinElectionTimeout + time.Duration(timeoutOffset)
}

func randReqVoteTimeout() time.Duration {
	timeoutOffset := rand.Intn(int(2 * DeltaRequestVoteTimeout))
	return MinRequestVoteTimeout + time.Duration(timeoutOffset)
}

func randAppendEntriesTimeout() time.Duration {
	timeoutOffset := rand.Intn(int(2 * DeltaAppendEntriesTimeout))
	return MinAppendEntriesTimeout + time.Duration(timeoutOffset)
}
