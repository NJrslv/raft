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
	// within a range [defaultTimeout, defaultTimeout + delta]

	DeltaHeartbeatTick     = 10
	DeltaElectionTick      = 150
	DeltaRequestVoteTick   = 20
	DeltaAppendEntriesTick = 20
)

func randHeartbeatTimeout() time.Duration {
	return DefaultHeartbeatTimeout + time.Duration(rand.Intn(DeltaHeartbeatTick))*time.Millisecond
}

func randElectionTimeout() time.Duration {
	return DefaultElectionTimeout + time.Duration(rand.Intn(DeltaElectionTick))*time.Millisecond
}

func randReqVoteTimeout() time.Duration {
	return DefaultRequestVoteTimeout + time.Duration(rand.Intn(DeltaRequestVoteTick))*time.Millisecond
}

func randAppendEntriesTimeout() time.Duration {
	return DefaultAppendEntriesTimeout + time.Duration(rand.Intn(DeltaAppendEntriesTick))*time.Millisecond
}
