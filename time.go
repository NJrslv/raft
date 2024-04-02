package raft

import (
	"math/rand"
	"time"
)

// Define default heartbeat interval and election timeout
const (
	DefaultHeartbeatInterval = 50 * time.Millisecond
	DefaultElectionTimeout   = 150 * time.Millisecond

	// DeltaElectionTimeout is used to randomize the election timeout
	// within a range [defaultTimeout - delta, defaultTimeout + delta]
	DeltaElectionTimeout = 20 * time.Millisecond

	MinElectionTimeout = DefaultElectionTimeout - DeltaElectionTimeout
)

func GenerateElectionTimer() *time.Timer {
	timeoutOffset := rand.Intn(int(2 * DeltaElectionTimeout))
	return time.NewTimer(MinElectionTimeout + time.Duration(timeoutOffset))
}
