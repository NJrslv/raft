package raft

import (
	"math/rand"
	"time"
)

const (
	slowCoeff = 10

	DefaultHeartbeatTimeout     = slowCoeff * 5 * time.Millisecond
	DefaultElectionTimeout      = slowCoeff * 150 * time.Millisecond
	DefaultRequestVoteTimeout   = slowCoeff * 40 * time.Millisecond
	DefaultAppendEntriesTimeout = slowCoeff * 30 * time.Millisecond
)

func randomTimeout(minVal time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minVal
	return minVal + extra
}
