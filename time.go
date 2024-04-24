package raft

import (
	"math/rand"
	"time"
)

const (
	DefaultHeartbeatTimeout     = 5 * time.Millisecond
	DefaultElectionTimeout      = 150 * time.Millisecond
	DefaultRequestVoteTimeout   = 40 * time.Millisecond
	DefaultAppendEntriesTimeout = 30 * time.Millisecond

	slowCoff = 10
)

func randomTimeout(minVal time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minVal
	return slowCoff * (minVal + extra)
}
