package raft

import (
	"math/rand"
	"time"
)

const (
	slowCoff = 10

	DefaultHeartbeatTimeout     = slowCoff * 5 * time.Millisecond
	DefaultElectionTimeout      = slowCoff * 150 * time.Millisecond
	DefaultRequestVoteTimeout   = slowCoff * 40 * time.Millisecond
	DefaultAppendEntriesTimeout = slowCoff * 30 * time.Millisecond
)

func randomTimeout(minVal time.Duration) time.Duration {
	extra := time.Duration(rand.Int63()) % minVal
	return minVal + extra
}
