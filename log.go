package raft

import (
	"raft/proto"
)

type Log []*proto.LogEntry

func (l *Log) isPrevLogTermTheSame(prevLogIndex int32, prevLogTerm int32) bool {
	return prevLogIndex == -1 || prevLogIndex < l.size() && (*l)[prevLogIndex].Term == prevLogTerm
}

func (l *Log) getLastLogIndexTerm() (int32, int32) {
	ind, term := int32(-1), int32(-1)
	if l.size() > 0 {
		ind = l.size() - 1
		term = (*l)[ind].Term
	}
	return ind, term
}

func (l *Log) isLogUpToDateWith(lastLogIndex int32, lastLogTerm int32) bool {
	isUpToDate := true
	if l.size() > 0 {
		currLastLogIndex, currLastLogTerm := (*l)[l.size()-1].Index, (*l)[l.size()-1].Term
		isUpToDate = lastLogTerm > currLastLogTerm ||
			lastLogTerm == currLastLogTerm && lastLogIndex >= currLastLogIndex
	}
	return isUpToDate
}

func (l *Log) size() int32 {
	return int32(len(*l))
}
