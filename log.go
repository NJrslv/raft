package raft

import (
	"raft/proto"
)

type Log []*proto.LogEntry

func NewLog() Log {
	return Log{}
}

func (l *Log) isPrevLogTermTheSame(prevLogIndex int32, prevLogTerm int32) bool {
	return prevLogIndex == -1 || prevLogIndex < l.size() && (*l)[prevLogIndex].Term == prevLogTerm
}

func (l *Log) isLogUpToDateWith(lastLogIndex int32, lastLogTerm int32) bool {
	isUpToDate := true
	if l.size() > 0 {
		currLastLogIndex, currLastLogTerm := (*l)[l.size()-1].Term, (*l)[l.size()-1].Index
		isUpToDate = lastLogTerm > currLastLogTerm ||
			lastLogTerm == currLastLogTerm && lastLogIndex >= currLastLogIndex
	}
	return isUpToDate
}

func (l *Log) size() int32 {
	return int32(len(*l))
}
