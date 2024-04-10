package raft

import "log"

const (
	HeartbeatTimeout     = "[HEARTBEAT TIMEOUT]     "
	StateChanged         = "[STATE CHANGED]         "
	ElectionStart        = "[ELECTION START]        "
	ElectionTimeout      = "[ELECTION TIMEOUT]      "
	ElectionStop         = "[ELECTION STOPPED]      "
	Error                = "[ERROR]                 "
	SendAppendEntries    = "[SEND APPEND ENTRIES]   "
	SendRequestVote      = "[SEND REQUEST VOTE]     "
	ReceiveAppendEntries = "[RECEIVE APPEND ENTRIES]"
	ReceiveRequestVote   = "[RECEIVE REQUEST VOTE]  "
	Apply                = "[APPLY]                 "
	ReceiveClientCmd     = "[RECEIVE CLIENT CMD]    "
	RejectAppendEntries  = "[REJECT APPEND ENTRIES] "
	RejectRequestVote    = "[REJECT REQUEST VOTE]   "
	ElectionWin          = "[ELECTION WIN]          "
	AdvanceCommitIndex   = "[ADVANCE COMMIT INDEX]  "
)

func logRaft(state State, prefix string, currId int32, term int32) {
	log.Printf("%s %s#%d in term %d\n", prefix, stateToStr(state), currId, term)
}

func logInfo(msg string) {
	log.Println(msg)
}

func logError(err string) {
	log.Fatalf("%s %s\n", Error, err)
}

func stateToStr(state State) string {
	var result string
	switch state {
	case Follower:
		result = "Follower"
	case Candidate:
		result = "Candidate"
	case Leader:
		result = "Leader"
	default:
		result = "UnknownState"
	}
	return result
}
