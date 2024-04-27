package raft

import (
	"fmt"
	"log"
	"strings"
)

type Op string

const (
	Get Op = "GET"
	Set Op = "SET"
)

type CommandKV struct {
	Op    Op
	Key   string
	Value string
}

type StateMachineKV struct {
	db *map[string]string
}

func NewStateMachineKV(db *map[string]string) *StateMachineKV {
	return &StateMachineKV{db: db}
}

func (fsm *StateMachineKV) Apply(cmd *CommandKV) Response {
	var response Response = ""
	if cmd.Op == Get {
		response = Response((*fsm.db)[cmd.Key])
	} else {
		(*fsm.db)[cmd.Key] = cmd.Value
	}
	return response
}

func stringToCommandKV(input string) *CommandKV {
	parts := strings.Split(input, "-")
	if len(parts) != 3 {
		log.Printf("invalid command: %d", len(parts))
		log.Fatalf("STATEMACHINE(FATAL): Invalid format: %s, expected {GET/SET}-{KEY}-{VALUE/}", input)
	}

	return &CommandKV{
		Op:    opFromString(parts[0]),
		Key:   parts[1],
		Value: parts[2],
	}
}

func commandKVToString(cmd *CommandKV) string {
	return fmt.Sprintf("%s-%s-%s", cmd.Op, cmd.Key, cmd.Value)
}

func opFromString(s string) Op {
	var op Op
	switch s {
	case "GET":
		op = Get
	case "SET":
		op = Set
	default:
		log.Fatalf("STATEMACHINE(FATAL): Invalid operation: %s", s)
	}
	return op
}
