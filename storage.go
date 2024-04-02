package raft

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
)

type PersistentState struct {
	CurrentTerm int32 `json:"currentTerm"`
	VotedFor    int32 `json:"votedFor"`
	Log         *Log  `json:"log"`
}

func NewPersistentState(term int32, votedFor int32, log *Log) *PersistentState {
	return &PersistentState{
		CurrentTerm: term,
		VotedFor:    votedFor,
		Log:         log,
	}
}

type Storage struct {
	filePath string
}

func NewStorage(serverId int32) *Storage {
	filePath := fmt.Sprintf("raft/state#%d.json", serverId)
	file, err := os.Create(filePath)
	if err != nil {
		log.Fatalf("STORAGE(FATAL): Failed to create file: %v", err)
	}
	defer file.Close()
	return &Storage{filePath: filePath}
}

func (s *Storage) Save(state *PersistentState) error {
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}

	file, err := os.OpenFile(s.filePath, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	_, err = file.Write(data)
	if err != nil {
		return err
	}

	if err := file.Sync(); err != nil {
		return err
	}

	return nil
}

func (s *Storage) Load() (*PersistentState, error) {
	data, err := os.ReadFile(s.filePath)
	if err != nil {
		if os.IsNotExist(err) {
			return &PersistentState{}, nil
		}
		return nil, err
	}

	var state PersistentState
	err = json.Unmarshal(data, &state)
	if err != nil {
		return nil, err
	}

	return &state, nil
}
