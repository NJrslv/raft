package raft

import "strings"

type ServerConfig struct {
	ID      int32
	Address string
}

func NewConfig(id int32) *ServerConfig {
	return &Cluster[id]
}

// config is statically defined

const ClusterSize = 3

var Cluster = []ServerConfig{
	{ID: 0, Address: "localhost:27000"},
	{ID: 1, Address: "localhost:27001"},
	{ID: 2, Address: "localhost:10002"},
}

func FindServerAddressByID(id int32) (string, bool) {
	if id < int32(len(Cluster)) {
		return Cluster[id].Address, true
	}
	return "", false
}

func GetPort(address string) string {
	return strings.SplitN(address, ":", 2)[1]
}
