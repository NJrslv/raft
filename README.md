# raft
## Table of Contents
- [Introduction](#introduction)
- [Features](#features)
- [Run](#run)
- [Structure](#structure)

## Introduction
This repository contains an educational implementation of the [Raft Consensus Algorithm](https://en.wikipedia.org/wiki/Raft_(algorithm)) in Go.

## Features

| Feature            | Implemented |
| ------------------ | ----------- |
| Log Replication    | Yes         |
| Leader Election    | Yes         |
| Persistence        | Yes         |
| Membership Changes | Yes         |
| Log Compaction     | No          |

## Run
- Format:
```bash
go run main.go --id {node server id} --kvaddr {key-value server address}
```

- Example, running all the nodes listed in the configuration file config.go

  
`Process#0`
```bash
go run -race main.go --id 0 --kvaddr localhost:27004
```
`Process#1`
```bash
go run -race main.go --id 1 --kvaddr localhost:27005
```
`Process#2`
```bash
go run -race main.go --id 2 --kvaddr localhost:27006
```
`Client`
```bash
curl http://localhost:27006/set?key=a&val=b
> set value

curl http://localhost:27006/get?key=a
> b
```

## Structure
key-value and raft servers are running under the same process
- `server.go` - main part
- `Makefile` - compiles proto
- `kv/bin/state#{id}.json` - persistent state of a node with id {id} (generated after running main.go)
- `kv/bin/main.go` - starts raft and key-value API servers
- `config.go` - config of the raft cluster in the following way:
```go
var Cluster = []ServerConfig{  
    {Id: 0, Address: "localhost:8080"}, 
}
```
- `time.go` - all raft time constants multiplied by slow coefficient in the following way:
```go
const (  
    slowCoeff = 10  
    DefaultElectionTimeout = slowCoeff * 150 * time.Millisecond  
    ...
)
```
- Require further validation to ensure the implementation's correctness
