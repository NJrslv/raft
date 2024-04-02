package raft

import (
	"context"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"raft/proto"
)

type Peer struct {
	connection *grpc.ClientConn
	rpcClient  proto.RaftClient
}

func NewPeer(id int32) *Peer {
	addr := Cluster[id].Address
	conn, err := grpc.Dial(addr, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil || id != Cluster[id].ID {
		log.Fatalf("RPC(FATAL): did not connect or id mismatch: %v", err)
	}
	return &Peer{connection: conn, rpcClient: proto.NewRaftClient(conn)}
}

func (p *Peer) AppendEntries(ctx context.Context, in *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	return p.rpcClient.AppendEntries(ctx, in)
}

func (p *Peer) RequestVote(ctx context.Context, in *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	return p.rpcClient.RequestVote(ctx, in)
}

type Peers []*Peer

func NewPeers() *Peers {
	peers := make(Peers, len(Cluster))
	for id := range Cluster {
		peers[id] = NewPeer(int32(id))
	}
	return &peers
}

func (ps *Peers) CloseConn() {
	for _, p := range *ps {
		p.connection.Close()
	}
}

type RPCserver struct {
	AppendEntriesInCh  chan *proto.AppendEntriesRequest
	AppendEntriesOutCh chan *proto.AppendEntriesResponse
	RequestVoteInCh    chan *proto.RequestVoteRequest
	RequestVoteOutCh   chan *proto.RequestVoteResponse

	proto.UnimplementedRaftServer
}

func NewRPCserver(bufferCapacity int) *RPCserver {
	return &RPCserver{
		AppendEntriesInCh:  make(chan *proto.AppendEntriesRequest, bufferCapacity),
		AppendEntriesOutCh: make(chan *proto.AppendEntriesResponse, bufferCapacity),
		RequestVoteInCh:    make(chan *proto.RequestVoteRequest, bufferCapacity),
		RequestVoteOutCh:   make(chan *proto.RequestVoteResponse, bufferCapacity),
	}
}

// in context maybe pass deadlines and timers like
// ctx, cancel := context.WithTimeout(context.Background(), time.Second)

func (s *RPCserver) AppendEntries(_ context.Context, in *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	s.AppendEntriesInCh <- in
	return <-s.AppendEntriesOutCh, nil
}

func (s *RPCserver) RequestVote(_ context.Context, in *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	s.RequestVoteInCh <- in
	return <-s.RequestVoteOutCh, nil
}
