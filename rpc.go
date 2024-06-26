package raft

import (
	"context"
	"fmt"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"log"
	"net"
	"raft/proto"
	"time"
)

type Peer struct {
	id         int32
	connection *grpc.ClientConn
	rpcClient  proto.RaftClient
}

func (p *Peer) GetId() int32 {
	return p.id
}

func NewPeer(id int32) *Peer {
	return &Peer{id: id, connection: nil, rpcClient: nil}
}

func (p *Peer) dial() {
	conn, err := grpc.Dial(Cluster[p.id].Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		log.Fatalf("RPC(FATAL): did not connect: %v", err)
	}
	p.connection = conn
	p.rpcClient = proto.NewRaftClient(conn)
}

func (p *Peer) AppendEntries(timeout time.Duration, in *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-timeoutCtx.Done():
		log.Printf("RPC(WARN): AppendEntries timed out")
		return nil, fmt.Errorf("AppendEntries timed out")
	default:
	}

	return p.rpcClient.AppendEntries(timeoutCtx, in)
}

func (p *Peer) RequestVote(timeout time.Duration, in *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	timeoutCtx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	select {
	case <-timeoutCtx.Done():
		log.Printf("RPC(WARN): RequestVote timed out")
		return nil, fmt.Errorf("RequestVote timed out")
	default:
	}

	return p.rpcClient.RequestVote(timeoutCtx, in)
}

type Peers []*Peer

func NewPeers(we int32) *Peers {
	var peers Peers
	for id := range Cluster {
		if id != int(we) {
			peers = append(peers, NewPeer(int32(id)))
		}
	}
	return &peers
}

func (ps *Peers) DialPeers() {
	for _, p := range *ps {
		p.dial()
	}
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
	server             *grpc.Server

	proto.UnimplementedRaftServer
}

func (s *RPCserver) start(addr string) {
	log.Printf("RPC: Start RPC server")
	l, err := net.Listen("tcp", addr)
	if err != nil {
		log.Fatalf("RPC(FATAL): failed to listen: %v", err)
	}
	defer l.Close()

	if err := s.server.Serve(l); err != nil {
		log.Fatalf("RPC(FATAL): Failed to serve: %s", err.Error())
	}
}

func (s *RPCserver) stop() {
	s.server.Stop()
}

func NewRPCserver(bufferCapacity int) *RPCserver {
	s := &RPCserver{
		AppendEntriesInCh:  make(chan *proto.AppendEntriesRequest, bufferCapacity),
		AppendEntriesOutCh: make(chan *proto.AppendEntriesResponse, bufferCapacity),
		RequestVoteInCh:    make(chan *proto.RequestVoteRequest, bufferCapacity),
		RequestVoteOutCh:   make(chan *proto.RequestVoteResponse, bufferCapacity),
	}

	s.server = grpc.NewServer()
	proto.RegisterRaftServer(s.server, s)
	return s
}

func (s *RPCserver) AppendEntries(_ context.Context, in *proto.AppendEntriesRequest) (*proto.AppendEntriesResponse, error) {
	s.AppendEntriesInCh <- in
	return <-s.AppendEntriesOutCh, nil
}

func (s *RPCserver) RequestVote(_ context.Context, in *proto.RequestVoteRequest) (*proto.RequestVoteResponse, error) {
	s.RequestVoteInCh <- in
	return <-s.RequestVoteOutCh, nil
}
