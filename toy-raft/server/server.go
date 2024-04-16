package server

import (
	"fmt"
	"toy-raft/state"
)

// receiver interface
type RaftNode interface {
	Start()
	Stop()
	Propose(block []byte) error
	Receive(block []byte)
}

type ServerImpl struct {
	Id string
	// raft node
	RaftNode RaftNode
	// state machine
	StateMachine state.StateMachine
}

func NewServer(id string, raftNode RaftNode, stateMachine state.StateMachine) *ServerImpl {
	return &ServerImpl{
		Id:           id,
		RaftNode:     raftNode,
		StateMachine: stateMachine,
	}
}

func (s *ServerImpl) Propose(blob []byte) error {
	return s.RaftNode.Propose(blob)
}

func (s *ServerImpl) Get() ([]byte, int) {
	latestBlock, offset := s.StateMachine.GetTailBlocks(1)
	return latestBlock[0], offset
}

func (s *ServerImpl) Start() {
	fmt.Printf("Starting server %s\n", s.Id)
	s.RaftNode.Start()
}

func (s *ServerImpl) Stop() {
	s.RaftNode.Stop()
}

func (s *ServerImpl) Receive(msg []byte) {
	s.RaftNode.Receive(msg)
}
