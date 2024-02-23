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
	id string
	// raft node
	raftNode RaftNode
	// state machine
	stateMachine state.StateMachine
}

func (s *ServerImpl) Propose(blob []byte) {
	s.raftNode.Propose(blob)
}

func (s *ServerImpl) Get() ([]byte, uint64) {
	return s.stateMachine.GetCurrentValue()
}

func (s *ServerImpl) Start() {
	fmt.Printf("Starting server %s\n", s.id)
	s.raftNode.Start()
}

func (s *ServerImpl) Stop() {
	s.raftNode.Stop()
}

func (s *ServerImpl) Receive(msg []byte) {
	s.raftNode.Receive(msg)
}
