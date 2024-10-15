package state

import "io"

type StateMachine interface {
	Applied() uint64

	// META
	GetId() string

	// should only be called by raft layer
	Apply(block []byte)

	CreateSnapshot(writer io.Writer) error

	InstallSnapshot(reader io.Reader) error
}
