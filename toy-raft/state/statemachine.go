package state

type StateMachine interface {

	Applied() uint64

	// META
	GetId() string

	// should only be called by raft layer
	Apply(block []byte)

	CreateSnapshot() ([]byte, error)

	InstallSnapshot(snapshot []byte) error
}
