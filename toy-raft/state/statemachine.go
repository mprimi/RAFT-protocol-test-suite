package state

type StateMachine interface {
	GetCurrentValue() ([]byte, uint64)

	// META
	GetId() string

	// should only be called by raft layer
	Apply(block []byte)
}
