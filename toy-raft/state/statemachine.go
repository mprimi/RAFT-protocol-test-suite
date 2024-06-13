package state

type StateMachine interface {

	Size() int

	// return the last n blocks and the index of the first block
	GetTailBlocks(n int) (blocks [][]byte, offset int)

	// META
	GetId() string

	// should only be called by raft layer
	Apply(block []byte)

	CreateSnapshot() ([]byte, error)

	InstallSnapshot(snapshot []byte) error
}
