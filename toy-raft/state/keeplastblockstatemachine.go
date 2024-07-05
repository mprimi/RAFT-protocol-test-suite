package state

import (
	"encoding/json"
	"fmt"
	"log"
)

type KeepLastBlocksStateMachine struct {
	Id string

	offset       int
	blocks       [][]byte
	appliedCount uint64

	n int
}

type Snapshot struct {
	Blocks [][]byte `json:"blocks"`
	Offset int      `json:"offset"`
}

func NewKeepLastBlocksStateMachine(id string, n int) *KeepLastBlocksStateMachine {
	sm := &KeepLastBlocksStateMachine{
		Id: id,
		// init state
		offset:       0,
		blocks:       make([][]byte, 0, n),
		n:            n,
		appliedCount: 0,
	}
	return sm
}

func (sm *KeepLastBlocksStateMachine) Log(format string, args ...any) {
	front := fmt.Sprintf("💾 SM-%s: ", sm.Id)
	log.Printf(front+format+"\n", args...)
}

func (sm *KeepLastBlocksStateMachine) GetId() string {
	return sm.Id
}

func (sm *KeepLastBlocksStateMachine) Apply(block []byte) {
	// replace block
	sm.blocks = append(sm.blocks, block)
	if len(sm.blocks) > sm.n {
		discardedElements := len(sm.blocks) - sm.n
		sm.blocks = sm.blocks[discardedElements:]
		sm.offset += discardedElements
	}
	sm.appliedCount++
	sm.Log("Applied block %d", sm.offset+len(sm.blocks))
}

// GetTailBlocks returns the last m blocks and the offset (actual index of the first block)
func (sm *KeepLastBlocksStateMachine) GetTailBlocks(m int) (blocks [][]byte, offset int) {
	if m > len(sm.blocks) {
		blocks = sm.blocks
		offset = sm.offset
	} else {
		x := len(sm.blocks) - m
		blocks = sm.blocks[x:]
		offset = sm.offset + x
	}
	return
}

func (sm *KeepLastBlocksStateMachine) Applied() uint64 {
	return sm.appliedCount
}

func (sm *KeepLastBlocksStateMachine) CreateSnapshot() ([]byte, error) {
	snapshot := Snapshot{
		Blocks: sm.blocks,
		Offset: sm.offset,
	}

	snapshotBytes, err := json.Marshal(snapshot)
	if err != nil {
		return nil, err
	}

	return snapshotBytes, nil
}

func (sm *KeepLastBlocksStateMachine) InstallSnapshot(snapshotBytes []byte) error {
	var snapshot Snapshot

	err := json.Unmarshal(snapshotBytes, &snapshot)
	if err != nil {
		return err
	}

	sm.blocks = snapshot.Blocks
	sm.offset = snapshot.Offset

	return nil
}
