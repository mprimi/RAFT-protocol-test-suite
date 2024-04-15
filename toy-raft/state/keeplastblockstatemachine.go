package state

import (
	"fmt"
	"log"
	"sync"
)

type KeepLastBlocksStateMachine struct {
	Id string

	sync.RWMutex

	offset int
	blocks [][]byte

	n int
}

func NewKeepLastBlocksStateMachine(id string, n int) StateMachine {
	sm := &KeepLastBlocksStateMachine{
		Id: id,
		// init state
		offset: 0,
		blocks: make([][]byte, 0, n),
		n:      n,
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
	sm.Lock()
	defer sm.Unlock()
	// replace block
	sm.blocks = append(sm.blocks, block)
	if len(sm.blocks) > sm.n {
		discardedElements := len(sm.blocks) - sm.n
		sm.blocks = sm.blocks[discardedElements:]
		sm.offset += discardedElements
	}
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

func (sm *KeepLastBlocksStateMachine) Size() int {
	return len(sm.blocks)
}
