package state

import (
	"fmt"
	"sync"
)

type KeepLastBlockStateMachine struct {
	Id string

	sync.RWMutex

	// state
	blockResult []byte
	blockCount  uint64
}

func NewKeepLastBlockStateMachine(id string) StateMachine {
	sm := &KeepLastBlockStateMachine{
		Id: id,
		// init state
		blockResult: make([]byte, 10),
		blockCount:  0,
	}
	return sm
}

func (sm *KeepLastBlockStateMachine) Log(format string, args ...any) {
	front := fmt.Sprintf("SM-%s: ", sm.Id)
	fmt.Printf(front+format+"\n", args...)
}

func (sm *KeepLastBlockStateMachine) GetId() string {
	return sm.Id
}

func (sm *KeepLastBlockStateMachine) Apply(block []byte) {
	sm.Lock()
	defer sm.Unlock()
	sm.Log("Applying block %s after block#%d", string(block), sm.blockCount)
	// replace block
	sm.blockResult = block
	sm.blockCount++
}

func (sm *KeepLastBlockStateMachine) GetCurrentValue() ([]byte, uint64) {
	sm.Lock()
	defer sm.Unlock()
	return sm.blockResult, sm.blockCount
}
