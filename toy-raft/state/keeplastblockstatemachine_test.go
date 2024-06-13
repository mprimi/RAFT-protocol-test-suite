package state

import (
	"fmt"
	"math/rand"
	"reflect"
	"testing"
)

func TestCreateSnapshotAndRestore(t *testing.T) {

	n := 5
	applyCounts := []int{
		0,
		1,
		100,
		n - 1,
		n,
		n + 1,
		n + 10,
		n * 2,
	}

	for _, applyCount := range applyCounts {
		t.Run(fmt.Sprintf("applied=%d", applyCount), func(t *testing.T) {

			oldSm := NewKeepLastBlocksStateMachine("new", n)

			rng := rand.New(rand.NewSource(12345))
			buffer := make([]byte, 8)

			for i := 0; i < applyCount; i++ {
				_, err := rng.Read(buffer)
				if err != nil {
					t.Fatal(err)
				}
				oldSm.Apply(buffer)
			}

			snapshotBytes, err := oldSm.CreateSnapshot()
			if err != nil {
				t.Fatal(err)
			}

			newSm := NewKeepLastBlocksStateMachine("old", n)
			err = newSm.InstallSnapshot(snapshotBytes)
			if err != nil {
				t.Fatal(err)
			}

			newSmBlocks, newSmOffset := newSm.GetTailBlocks(n)
			oldSmBlocks, oldSmOffset := oldSm.GetTailBlocks(n)

			if newSmOffset != oldSmOffset {
				t.Fatalf("offset mismatch: %d vs. %d", newSmOffset, oldSmOffset)
			}

			if !reflect.DeepEqual(newSmBlocks, oldSmBlocks) {
				t.Fatalf("blocks mismatch, %+v vs. %+v", newSmBlocks, oldSmBlocks)
			}
		})
	}

}
