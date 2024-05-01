package checks

import "testing"

func TestServerConsistencyCheckLagging(t *testing.T) {
	serverSnapshotMap := map[string]ServerStateSnapshot{
		"s1": {
			Blocks: [][]byte{
				{2},
				{3},
				{4},
			},
			Offset: 2,
		},
		"s2": {
			Blocks: [][]byte{
				{3},
				{4},
				{5},
			},
			Offset: 3,
		},
		"s3": {
			Blocks: [][]byte{
				{4},
				{5},
				{6},
			},
			Offset: 4,
		},
		"s4": {
			Blocks: [][]byte{
				{1},
				{2},
			},
			Offset: 1,
		},
	}

	err := ServersConsistencyCheck(serverSnapshotMap, 3)
	if err == nil {
		t.Fatalf("expected error, s4 is lagging")
	}

}

func TestServerConsistencyCheckConsistentState(t *testing.T) {
	serverSnapshotMap := map[string]ServerStateSnapshot{
		"s1": {
			Blocks: [][]byte{
				{2},
				{3},
				{4},
			},
			Offset: 2,
		},
		"s2": {
			Blocks: [][]byte{
				{3},
				{4},
				{5},
			},
			Offset: 3,
		},
		"s3": {
			Blocks: [][]byte{
				{4},
				{5},
				{6},
			},
			Offset: 4,
		},
	}

	err := ServersConsistencyCheck(serverSnapshotMap, 3)
	if err != nil {
		t.Fatalf("expected nil, servers all share at least one block")
	}

}
