package checks

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
)

type ServerStateSnapshot struct {
	Blocks [][]byte
	Offset int
}

func ServersConsistencyCheck(serverSnapshotMap map[string]ServerStateSnapshot, n int) error {

	var min_x int
	max_x := -1

	//for each server_snap:
	//max = Max(offset+len(blocks) - 1, max)
	//min = max - n
	for _, serverSnap := range serverSnapshotMap {
		max_x = max(serverSnap.Offset+len(serverSnap.Blocks)-1, max_x)
		min_x = max_x - n
	}

	//for each server_snap:
	//highest_block = len(blocks) + offset - 1
	//if highest_block < min {
	//panic("LAGGING")
	//}
	for serverId, serverSnap := range serverSnapshotMap {
		highestBlock := len(serverSnap.Blocks) + serverSnap.Offset - 1
		if highestBlock < min_x {
			return fmt.Errorf("server %s is lagging", serverId)
		}
	}

	for i := min_x; i <= max_x; i++ {
		var comparisonBlock []byte
		var comparisonServer string
		for serverId, serverSnap := range serverSnapshotMap {
			if i < serverSnap.Offset || i >= serverSnap.Offset+len(serverSnap.Blocks) {
				continue
			}
			targetBlock := serverSnap.Blocks[i-serverSnap.Offset]
			if comparisonBlock == nil {
				comparisonBlock = targetBlock
				comparisonServer = serverId
			} else if string(targetBlock) != string(comparisonBlock) {
				// pretty print snapshot
				b, _ := json.MarshalIndent(serverSnapshotMap, "", "  ")
				fmt.Println(string(b))

				comparisonBlockBase64 := base64.StdEncoding.EncodeToString(comparisonBlock)
				targetBlockBase64 := base64.StdEncoding.EncodeToString(targetBlock)
				return fmt.Errorf("block mismatch at index %d, expected (%s) %+v, got (%s) %+v", i, comparisonServer, comparisonBlockBase64, serverId, targetBlockBase64)
			}
		}
	}

	return nil
}
