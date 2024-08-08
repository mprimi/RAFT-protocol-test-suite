package main

import (
	"errors"
	"log"
	"math/rand"
	"time"

	"toy-raft/checks"
	"toy-raft/network"
	"toy-raft/raft"
	"toy-raft/server"
	"toy-raft/state"
)

func TestRaftWithNatsNetwork() {

	n := 10
	groupId := "foo"
	ids := []string{"A", "B", "C"}
	servers := make([]*server.ServerImpl, 0, 3)

	for _, id := range ids {
		net, err := network.NewNatsNetwork(groupId, "nats://127.0.0.1:18001,nats://127.0.0.1:18002,nats://127.0.0.1:18003,nats://127.0.0.1:18004,nats://127.0.0.1:18005,")
		if err != nil {
			panic(err)
		}
		sm := state.NewKeepLastBlocksStateMachine(id, n)
		raftNode := raft.NewRaftNodeImpl(id, groupId, sm, raft.NewInMemoryStorage(), net, ids)
		net.RegisterNode(id, raftNode)
		server := server.NewServer(
			id,
			raftNode,
			sm,
		)
		servers = append(servers, server)
		go server.Start()
	}

	rng := rand.New(rand.NewSource(12345))
	buffer := make([]byte, 10)

	proposeTicker := time.NewTicker(800 * time.Millisecond)
	blockCheckTicker := time.NewTicker(8 * time.Second)
	progressCheckTicker := time.NewTicker(10 * time.Second)

	blocksProposed := 0

	highestBlockApplied := uint64(0)

	for {
		select {
		case <-proposeTicker.C:
			for _, server := range servers {
				rng.Read(buffer)
				err := server.Propose(buffer)
				if errors.Is(err, raft.ErrNotLeader) {
					// ignore
				} else if err != nil {
					panic(err)
				} else {
					log.Printf("👹 proposed block %d: %v\n", blocksProposed, buffer)
					blocksProposed++
				}
			}
		case <-progressCheckTicker.C:
			maxApplied := uint64(0)
			for _, server := range servers {
				maxApplied = max(server.StateMachine.Applied(), maxApplied)
			}
			if maxApplied > highestBlockApplied {
				highestBlockApplied = maxApplied
			} else {
				panic("no new blocks applied")
			}
		case <-blockCheckTicker.C:
			log.Printf("🔮 Snapshotting servers")
			serverSnapshotMap := make(map[string]checks.ServerStateSnapshot, len(servers))
			for _, server := range servers {
				blocks, offset := server.StateMachine.(*state.KeepLastBlocksStateMachine).GetTailBlocks(n)
				serverSnapshotMap[server.Id] = checks.ServerStateSnapshot{
					Blocks: blocks,
					Offset: offset,
				}
			}

			if err := checks.ServersConsistencyCheck(serverSnapshotMap, n); err != nil {
				panic("Servers are not consistent: " + err.Error())
			}
			log.Printf("✅ Servers are consistent")
		}
	}
}
