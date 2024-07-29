package main

import (
	"errors"
	"flag"
	"fmt"
	"log"
	"strings"
	"time"
	"toy-raft/network"
	"toy-raft/raft"
	"toy-raft/server"
	"toy-raft/state"

	"github.com/nats-io/nats.go"
)

const n = 20

func main() {
	var (
		replicaId  string
		groupId    string
		natsUrl    string
		peerString string
	)
	flag.StringVar(&replicaId, "replicaId", "", "unique id of replica")
	flag.StringVar(&groupId, "groupId", "", "raft group id")
	flag.StringVar(&natsUrl, "natsUrl", "", "nats url")
	flag.StringVar(&peerString, "peers", "", "comma separated list of peer ids (including self)")
	flag.Parse()

	if replicaId == "" {
		panic("requires a valid replica id")
	}

	if groupId == "" {
		panic("requires a valid raft group id")
	}

	if peerString == "" {
		panic("requires a valid peer list")
	}
	peers := strings.Split(peerString, ",")

	if natsUrl == "" {
		log.Printf("no natsUrl was provided, using default: %s", nats.DefaultURL)
		natsUrl = nats.DefaultURL
	}

	network := network.NewNatsNetwork(groupId, natsUrl)

	sm := state.NewKeepLastBlocksStateMachine(replicaId, n)
	raftNode := raft.NewRaftNodeImpl(replicaId, sm, raft.NewInMemoryStorage(), network, peers)
	network.RegisterNode(replicaId, raftNode)
	server := server.NewServer(replicaId, raftNode, sm)
	server.Start()

	proposalCount := 0

	for {
		select {
		case <-time.After(1 * time.Second):
			if err := server.Propose([]byte{}); err != nil {
				if errors.Is(err, raft.ErrNotLeader) {
					continue
				}
				panic(err)
			}
			proposalCount++

		case <-time.After(10 * time.Second):
			fmt.Printf("proposalCount: %d\n", proposalCount)
		}
	}
}
