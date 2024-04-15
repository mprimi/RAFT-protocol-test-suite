package network

import (
	"fmt"
	"log"
	"math/rand"
	"sync"
	"time"
)

const messageQueueBufferSz = 1000
const messageQueueWorkerBackoffDuration = 100 * time.Millisecond

type PseudoAsyncNetwork struct {
	nodes map[string]NetworkDevice

	messageQueues map[string]chan []byte

	sync.RWMutex
}

// packetLoss is an integer between 0-100
func NewPseudoAsyncNetwork(packetLoss int) Network {
	asyncNetwork := &PseudoAsyncNetwork{
		nodes:         make(map[string]NetworkDevice),
		messageQueues: make(map[string]chan []byte),
	}

	if packetLoss < 0 || packetLoss > 100 {
		panic("bad packet loss value")
	}

	// message queue worker
	go func() {
		for {
			allQueuesEmpty := true
			for id, messageQueue := range asyncNetwork.messageQueues {
				select {
				case message := <-messageQueue:
					random := rand.Int() % 101
					if packetLoss > random {
						asyncNetwork.Log("dropping packet intended for %s", id)
					} else {
						asyncNetwork.nodes[id].Receive(message)
					}
					allQueuesEmpty = false
				default:
				}
			}
			if allQueuesEmpty {
				time.Sleep(messageQueueWorkerBackoffDuration)
			}
		}
	}()

	return asyncNetwork
}

func (net *PseudoAsyncNetwork) Log(format string, args ...any) {
	log.Printf("🛜 NETWORK: "+format+"\n", args...)
}

func (net *PseudoAsyncNetwork) Broadcast(msg []byte) {
	net.Lock()
	defer net.Unlock()
	for id := range net.nodes {
		net.Send(id, msg)
	}
}

func (net *PseudoAsyncNetwork) Send(id string, msg []byte) {
	select {
	case net.messageQueues[id] <- msg:
	default:
		panic(fmt.Sprintf("message queue (%s) is full", id))
	}
}

func (net *PseudoAsyncNetwork) RegisterNode(id string, networkDevice NetworkDevice) {
	net.nodes[id] = networkDevice
	net.messageQueues[id] = make(chan []byte, messageQueueBufferSz)
}
