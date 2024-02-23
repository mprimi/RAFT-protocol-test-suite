package network

import "sync"

type PerfectNetwork struct {
	nodes map[string]NetworkDevice
	sync.RWMutex
}

func NewPerfectNetwork() Network {
	return &PerfectNetwork{
		nodes: make(map[string]NetworkDevice),
	}
}

func (net *PerfectNetwork) Broadcast(msg []byte) {
	net.Lock()
	defer net.Unlock()
	// "sending to everyone"
	for id := range net.nodes {
		net.Send(id, msg)
	}
}

func (net *PerfectNetwork) Send(id string, msg []byte) {
	net.nodes[id].Receive(msg)
}

func (net *PerfectNetwork) RegisterNode(id string, networkDevice NetworkDevice) {
	net.nodes[id] = networkDevice
}
