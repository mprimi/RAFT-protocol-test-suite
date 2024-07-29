package network

import (
	"fmt"
	"log"

	"github.com/nats-io/nats.go"
)

const NatsSubjectPrefix = "RAFT"

type NatsNetwork struct {
	conn *nats.Conn

	groupId          string
	proposalSubject  string
	broadcastSubject string
	unicastPrefix    string

	networkDevices map[string]NetworkDevice
}

func NewNatsNetwork(groupId, natsUrl string) Network {

	nc, err := nats.Connect(natsUrl)
	if err != nil {
		panic("todo")
	}

	natsNetwork := &NatsNetwork{
		conn:             nc,
		groupId:          groupId,
		proposalSubject:  fmt.Sprintf("%s.%s.proposal", NatsSubjectPrefix, groupId),
		broadcastSubject: fmt.Sprintf("%s.%s.broadcast", NatsSubjectPrefix, groupId),
		unicastPrefix:    fmt.Sprintf("%s.%s", NatsSubjectPrefix, groupId),
		networkDevices:   make(map[string]NetworkDevice),
	}

	return natsNetwork
}

func (net *NatsNetwork) RegisterNode(id string, networkDevice NetworkDevice) {
	// subscribe to unicast messages
	{
		recipientSubj := fmt.Sprintf("%s.%s", net.unicastPrefix, id)
		_, err := net.conn.Subscribe(recipientSubj, func(msg *nats.Msg) {
			networkDevice.Receive(msg.Data)
		})
		if err != nil {
			panic(fmt.Sprintf("failed to subscribe to %s: %s", recipientSubj, err))
		}
	}

	// subscribe to broadcast messages
	{
		_, err := net.conn.Subscribe(net.broadcastSubject, func(msg *nats.Msg) {
			networkDevice.Receive(msg.Data)
		})
		if err != nil {
			panic(fmt.Sprintf("failed to subscribe to broadcast subject: %s", err))
		}
	}
}

// TODO: add retries
func (net *NatsNetwork) Broadcast(msg []byte) {
	if err := net.conn.Publish(net.broadcastSubject, msg); err != nil {
		log.Printf("failed to broadcast message: %s", err)
	}
}

// TODO: add retries
func (net *NatsNetwork) Send(id string, msg []byte) {
	recipientSubj := fmt.Sprintf("%s.%s", net.unicastPrefix, id)
	if err := net.conn.Publish(recipientSubj, msg); err != nil {
		log.Printf("failed to send message: %s", err)
	}
}
