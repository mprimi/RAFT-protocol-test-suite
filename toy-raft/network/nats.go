package network

import (
	"fmt"

	"github.com/nats-io/nats.go"
)

const NatsSubjectPrefix = "RAFT"

type NatsNetwork struct {
	conn *nats.Conn

	groupId          string
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
		broadcastSubject: fmt.Sprintf("%s.%s.broadcast", NatsSubjectPrefix, groupId),
		unicastPrefix:    fmt.Sprintf("%s.%s", NatsSubjectPrefix, groupId),
	}

	return natsNetwork
}

func (net *NatsNetwork) RegisterNode(id string, networkDevice NetworkDevice) {
	net.networkDevices[id] = networkDevice
}

func (net *NatsNetwork) Broadcast(msg []byte) {
	if err := net.conn.Publish(net.broadcastSubject, msg); err != nil {
		return
	}
}

func (net *NatsNetwork) Send(id string, msg []byte) {
	recipientSubj := fmt.Sprintf("%s.%s", net.unicastPrefix, id)

}

