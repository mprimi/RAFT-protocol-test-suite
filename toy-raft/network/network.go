package network

type NetworkDevice interface {
	Receive(msg []byte)
}

type Network interface {
	Broadcast(msg []byte)
	Send(id string, msg []byte)
	RegisterNode(id string, networkDevice NetworkDevice)
}

