package p2p

//peer is and interface that represents a remote node
type Peer interface{}

//transport is an interface that handles the communication
//between the nodes in the network. this can be of any form
// (TCP, UDP, websockets etc.)
type Transport interface{
	ListenAndAccept() error
}