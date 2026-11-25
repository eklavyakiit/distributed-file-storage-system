package p2p

import (
	"fmt"
	"net"
	"sync"
)
//TCP Peer represents the remote node over a TCP established connection
type TCPPeer struct{
	//conn is underlying connection of the peer
	conn net.Conn

	//if we dial and retrieve a conn => outbound == true
	//if we accept and retrieve a conn => outbound == false
	outbound bool
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer{
	return &TCPPeer{
		conn: conn,
		outbound: outbound,
	}
}


//RWMutex stands for Read-Write Mutual Exclusion
//it allows multiple simultaenous read but no write simultaneously

type TCPTransport struct {
    listenAddr string
    listener   net.Listener
    mu         sync.RWMutex
    peers      map[net.Addr]Peer
}

/* Info about the below function
 * NewTCPTransport acts as a factory function to initialize a new TCPTransport instance.
 *
 * What is happening here:
 *   When someone wants to set up a new TCP transport, this function allocates a fresh
 *   TCPTransport object in memory and saves the given address/port (e.g., ":4000")
 *   into its `listenAddress` variable. Think of it like writing the phone number on a
 *   sticky note before picking up the phone.
 *   (Note: It simply stores the string as-is—it does not validate the port or auto-assign
 *   a free port if left blank.)
 *
 * What is NOT happening yet:
 *   The server is NOT actually listening on that port yet. No network socket is open,
 *   and incoming connections cannot be accepted yet. You must call the listen function
 *   next to actually bind the port and start accepting traffic.
 */
func NewTCPTransport(listenAddr string) *TCPTransport{ 
	return &TCPTransport{
		listenAddr: listenAddr,
	}
}
//lets write a function where server start listening on that port number
//It only listens (opens the port)—it doesn't have the loop to accept incoming connections yet.
//just opening the gate for the visitors

func (t *TCPTransport) ListenAndAccept() error{
	var err error
	t.listener, err = net.Listen("tcp", t.listenAddr) //Go asks the operating system to create a TCP listening socket and bind it to port 4000
	if err !=nil{
		return err
	}
	go t.startAcceptLoop()
	return nil
}

//the loop to recieve incoming messages from peers
//the standing at the gate and shaking hands with the visitors

func (t *TCPTransport) startAcceptLoop(){
	for{
		conn, err := t.listener.Accept()
		if err !=nil{
			fmt.Printf("TCP accept error: %s\n", err)
			continue
		}
		go t.handleConn(conn)
	}
}
func (t *TCPTransport) handleConn(conn net.Conn){
	peer := NewTCPPeer(conn, true)
	fmt.Printf("new incoming connection: %+v\n", peer)
}



