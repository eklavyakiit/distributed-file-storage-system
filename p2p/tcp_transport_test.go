package p2p

import "testing"

func TestTCPTransport(t *testing.T) {
	tr := NewTCPTransport("127.0.0.1:0")
	tr.Handler = func(m Message) Message { return m }
	if err := tr.ListenAndAccept(); err != nil {
		t.Fatal(err)
	}
	defer tr.Close()
	m, err := Request(tr.Addr(), Message{Op: "ping"})
	if err != nil || m.Op != "ping" {
		t.Fatalf("%+v %v", m, err)
	}
}
