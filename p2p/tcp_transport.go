package p2p

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"net"
	"sync"
	"time"
)

const MaxFile = 8 << 20
const maxFrame = 12 << 20
const version = "dfs/1"

type Message struct {
	Op    string `json:"op"`
	Hash  string `json:"hash,omitempty"`
	Data  []byte `json:"data,omitempty"`
	Error string `json:"error,omitempty"`
}

type TCPPeer struct {
	conn     net.Conn
	outbound bool
	trace    func(string, string)
}

func NewTCPPeer(conn net.Conn, outbound bool) *TCPPeer {
	return &TCPPeer{conn: conn, outbound: outbound}
}
func (p *TCPPeer) emit(stage, detail string) {
	if p.trace != nil {
		p.trace(stage, detail)
	}
}
func (p *TCPPeer) Write(m Message) error {
	b, e := json.Marshal(m)
	if e != nil {
		return e
	}
	if len(b) > maxFrame {
		return errors.New("frame too large")
	}
	var h [4]byte
	binary.BigEndian.PutUint32(h[:], uint32(len(b)))
	p.emit("encode", m.Op+" JSON + length prefix")
	if _, e = io.Copy(p.conn, bytes.NewReader(h[:])); e != nil {
		return e
	}
	_, e = io.Copy(p.conn, bytes.NewReader(b))
	if e == nil {
		p.emit("send", m.Op+" frame sent")
	}
	return e
}
func (p *TCPPeer) Read() (m Message, e error) {
	var h [4]byte
	if _, e = io.ReadFull(p.conn, h[:]); e != nil {
		return
	}
	n := binary.BigEndian.Uint32(h[:])
	if n == 0 || n > maxFrame {
		return m, errors.New("invalid frame size")
	}
	b := make([]byte, n)
	if _, e = io.ReadFull(p.conn, b); e != nil {
		return
	}
	e = json.Unmarshal(b, &m)
	if e == nil {
		p.emit("decode", m.Op+" frame decoded")
	}
	return
}
func (p *TCPPeer) handshake() error {
	if p.outbound {
		if e := p.Write(Message{Op: version}); e != nil {
			return e
		}
	}
	m, e := p.Read()
	if e != nil {
		return e
	}
	if m.Op != version {
		return errors.New("incompatible protocol")
	}
	p.emit("handshake", "Protocol dfs/1 accepted")
	if !p.outbound {
		return p.Write(Message{Op: version})
	}
	return nil
}

type TCPTransport struct {
	listenAddr string
	listener   net.Listener
	mu         sync.RWMutex
	peers      map[net.Conn]struct{}
	closed     bool
	Handler    func(Message) Message
	Trace      func(string, string)
}

func NewTCPTransport(addr string) *TCPTransport {
	return &TCPTransport{listenAddr: addr, peers: make(map[net.Conn]struct{})}
}
func (t *TCPTransport) ListenAndAccept() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.listener != nil {
		return errors.New("transport already started or closed")
	}
	l, e := net.Listen("tcp", t.listenAddr)
	if e != nil {
		return e
	}
	t.listener = l
	go t.startAcceptLoop(l)
	return nil
}
func (t *TCPTransport) Addr() string {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if t.listener == nil {
		return t.listenAddr
	}
	return t.listener.Addr().String()
}
func (t *TCPTransport) startAcceptLoop(l net.Listener) {
	for {
		c, e := l.Accept()
		if e != nil {
			return
		}
		t.mu.Lock()
		if t.closed || len(t.peers) >= 32 {
			t.mu.Unlock()
			c.Close()
			continue
		}
		t.peers[c] = struct{}{}
		t.mu.Unlock()
		go t.handleConn(c)
	}
}
func (t *TCPTransport) handleConn(c net.Conn) {
	defer func() { c.Close(); t.mu.Lock(); delete(t.peers, c); t.mu.Unlock() }()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	p := NewTCPPeer(c, false)
	p.trace = t.Trace
	p.emit("connect", "Inbound TCP connection accepted")
	if p.handshake() != nil {
		return
	}
	m, e := p.Read()
	if e != nil {
		return
	}
	if t.Handler == nil {
		return
	}
	_ = p.Write(t.Handler(m))
}
func (t *TCPTransport) Close() error {
	t.mu.Lock()
	defer t.mu.Unlock()
	t.closed = true
	for c := range t.peers {
		c.Close()
	}
	if t.listener != nil {
		return t.listener.Close()
	}
	return nil
}
func Request(addr string, m Message) (Message, error) { return RequestTrace(addr, m, nil) }
func RequestTrace(addr string, m Message, trace func(string, string)) (Message, error) {
	c, e := net.DialTimeout("tcp", addr, 2*time.Second)
	if e != nil {
		return Message{}, e
	}
	defer c.Close()
	c.SetDeadline(time.Now().Add(5 * time.Second))
	p := NewTCPPeer(c, true)
	p.trace = trace
	p.emit("connect", "TCP connected to "+addr)
	if e = p.handshake(); e != nil {
		return Message{}, e
	}
	if e = p.Write(m); e != nil {
		return Message{}, e
	}
	return p.Read()
}

func (t *TCPTransport) Start(l net.Listener) error {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.closed || t.listener != nil {
		return errors.New("already started")
	}
	t.listener = l
	go t.startAcceptLoop(l)
	return nil
}
