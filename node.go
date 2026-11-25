package main

import (
	"errors"
	"github.com/eklavyakiit/distributed-file-storage-system/p2p"
	"strings"
	"sync"
)

type Node struct {
	store     *Store
	transport *p2p.TCPTransport
	peers     []string
}

func NewNode(dir, addr string, peers []string) (*Node, error) {
	s, e := NewStore(dir)
	if e != nil {
		return nil, e
	}
	n := &Node{store: s, transport: p2p.NewTCPTransport(addr)}
	seen := map[string]bool{}
	for _, p := range peers {
		p = strings.TrimSpace(p)
		if p != "" && !seen[p] {
			n.peers = append(n.peers, p)
			seen[p] = true
		}
	}
	n.transport.Handler = n.handle
	return n, nil
}
func (n *Node) handle(m p2p.Message) p2p.Message {
	switch m.Op {
	case "ping":
		return p2p.Message{Op: "ok"}
	case "put":
		if e := n.store.Put(m.Hash, m.Data); e != nil {
			return p2p.Message{Op: "error", Error: "storage rejected"}
		}
		return p2p.Message{Op: "ok", Hash: m.Hash}
	case "get":
		b, e := n.store.Get(m.Hash)
		if e == nil {
			return p2p.Message{Op: "file", Hash: m.Hash, Data: b}
		}
		return p2p.Message{Op: "error", Error: "file unavailable"}
	default:
		return p2p.Message{Op: "error", Error: "unknown operation"}
	}
}
func (n *Node) Upload(b []byte) (string, int, error) {
	h := digest(b)
	if e := n.store.Put(h, b); e != nil {
		return "", 0, e
	}
	count := 1
	var mu sync.Mutex
	var wg sync.WaitGroup
	for _, addr := range n.peers {
		if addr == n.transport.Addr() {
			continue
		}
		wg.Add(1)
		go func(a string) {
			defer wg.Done()
			r, e := p2p.Request(a, p2p.Message{Op: "put", Hash: h, Data: b})
			if e == nil && r.Op == "ok" && r.Hash == h {
				mu.Lock()
				count++
				mu.Unlock()
			}
		}(addr)
	}
	wg.Wait()
	return h, count, nil
}

var errUnavailable = errors.New("file unavailable")

func (n *Node) Download(h string) ([]byte, error) {
	if !validHash(h) {
		return nil, errors.New("invalid hash")
	}
	if b, e := n.store.Get(h); e == nil {
		return b, nil
	}
	for _, a := range n.peers {
		if a == n.transport.Addr() {
			continue
		}
		r, e := p2p.Request(a, p2p.Message{Op: "get", Hash: h})
		if e == nil && r.Op == "file" && r.Hash == h && len(r.Data) <= p2p.MaxFile && digest(r.Data) == h {
			if e = n.store.Put(h, r.Data); e != nil {
				return nil, e
			}
			return r.Data, nil
		}
	}
	return nil, errUnavailable
}
