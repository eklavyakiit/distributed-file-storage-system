package main

import (
	"fmt"
	"github.com/eklavyakiit/distributed-file-storage-system/p2p"
	"net"
	"net/http"
	"path/filepath"
	"sync"
	"time"
)

type Event struct {
	ID     uint64 `json:"id"`
	Time   string `json:"time"`
	Node   string `json:"node"`
	Stage  string `json:"stage"`
	Detail string `json:"detail"`
}
type EventLog struct {
	mu     sync.Mutex
	next   uint64
	events []Event
}

func (l *EventLog) Add(node, stage, detail string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.next++
	l.events = append(l.events, Event{l.next, time.Now().Format("15:04:05.000"), node, stage, detail})
	if len(l.events) > 250 {
		l.events = append([]Event(nil), l.events[len(l.events)-250:]...)
	}
}
func (l *EventLog) Since(id uint64) []Event {
	l.mu.Lock()
	defer l.mu.Unlock()
	out := []Event{}
	for _, e := range l.events {
		if e.ID > id {
			out = append(out, e)
		}
	}
	return out
}

type Demo struct {
	mu        sync.RWMutex
	nodes     [3]*Node
	addresses [3]string
	online    [3]bool
	events    EventLog
}

func NewDemo(dir string) (*Demo, error) {
	d := &Demo{}
	var listeners [3]net.Listener
	for i := range listeners {
		l, e := net.Listen("tcp", "127.0.0.1:0")
		if e != nil {
			for _, old := range listeners {
				if old != nil {
					old.Close()
				}
			}
			return nil, e
		}
		listeners[i] = l
		d.addresses[i] = l.Addr().String()
	}
	for i, l := range listeners {
		peers := []string{}
		for j, a := range d.addresses {
			if i != j {
				peers = append(peers, a)
			}
		}
		n, e := NewNode(filepath.Join(dir, fmt.Sprintf("node-%c", 'a'+i)), d.addresses[i], peers)
		if e != nil {
			for _, old := range listeners {
				old.Close()
			}
			return nil, e
		}
		d.nodes[i] = n
		n.trace = func(stage, detail string) { d.events.Add(string(rune('A'+i)), stage, detail) }
		n.transport.Trace = n.trace
		if e = n.transport.Start(l); e != nil {
			return nil, e
		}
		d.online[i] = true
	}
	return d, nil
}
func demoIndex(id string) int {
	if len(id) != 1 || id[0] < 'A' || id[0] > 'C' {
		return -1
	}
	return int(id[0] - 'A')
}
func (d *Demo) API() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.Write([]byte(demoHTML))
	})
	m.HandleFunc("GET /demo/status", func(w http.ResponseWriter, r *http.Request) {
		d.mu.RLock()
		defer d.mu.RUnlock()
		out := []map[string]any{}
		for i, n := range d.nodes {
			files, _ := n.store.List()
			out = append(out, map[string]any{"id": string(rune('A' + i)), "address": d.addresses[i], "online": d.online[i], "files": files})
		}
		respond(w, 200, out)
	})
	m.HandleFunc("GET /demo/events", func(w http.ResponseWriter, r *http.Request) {
		var id uint64
		fmt.Sscan(r.URL.Query().Get("after"), &id)
		respond(w, 200, d.events.Since(id))
	})
	m.HandleFunc("POST /demo/{id}/toggle", func(w http.ResponseWriter, r *http.Request) {
		i := demoIndex(r.PathValue("id"))
		if i < 0 {
			http.NotFound(w, r)
			return
		}
		d.mu.Lock()
		defer d.mu.Unlock()
		n := d.nodes[i]
		if d.online[i] {
			n.transport.Close()
			d.online[i] = false
			n.emit("offline", "TCP listener closed")
		} else {
			t := p2p.NewTCPTransport(d.addresses[i])
			t.Handler = n.handle
			t.Trace = n.trace
			if e := t.ListenAndAccept(); e != nil {
				http.Error(w, "could not restart listener", 503)
				return
			}
			n.transport = t
			d.online[i] = true
			n.emit("online", "TCP listener restarted")
		}
		respond(w, 200, map[string]bool{"online": d.online[i]})
	})
	m.HandleFunc("/demo/{id}/", func(w http.ResponseWriter, r *http.Request) {
		i := demoIndex(r.PathValue("id"))
		if i < 0 {
			http.NotFound(w, r)
			return
		}
		d.mu.RLock()
		defer d.mu.RUnlock()
		if !d.online[i] {
			http.Error(w, "node offline", 503)
			return
		}
		http.StripPrefix("/demo/"+r.PathValue("id"), d.nodes[i].API()).ServeHTTP(w, r)
	})
	return m
}
func (d *Demo) Close() {
	d.mu.Lock()
	defer d.mu.Unlock()
	for _, n := range d.nodes {
		n.transport.Close()
	}
}
