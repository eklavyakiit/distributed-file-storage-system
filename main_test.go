package main

import (
	"bytes"
	"encoding/json"
	"github.com/eklavyakiit/distributed-file-storage-system/p2p"
	"net/http/httptest"
	"sync"
	"testing"
)

func start(t *testing.T, peers ...string) *Node {
	t.Helper()
	n, e := NewNode(t.TempDir(), "127.0.0.1:0", peers)
	if e != nil {
		t.Fatal(e)
	}
	if e = n.transport.ListenAndAccept(); e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { n.transport.Close() })
	return n
}
func TestReplicationAndOfflineRetrieval(t *testing.T) {
	b := start(t)
	a := start(t, b.transport.Addr())
	data := []byte("distributed content")
	h, c, e := a.Upload(data)
	if e != nil || c != 2 {
		t.Fatalf("upload %s %d %v", h, c, e)
	}
	if got, e := b.store.Get(h); e != nil || !bytes.Equal(got, data) {
		t.Fatalf("replica %q %v", got, e)
	}
	a.transport.Close()
	reader := start(t, a.transport.Addr(), b.transport.Addr())
	if got, e := reader.Download(h); e != nil || !bytes.Equal(got, data) {
		t.Fatalf("remote %q %v", got, e)
	}
	b.transport.Close()
	if got, e := reader.Download(h); e != nil || !bytes.Equal(got, data) {
		t.Fatalf("cached %q %v", got, e)
	}
}
func TestConcurrentStorage(t *testing.T) {
	n := start(t)
	data := []byte("same")
	h := digest(data)
	var wg sync.WaitGroup
	for i := 0; i < 12; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if e := n.store.Put(h, data); e != nil {
				t.Error(e)
			}
			if b, e := n.store.Get(h); e != nil || !bytes.Equal(b, data) {
				t.Errorf("get %q %v", b, e)
			}
		}()
	}
	wg.Wait()
	files, e := n.store.List()
	if e != nil || len(files) != 1 {
		t.Fatalf("%v %v", files, e)
	}
	if e = n.store.Put(h, []byte("wrong")); e == nil {
		t.Fatal("accepted corrupted data")
	}
	if _, e = n.store.Get("../test"); e == nil {
		t.Fatal("accepted invalid hash")
	}
}
func TestHTTP(t *testing.T) {
	n := start(t)
	api := n.API()
	r := httptest.NewRecorder()
	api.ServeHTTP(r, httptest.NewRequest("POST", "/files", bytes.NewBufferString("hello")))
	if r.Code != 201 {
		t.Fatal(r.Code, r.Body.String())
	}
	var out struct {
		Hash   string
		Copies int
	}
	if e := json.Unmarshal(r.Body.Bytes(), &out); e != nil || out.Hash != digest([]byte("hello")) || out.Copies != 1 {
		t.Fatal(out, e)
	}
	r = httptest.NewRecorder()
	api.ServeHTTP(r, httptest.NewRequest("GET", "/files/"+out.Hash, nil))
	if r.Code != 200 || r.Body.String() != "hello" {
		t.Fatal(r.Code, r.Body.String())
	}
	r = httptest.NewRecorder()
	api.ServeHTTP(r, httptest.NewRequest("GET", "/status", nil))
	if r.Code != 200 || !bytes.Contains(r.Body.Bytes(), []byte(out.Hash)) {
		t.Fatal(r.Code, r.Body.String())
	}
	r = httptest.NewRecorder()
	api.ServeHTTP(r, httptest.NewRequest("POST", "/files", bytes.NewReader(make([]byte, p2p.MaxFile+1))))
	if r.Code != 413 {
		t.Fatal(r.Code)
	}
}
