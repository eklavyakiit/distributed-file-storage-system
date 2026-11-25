package main

import (
	"bytes"
	"encoding/json"
	"net/http/httptest"
	"testing"
)

func TestVisualDemo(t *testing.T) {
	d, e := NewDemo(t.TempDir())
	if e != nil {
		t.Fatal(e)
	}
	defer d.Close()
	api := d.API()
	call := func(method, path string, b []byte) *httptest.ResponseRecorder {
		r := httptest.NewRecorder()
		api.ServeHTTP(r, httptest.NewRequest(method, path, bytes.NewReader(b)))
		return r
	}
	if r := call("GET", "/", nil); r.Code != 200 || !bytes.Contains(r.Body.Bytes(), []byte("Distributed Storage Lab")) {
		t.Fatal(r.Code)
	}
	if r := call("POST", "/demo/C/toggle", nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
	b := []byte("visual demo")
	r := call("POST", "/demo/A/files", b)
	var x struct {
		Hash   string
		Copies int
	}
	if e = json.Unmarshal(r.Body.Bytes(), &x); e != nil || r.Code != 201 || x.Copies != 2 {
		t.Fatal(r.Code, x, e)
	}
	if r = call("POST", "/demo/A/toggle", nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
	if r = call("POST", "/demo/C/toggle", nil); r.Code != 200 {
		t.Fatal(r.Code)
	}
	if r = call("GET", "/demo/C/files/"+x.Hash, nil); r.Code != 200 || !bytes.Equal(r.Body.Bytes(), b) {
		t.Fatal(r.Code, r.Body.String())
	}
	events := d.events.Since(0)
	seen := map[string]bool{}
	for _, e := range events {
		seen[e.Stage] = true
	}
	for _, s := range []string{"hash", "store", "connect", "handshake", "encode", "decode", "send", "replicate", "retrieve", "offline", "online"} {
		if !seen[s] {
			t.Error("missing event", s)
		}
	}
}
