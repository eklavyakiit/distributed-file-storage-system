package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/eklavyakiit/distributed-file-storage-system/p2p"
	"io"
	"net/http"
)

func respond(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}
func (n *Node) API() http.Handler {
	m := http.NewServeMux()
	m.HandleFunc("POST /files", func(w http.ResponseWriter, r *http.Request) {
		b, e := io.ReadAll(http.MaxBytesReader(w, r.Body, p2p.MaxFile))
		if e != nil {
			var limit *http.MaxBytesError
			if errors.As(e, &limit) {
				http.Error(w, "file too large", 413)
			} else {
				http.Error(w, "invalid body", 400)
			}
			return
		}
		h, c, e := n.Upload(b)
		if e != nil {
			http.Error(w, "storage failed", 500)
			return
		}
		respond(w, 201, map[string]any{"hash": h, "copies": c})
	})
	m.HandleFunc("GET /files/{hash}", func(w http.ResponseWriter, r *http.Request) {
		h := r.PathValue("hash")
		if !validHash(h) {
			http.Error(w, "invalid hash", 400)
			return
		}
		b, e := n.Download(h)
		if e != nil {
			if errors.Is(e, errUnavailable) {
				http.Error(w, "file unavailable", 404)
			} else {
				http.Error(w, "storage failed", 500)
			}
			return
		}
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("X-Content-Type-Options", "nosniff")
		w.Header().Set("Content-Disposition", fmt.Sprintf("attachment; filename=%s", h))
		w.Write(b)
	})
	m.HandleFunc("GET /status", func(w http.ResponseWriter, r *http.Request) {
		files, e := n.store.List()
		if e != nil {
			http.Error(w, "storage failed", 500)
			return
		}
		respond(w, 200, map[string]any{"address": n.transport.Addr(), "peers": n.peers, "files": files})
	})
	m.HandleFunc("GET /{$}", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		io.WriteString(w, dashboard)
	})
	return m
}

const dashboard = `<!doctype html><html lang="en"><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1"><title>Distributed storage</title><style>body{font:16px system-ui;max-width:760px;margin:4rem auto;padding:0 1rem;background:#f3f6fa;color:#182b40}section{background:white;padding:1.5rem;margin:1rem 0;border-radius:12px}button{background:#125b71;color:white;border:0;padding:.7rem 1rem;border-radius:6px}input{max-width:100%;padding:.5rem}pre{white-space:pre-wrap;overflow-wrap:anywhere}a{color:#125b71}</style><h1>Distributed storage</h1><p>Upload a file, share its SHA-256 address, and retrieve it from any configured node.</p><section><h2>Upload</h2><input type="file" id="file"><button id="upload">Store file</button><pre id="result" aria-live="polite"></pre></section><section><h2>Download</h2><input id="hash" placeholder="SHA-256 hash" size="50"><button id="download">Download</button></section><section><h2>Node status</h2><button id="refresh">Refresh</button><pre id="status"></pre></section><script>const el=id=>document.getElementById(id);async function refresh(){try{const r=await fetch('/status');el('status').textContent=JSON.stringify(await r.json(),null,2)}catch(e){el('status').textContent=String(e)}}el('refresh').onclick=refresh;el('upload').onclick=async()=>{try{const f=el('file').files[0];if(!f)throw Error('Select a file');const r=await fetch('/files',{method:'POST',body:f});if(!r.ok)throw Error(await r.text());const x=await r.json();el('result').textContent=JSON.stringify(x,null,2);el('hash').value=x.hash;refresh()}catch(e){el('result').textContent=String(e)}};el('download').onclick=()=>{const h=el('hash').value.trim();if(/^[a-f0-9]{64}$/.test(h))location.href='/files/'+h;else el('result').textContent='Enter a valid SHA-256 hash'};refresh()</script></html>`
