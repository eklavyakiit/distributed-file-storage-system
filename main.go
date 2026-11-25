package main

import (
	"flag"
	"log"
	"net/http"
	"strings"
	"time"
)

func main() {
	tcp := flag.String("tcp", "127.0.0.1:3000", "TCP address")
	httpAddr := flag.String("http", "127.0.0.1:8080", "HTTP address")
	dir := flag.String("data", "data", "local storage directory")
	peers := flag.String("peers", "", "comma-separated bootstrap addresses")
	flag.Parse()
	n, e := NewNode(*dir, *tcp, strings.Split(*peers, ","))
	if e != nil {
		log.Fatal(e)
	}
	if e = n.transport.ListenAndAccept(); e != nil {
		log.Fatal(e)
	}
	defer n.transport.Close()
	log.Printf("TCP %s, HTTP %s", n.transport.Addr(), *httpAddr)
	server := &http.Server{Addr: *httpAddr, Handler: n.API(), ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 30 * time.Second}
	log.Fatal(server.ListenAndServe())
}
