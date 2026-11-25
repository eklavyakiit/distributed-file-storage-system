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
	demo := flag.Bool("demo", false, "run three-node visual lab")
	flag.Parse()
	var handler http.Handler
	if *demo {
		d, e := NewDemo(*dir)
		if e != nil {
			log.Fatal(e)
		}
		defer d.Close()
		handler = d.API()
	} else {
		n, e := NewNode(*dir, *tcp, strings.Split(*peers, ","))
		if e != nil {
			log.Fatal(e)
		}
		if e = n.transport.ListenAndAccept(); e != nil {
			log.Fatal(e)
		}
		defer n.transport.Close()
		handler = n.API()
	}
	log.Printf("Dashboard: http://%s", *httpAddr)
	server := &http.Server{Addr: *httpAddr, Handler: handler, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 30 * time.Second, WriteTimeout: 60 * time.Second, IdleTimeout: 30 * time.Second}
	log.Fatal(server.ListenAndServe())
}
