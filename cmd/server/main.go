package main

import (
	"flag"
	"log"

	"multiplayer-session-service/internal"
)

func main() {
	address := flag.String("addr", "127.0.0.1:7700", "listen address")
	statePath := flag.String("state", "state.json", "state file path")
	flag.Parse()
	engine, err := internal.NewEngine(internal.NewStore(*statePath), nil)
	if err != nil {
		log.Fatal(err)
	}
	server, err := internal.NewServer(engine, *address)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("listening on %s using %s", server.Address(), *statePath)
	server.Serve()
}
