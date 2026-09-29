package main

import (
	"flag"
	"fmt"
	"log"
	"sync/atomic"
	"time"
)

var sessionSeq atomic.Int64

func (s *Server) newSession() string {
	return fmt.Sprintf("sess-%d-%d", time.Now().UnixNano(), sessionSeq.Add(1))
}

func main() {
	addr := flag.String("addr", "127.0.0.1:7777", "listen address")
	data := flag.String("data", "data", "state directory")
	roomSize := flag.Int("room-size", 2, "players per room")
	confirmTimeout := flag.Duration("confirm-timeout", 5*time.Second, "match confirm deadline")
	flag.Parse()

	srv := NewServer(NewStore(*data), *roomSize, *confirmTimeout)
	if err := srv.checkInvariants(); err != nil {
		log.Fatalf("corrupt persisted state: %v", err)
	}
	go srv.Run()
	ln, err := ListenAndServe(srv, *addr)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("sessionorg listening on %s (room-size=%d)", ln.Addr(), *roomSize)
	select {}
}
