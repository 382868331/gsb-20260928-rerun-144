package main

import (
	"bufio"
	"encoding/json"
	"net"
	"sync"
)

type clientConn struct {
	session  string
	playerID string
	conn     net.Conn
	mu       sync.Mutex
	enc      *json.Encoder
	server   *Server
}

func (c *clientConn) send(m OutMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	_ = c.enc.Encode(m)
}

func serveConn(srv *Server, nc net.Conn, session string) {
	c := &clientConn{session: session, conn: nc, enc: json.NewEncoder(nc), server: srv}
	defer nc.Close()
	sc := bufio.NewScanner(nc)
	sc.Buffer(make([]byte, 64*1024), 64*1024)
	for sc.Scan() {
		var m InMessage
		if err := json.Unmarshal(sc.Bytes(), &m); err != nil {
			c.send(OutMessage{Type: "error", Reason: "bad_json"})
			continue
		}
		srv.events <- event{kind: "msg", conn: c, msg: m}
	}
	srv.events <- event{kind: "disconnect", conn: c}
}

func ListenAndServe(srv *Server, addr string) (net.Listener, error) {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, err
	}
	go func() {
		for {
			nc, err := ln.Accept()
			if err != nil {
				return
			}
			srv.events <- event{kind: "tick"} // keep loop warm; harmless
			go serveConn(srv, nc, srv.newSession())
		}
	}()
	return ln, nil
}
