package internal

import (
	"bufio"
	"encoding/json"
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"
)

type Server struct {
	engine    *Engine
	listener  net.Listener
	ch        chan ClientEvent
	wg        sync.WaitGroup
	mu        sync.Mutex
	conns     map[string]net.Conn
	done      chan struct{}
	closeOnce sync.Once
}

func NewServer(engine *Engine, address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &Server{engine: engine, listener: listener, ch: make(chan ClientEvent, 1024), conns: map[string]net.Conn{}, done: make(chan struct{})}
	s.wg.Add(2)
	go s.loop()
	go s.tickLoop()
	return s, nil
}

func (s *Server) Address() string { return s.listener.Addr().String() }

func (s *Server) Close() error {
	s.closeOnce.Do(func() { close(s.done); s.listener.Close() })
	s.wg.Wait()
	return nil
}

func (s *Server) loop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case event := <-s.ch:
			switch event.Kind {
			case "hello":
				sessionID, resp := s.engine.Hello(event.PlayerID)
				s.attachConn(sessionID, event.Conn.(net.Conn))
				event.Resp <- resp
			case "request":
				resp := s.engine.Handle(event.Request, event.SessionID)
				s.sendToSession(event.SessionID, resp)
				s.broadcastRoomStates()
			case "disconnect":
				s.engine.Disconnect(event.SessionID)
				s.unregister(event.SessionID)
				s.broadcastRoomStates()
			case "tick":
				s.engine.Tick()
				s.broadcastRoomStates()
			}
		}
	}
}

func (s *Server) tickLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			s.ch <- ClientEvent{Kind: "tick"}
		}
	}
}

func (s *Server) unregister(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.conns, sessionID)
}

func (s *Server) sendToSession(sessionID string, resp Response) {
	s.mu.Lock()
	conn := s.conns[sessionID]
	s.mu.Unlock()
	if conn != nil {
		_ = encode(conn, resp)
	}
}

func (s *Server) sendToPlayer(player string, resp Response) {
	sessionID := s.engine.CurrentSession(player)
	if sessionID != "" {
		s.sendToSession(sessionID, resp)
	}
}

func (s *Server) broadcastRoomStates() {
	for player, state := range s.engine.PublicRoomStates() {
		s.sendToPlayer(player, state)
	}
}

func (s *Server) Serve() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.wg.Add(1)
		go s.serveConn(conn)
	}
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.wg.Done()
	defer conn.Close()
	reader := bufio.NewReader(conn)
	var sessionID string
	defer func() {
		if sessionID != "" {
			s.ch <- ClientEvent{Kind: "disconnect", SessionID: sessionID}
		}
	}()
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var req Request
			if json.Unmarshal(line, &req) != nil {
				_ = encode(conn, Response{Type: "error", OK: false, Reason: "bad_json"})
			} else if req.Type == "hello" {
				event := ClientEvent{Kind: "hello", PlayerID: req.PlayerID, Conn: conn, Resp: make(chan Response, 1)}
				s.ch <- event
				resp := <-event.Resp
				sessionID = resp.SessionID
				_ = encode(conn, resp)
			} else if req.SessionID != sessionID {
				_ = encode(conn, Response{Type: "error", OK: false, Reason: "stale_session"})
			} else {
				s.ch <- ClientEvent{Kind: "request", SessionID: sessionID, Request: req}
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				log.Printf("connection: %v", err)
			}
			return
		}
	}
}

func (s *Server) attachConn(sessionID string, conn net.Conn) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.conns[sessionID] = conn
}

func encode(conn net.Conn, resp Response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}
