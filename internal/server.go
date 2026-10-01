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
	engine       *Engine
	listener     net.Listener
	ch           chan ClientEvent
	wg           sync.WaitGroup
	connWg       sync.WaitGroup
	mu           sync.Mutex
	clients      map[string]*serverClient
	done         chan struct{}
	closeOnce    sync.Once
	tickInterval time.Duration
}

type serverClient struct {
	conn net.Conn
	ch   chan Response
}

func NewServer(engine *Engine, address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	return newServer(engine, listener, 50*time.Millisecond), nil
}

func newServer(engine *Engine, listener net.Listener, tickInterval time.Duration) *Server {
	if tickInterval <= 0 {
		tickInterval = 50 * time.Millisecond
	}
	s := &Server{
		engine: engine, listener: listener, ch: make(chan ClientEvent, 1024),
		clients: map[string]*serverClient{},
		done:    make(chan struct{}), tickInterval: tickInterval,
	}
	s.wg.Add(2)
	go s.loop()
	go s.tickLoop()
	return s
}

func (s *Server) Address() string {
	if s.listener == nil {
		return ""
	}
	return s.listener.Addr().String()
}

func (s *Server) Close() error {
	var err error
	s.closeOnce.Do(func() {
		close(s.done)
		if s.listener != nil {
			err = s.listener.Close()
		}
		s.mu.Lock()
		clients := make([]*serverClient, 0, len(s.clients))
		for _, client := range s.clients {
			clients = append(clients, client)
		}
		s.mu.Unlock()
		for _, client := range clients {
			_ = client.conn.Close()
		}
	})
	s.wg.Wait()
	s.connWg.Wait()
	return err
}

func (s *Server) loop() {
	defer s.wg.Done()
	for {
		select {
		case <-s.done:
			return
		case event := <-s.ch:
			s.handleEvent(event)
		}
	}
}

func (s *Server) handleEvent(event ClientEvent) {
	switch event.Kind {
	case "hello":
		sessionID, resp := s.engine.Hello(event.PlayerID)
		s.attachConn(sessionID, event.Conn.(*serverClient))
		event.Resp <- resp
	case "request":
		resp := s.engine.Handle(event.Request, event.SessionID)
		s.sendToSession(event.SessionID, resp)
		s.sendNotifications(event.SessionID)
	case "disconnect":
		s.engine.Disconnect(event.SessionID)
		s.unregister(event.SessionID)
		s.sendNotifications("")
	case "tick":
		s.engine.Tick()
		s.sendNotifications("")
	}
}

func (s *Server) tickLoop() {
	defer s.wg.Done()
	ticker := time.NewTicker(s.tickInterval)
	defer ticker.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-ticker.C:
			select {
			case s.ch <- ClientEvent{Kind: "tick"}:
			case <-s.done:
				return
			}
		}
	}
}

func (s *Server) unregister(sessionID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.clients, sessionID)
}

func (s *Server) sendToSession(sessionID string, resp Response) {
	s.mu.Lock()
	client := s.clients[sessionID]
	s.mu.Unlock()
	if client != nil {
		select {
		case client.ch <- resp:
		default:
			_ = client.conn.Close()
		}
	}
}

func (s *Server) sendNotifications(skipSession string) {
	for player, state := range s.engine.Notifications() {
		sessionID := s.engine.CurrentSession(player)
		if sessionID == "" || sessionID == skipSession {
			continue
		}
		s.sendToSession(sessionID, state)
	}
}

func (s *Server) Serve() {
	if s.listener == nil {
		return
	}
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			return
		}
		s.ServeConn(conn)
	}
}

func (s *Server) ServeConn(conn net.Conn) {
	s.connWg.Add(1)
	go s.serveConn(conn)
}

func (s *Server) serveConn(conn net.Conn) {
	defer s.connWg.Done()
	defer conn.Close()
	client := &serverClient{conn: conn, ch: make(chan Response, 64)}
	writerDone := make(chan struct{})
	stopWriter := make(chan struct{})
	var writerOnce sync.Once
	stop := func() { writerOnce.Do(func() { close(stopWriter) }) }
	defer stop()
	go func() {
		defer close(writerDone)
		for {
			select {
			case <-stopWriter:
				return
			case resp := <-client.ch:
				if s.encode(conn, resp) != nil {
					_ = conn.Close()
					return
				}
			}
		}
	}()
	defer func() { _ = conn.Close(); stop(); <-writerDone }()
	reader := bufio.NewReader(conn)
	var sessionID string
	defer func() {
		if sessionID == "" {
			return
		}
		event := ClientEvent{Kind: "disconnect", SessionID: sessionID}
		go func() {
			select {
			case s.ch <- event:
			case <-s.done:
			}
		}()
	}()
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var req Request
			if json.Unmarshal(line, &req) != nil {
				s.sendToConn(client, Response{Type: "error", OK: false, Reason: "bad_json"})
			} else if req.Type == "hello" {
				event := ClientEvent{Kind: "hello", PlayerID: req.PlayerID, Conn: client, Resp: make(chan Response, 1)}
				if !s.submit(event) {
					return
				}
				select {
				case resp := <-event.Resp:
					sessionID = resp.SessionID
					s.sendToConn(client, resp)
				case <-s.done:
					return
				}
			} else if req.SessionID != sessionID {
				s.sendToConn(client, Response{Type: "error", OK: false, Reason: "stale_session"})
			} else if !s.submit(ClientEvent{Kind: "request", SessionID: sessionID, Request: req}) {
				return
			}
		}
		if err != nil {
			if !errors.Is(err, io.EOF) {
				select {
				case <-s.done:
				default:
					log.Printf("connection: %v", err)
				}
			}
			return
		}
	}
}

func (s *Server) submit(event ClientEvent) bool {
	select {
	case s.ch <- event:
		return true
	case <-s.done:
		return false
	}
}

func (s *Server) attachConn(sessionID string, client *serverClient) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.clients[sessionID] = client
}

func (s *Server) sendToConn(client *serverClient, resp Response) {
	select {
	case client.ch <- resp:
	default:
		_ = client.conn.Close()
	}
}

func (s *Server) encode(conn net.Conn, resp Response) error {
	data, err := json.Marshal(resp)
	if err != nil {
		return err
	}
	data = append(data, '\n')
	_, err = conn.Write(data)
	return err
}
