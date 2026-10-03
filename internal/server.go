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

type connWriter struct {
	conn net.Conn
	mu   sync.Mutex
}

func (w *connWriter) write(resp Response) {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	_ = encode(w.conn, resp)
}

func (w *connWriter) close() {
	w.mu.Lock()
	defer w.mu.Unlock()
	_ = w.conn.Close()
}

type Server struct {
	engine     *Engine
	listener   net.Listener
	ch         chan ClientEvent
	wg         sync.WaitGroup
	mu         sync.Mutex
	conns      map[string]*connWriter
	done       chan struct{}
	closed     chan struct{}
	closeOnce  sync.Once
	finishOnce sync.Once
}

func NewServer(engine *Engine, address string) (*Server, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}
	s := &Server{engine: engine, listener: listener, ch: make(chan ClientEvent, 1024), conns: map[string]*connWriter{}, done: make(chan struct{}), closed: make(chan struct{})}
	s.wg.Add(2)
	go s.loop()
	go s.tickLoop()
	return s, nil
}

func (s *Server) Address() string { return s.listener.Addr().String() }

func (s *Server) Close() error {
	s.closeOnce.Do(func() {
		go func() {
			select {
			case <-s.closed:
			case s.ch <- ClientEvent{Kind: "shutdown"}:
			case <-time.After(time.Second):
				_ = s.listener.Close()
				s.closeAllConns()
				s.finish()
			}
		}()
	})
	<-s.closed
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
				s.broadcastRoomStates()
			case "request":
				resp := s.engine.Handle(event.Request, event.SessionID)
				s.sendToSession(event.SessionID, resp)
				if resp.Type == "confirmed" && resp.Status == StatusActive {
					for _, playerID := range resp.Members {
						s.sendToPlayer(playerID, resp)
					}
				}
				s.broadcastRoomStatesExcept(event.SessionID)
			case "disconnect":
				s.engine.Disconnect(event.SessionID)
				s.unregister(event.SessionID)
				s.broadcastRoomStates()
			case "tick":
				s.engine.Tick()
				s.broadcastRoomStates()
			case "shutdown":
				canceled := s.engine.Shutdown()
				_ = s.listener.Close()
				for _, playerID := range canceled {
					s.sendToPlayer(playerID, Response{Type: "canceled", OK: true, Reason: "server_shutdown"})
				}
				s.closeAllConns()
				s.finish()
				return
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
			select {
			case <-s.done:
				return
			case s.ch <- ClientEvent{Kind: "tick"}:
			}
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
	writer := s.conns[sessionID]
	s.mu.Unlock()
	if writer != nil {
		writer.write(resp)
	}
}

func (s *Server) sendToPlayer(player string, resp Response) {
	sessionID := s.engine.CurrentSession(player)
	if sessionID != "" {
		s.sendToSession(sessionID, resp)
	}
}

func (s *Server) broadcastRoomStatesExcept(excludeSession string) {
	for player, state := range s.engine.PublicRoomStates() {
		if s.engine.CurrentSession(player) == excludeSession {
			continue
		}
		s.sendToPlayer(player, state)
	}
}

func (s *Server) broadcastRoomStates() { s.broadcastRoomStatesExcept("") }

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
	var writer *connWriter
	defer func() {
		if sessionID == "" {
			return
		}
		select {
		case <-s.done:
		case s.ch <- ClientEvent{Kind: "disconnect", SessionID: sessionID}:
		}
	}()
	for {
		line, err := reader.ReadBytes('\n')
		if len(line) > 0 {
			var req Request
			if json.Unmarshal(line, &req) != nil {
				writeTo(conn, Response{Type: "error", OK: false, Reason: "bad_json"})
			} else if req.Type == "hello" {
				event := ClientEvent{Kind: "hello", PlayerID: req.PlayerID, Conn: conn, Resp: make(chan Response, 1)}
				select {
				case <-s.done:
					return
				case s.ch <- event:
				}
				select {
				case <-s.done:
					return
				case resp := <-event.Resp:
					sessionID = resp.SessionID
					writer = s.connWriter(sessionID)
					if writer != nil {
						writer.write(resp)
					} else {
						writeTo(conn, resp)
					}
				}
			} else if req.SessionID != sessionID {
				if writer != nil {
					writer.write(Response{Type: "error", OK: false, Reason: "stale_session"})
				} else {
					writeTo(conn, Response{Type: "error", OK: false, Reason: "stale_session"})
				}
			} else {
				select {
				case <-s.done:
					return
				case s.ch <- ClientEvent{Kind: "request", SessionID: sessionID, Request: req}:
				}
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

func (s *Server) connWriter(sessionID string) *connWriter {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conns[sessionID]
}

func (s *Server) attachConn(sessionID string, conn net.Conn) *connWriter {
	s.mu.Lock()
	defer s.mu.Unlock()
	writer := &connWriter{conn: conn}
	if old := s.conns[sessionID]; old != nil {
		old.close()
	}
	s.conns[sessionID] = writer
	return writer
}

func (s *Server) closeAllConns() {
	s.mu.Lock()
	writers := make([]*connWriter, 0, len(s.conns))
	for _, writer := range s.conns {
		writers = append(writers, writer)
	}
	s.conns = map[string]*connWriter{}
	s.mu.Unlock()
	for _, writer := range writers {
		writer.close()
	}
}

func (s *Server) finish() {
	s.finishOnce.Do(func() {
		close(s.done)
		close(s.closed)
	})
}

func writeTo(conn net.Conn, resp Response) {
	_ = conn.SetWriteDeadline(time.Now().Add(2 * time.Second))
	data, err := json.Marshal(resp)
	if err != nil {
		return
	}
	data = append(data, '\n')
	_, _ = conn.Write(data)
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
