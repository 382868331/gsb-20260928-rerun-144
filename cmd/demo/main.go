package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"strings"
	"time"

	"multiplayer-session-service/internal"
)

type client struct {
	id       string
	conn     net.Conn
	session  string
	room     string
	status   string
	waiting  []string
	members  []string
	messages []string
	lastLine string
}

func main() {
	statePath := flag.String("state", filepath.Join(os.TempDir(), "multiplayer-session-demo.json"), "state file")
	proposalTTL := flag.Duration("proposal-ttl", 10*time.Second, "proposal TTL")
	emptyTTL := flag.Duration("empty-ttl", 15*time.Second, "vacancy TTL")
	flag.Parse()
	_ = os.Remove(*statePath)

	addr, shutdown := startServer(*statePath, *proposalTTL, *emptyTTL)
	defer shutdown()

	p1 := dial("p1", addr)
	p2 := dial("p2", addr)
	p3 := dial("p3", addr)
	defer closeAll(p1, p2, p3)

	readUntil(p1, "hello")
	readUntil(p2, "hello")
	readUntil(p3, "hello")
	p1.send("queue", 10, 2, "")
	time.Sleep(50 * time.Millisecond)
	p2.send("queue", 11, 2, "")
	readUntil(p1, "invited")
	readUntil(p2, "invited")
	p1.send("confirm", 0, 0, p1.room)
	p2.send("confirm", 0, 0, p2.room)
	readUntil(p1, "confirmed")
	readUntil(p2, "confirmed")

	p3.send("queue", 12, 2, "")
	readUntil(p3, "queued")
	log.Println("p2 disconnects; released slot is offered to waiting p3")
	_ = p2.conn.Close()
	readUntil(p1, "room_state")
	readUntil(p3, "room_state")
	p3.send("confirm", 0, 0, p3.room)
	readUntil(p3, "confirmed")
	time.Sleep(100 * time.Millisecond)
	shutdown()

	log.Println("server restarts from saved state")
	addr, shutdown = startServer(*statePath, *proposalTTL, *emptyTTL)
	defer shutdown()
	p1.reconnect(addr)
	p3.reconnect(addr)
	resumedP1 := readUntil(p1, "resumed")
	resumedP3 := readUntil(p3, "resumed")
	if resumedP1.room != resumedP3.room {
		log.Fatalf("restarted players are in different rooms: %s %s", resumedP1.room, resumedP3.room)
	}

	p4 := dial("p4", addr)
	defer p4.close()
	readUntil(p4, "hello")
	old := p4.session
	p4.hello()
	readUntil(p4, "hello")
	p4.sendRaw(map[string]string{"type": "queue", "session_id": old, "player_id": "p4", "mode": "ranked"})
	stale := readUntil(p4, "error")
	if stale.reason() != "stale_session" {
		log.Fatalf("old session was accepted: %s", stale.lastLine)
	}

	log.Printf("demo complete: room %s contains confirmed %v", resumedP1.room, resumedP1.members)
}

func startServer(statePath string, proposalTTL, emptyTTL time.Duration) (string, func()) {
	engine, err := internal.NewEngine(internal.NewStore(statePath), nil)
	if err != nil {
		log.Fatal(err)
	}
	server, err := internal.NewServer(engine, "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go server.Serve()
	return server.Address(), func() { _ = server.Close() }
}

func dial(id, addr string) *client {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	c := &client{id: id, conn: conn}
	c.hello()
	return c
}

func (c *client) reconnect(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	c.conn = conn
	c.room = ""
	c.status = ""
	c.hello()
}

func (c *client) hello() {
	c.sendRaw(map[string]string{"type": "hello", "player_id": c.id})
}

func (c *client) send(kind string, level, size int, roomID string) {
	req := map[string]interface{}{"type": kind, "session_id": c.session, "player_id": c.id}
	if level != 0 {
		req["level"] = level
	}
	if size != 0 {
		req["room_size"] = size
	}
	if roomID != "" {
		req["room_id"] = roomID
	}
	c.sendRaw(req)
}

func (c *client) sendRaw(req interface{}) {
	data, _ := json.Marshal(req)
	if _, err := c.conn.Write(append(data, '\n')); err != nil {
		log.Fatal(err)
	}
}

func readUntil(c *client, kind string) *client {
	reader := bufio.NewReader(c.conn)
	for {
		line, err := reader.ReadString('\n')
		if err != nil && err != io.EOF {
			log.Fatal(err)
		}
		if line == "" {
			log.Fatalf("%s closed while waiting for %s", c.id, kind)
		}
		var resp internal.Response
		if err := json.Unmarshal([]byte(strings.TrimSpace(line)), &resp); err != nil {
			log.Fatal(err)
		}
		c.lastLine = line
		if resp.SessionID != "" {
			c.session = resp.SessionID
		}
		if resp.RoomID != "" {
			c.room = resp.RoomID
		}
		if resp.Status != "" {
			c.status = resp.Status
		}
		c.waiting = resp.Waiting
		c.members = resp.Members
		if resp.Reason != "" {
			c.messages = append(c.messages, resp.Reason)
		}
		if resp.Type == kind {
			return c
		}
	}
}

func (c *client) reason() string {
	if len(c.messages) == 0 {
		return ""
	}
	return c.messages[len(c.messages)-1]
}
func (c *client) close() { _ = c.conn.Close() }
func closeAll(clients ...*client) {
	for _, c := range clients {
		c.close()
	}
}
