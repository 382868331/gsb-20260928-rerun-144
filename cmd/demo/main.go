package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"multiplayer-session-service/internal"
)

type demoClient struct {
	id       string
	conn     net.Conn
	session  string
	room     string
	reader   *bufio.Reader
	messages chan internal.Response
}

func main() {
	statePath := flag.String("state", filepath.Join(os.TempDir(), "multiplayer-session-demo.json"), "state file")
	proposalTTL := flag.Duration("proposal-ttl", 250*time.Millisecond, "proposal TTL")
	emptyTTL := flag.Duration("empty-ttl", 500*time.Millisecond, "vacancy TTL")
	flag.Parse()
	_ = os.Remove(*statePath)

	addr, shutdown := startServer(*statePath, *proposalTTL, *emptyTTL)

	p1 := dial(addr, "p1")
	p2 := dial(addr, "p2")
	p3 := dial(addr, "p3")
	p4 := dial(addr, "p4")
	p5 := dial(addr, "p5")
	p6 := dial(addr, "p6")
	closeClients := func(clients ...*demoClient) {
		for _, client := range clients {
			_ = client.conn.Close()
		}
	}
	defer closeClients(p1, p2, p3, p5, p6)

	expect(p1, "hello")
	expect(p2, "hello")
	expect(p3, "hello")
	expect(p4, "hello")
	expect(p5, "hello")
	expect(p6, "hello")

	log.Println("send a fragmented queue request")
	p1.writeParts(`{"type":"queue","session_id":"`, p1.session, `","player_id":"p1","mode":"ranked","level":10,"room_size":2}`, "\n")
	expect(p1, "queued")

	log.Println("send coalesced queue and reject-from-nowhere requests")
	p2.writeRaw(`{"type":"queue","session_id":"` + p2.session + `","player_id":"p2","mode":"ranked","level":11,"room_size":2}` + "\n" +
		`{"type":"reject","session_id":"` + p2.session + `","player_id":"p2","room_id":"room-old"}` + "\n")
	expect(p2, "invited")
	staleRoom := p2.room
	expect(p2, "error")
	expect(p1, "invited")

	p1.send("confirm", 0, 0, p1.room)
	p2.send("reject", 0, 0, p2.room)
	expect(p2, "rejected")
	expect(p1, "canceled")
	log.Println("rejection dissolved the proposal and gave both members final results")

	log.Println("queue and cancel before a match arrives, then requeue in one write")
	p3.writeRaw(`{"type":"queue","session_id":"` + p3.session + `","player_id":"p3","mode":"ranked","level":10,"room_size":2}` + "\n" +
		`{"type":"cancel","session_id":"` + p3.session + `","player_id":"p3"}` + "\n")
	expect(p3, "queued")
	expect(p3, "canceled")
	p3.send("queue", 10, 2, "")
	expect(p3, "queued")

	p4.send("queue", 11, 2, "")
	expect(p4, "invited")
	expect(p3, "invited")
	p3.send("confirm", 0, 0, p3.room)
	p4.send("confirm", 0, 0, p4.room)
	expect(p3, "confirmed")
	expect(p4, "confirmed")

	p5.send("queue", 12, 2, "")
	expect(p5, "queued")
	log.Println("p4 exits while p5 is waiting; p5 must be invited into the released slot")
	_ = p4.conn.Close()
	expect(p3, "room_state")
	expect(p5, "invited")
	p5.send("confirm", 0, 0, p5.room)
	expect(p5, "confirmed")

	log.Println("a queued client can cancel; coalesced arrival preserves both request results")
	p6.writeRaw(`{"type":"queue","session_id":"` + p6.session + `","player_id":"p6","mode":"ranked","level":11,"room_size":2}` + "\n" +
		`{"type":"cancel","session_id":"` + p6.session + `","player_id":"p6"}` + "\n")
	expect(p6, "queued")
	expect(p6, "canceled")

	log.Println("old invitation confirmation cannot act on a different new room")
	p6.send("queue", 20, 2, "")
	expect(p6, "queued")
	p2.send("queue", 20, 2, "")
	resp := expect(p2, "invited")
	newRoom := resp.RoomID
	expect(p6, "invited")
	if newRoom == staleRoom {
		log.Fatalf("new match reused stale room %s", staleRoom)
	}
	p2.send("confirm", 0, 0, staleRoom)
	expect(p2, "error")
	p2.send("confirm", 0, 0, newRoom)
	expect(p6, "confirmed")
	expect(p2, "confirmed")

	shutdown()
	log.Println("restart server and resume both completed rooms from saved state")
	addr, shutdown = startServer(*statePath, *proposalTTL, *emptyTTL)
	p2.reconnect(addr)
	p3.reconnect(addr)
	p5.reconnect(addr)
	p6.reconnect(addr)
	expect(p2, "resumed")
	expect(p3, "resumed")
	expect(p5, "resumed")
	expect(p6, "resumed")
	shutdown()
	log.Printf("demo complete; state saved at %s", *statePath)
}

func startServer(statePath string, proposalTTL, emptyTTL time.Duration) (string, func()) {
	engine, err := internal.NewEngine(internal.NewStore(statePath), nil)
	if err != nil {
		log.Fatal(err)
	}
	engine.SetTimeouts(proposalTTL, emptyTTL)
	server, err := internal.NewServer(engine, "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go server.Serve()
	return server.Address(), func() { _ = server.Close() }
}

func dial(addr, id string) *demoClient {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	client := &demoClient{id: id, conn: conn, reader: bufio.NewReader(conn), messages: make(chan internal.Response, 32)}
	go client.readLoop()
	client.hello()
	return client
}

func (c *demoClient) reconnect(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	c.conn = conn
	c.room = ""
	c.reader = bufio.NewReader(conn)
	c.messages = make(chan internal.Response, 32)
	go c.readLoop()
	c.hello()
}

func (c *demoClient) readLoop() {
	for {
		line, err := c.reader.ReadBytes('\n')
		if len(line) > 0 {
			var resp internal.Response
			if json.Unmarshal(line, &resp) == nil {
				if resp.SessionID != "" {
					c.session = resp.SessionID
				}
				if resp.RoomID != "" {
					c.room = resp.RoomID
				}
				c.messages <- resp
			}
		}
		if err != nil {
			return
		}
	}
}

func (c *demoClient) hello() {
	c.writeRaw(`{"type":"hello","player_id":"` + c.id + `"}`)
}

func (c *demoClient) send(kind string, level, size int, roomID string) {
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
	data, _ := json.Marshal(req)
	c.writeRaw(string(data))
}

func (c *demoClient) writeRaw(message string) {
	if _, err := fmt.Fprint(c.conn, message); err != nil {
		log.Fatalf("%s write: %v", c.id, err)
	}
}

func (c *demoClient) writeParts(parts ...string) {
	for _, part := range parts {
		if _, err := fmt.Fprint(c.conn, part); err != nil {
			log.Fatalf("%s write: %v", c.id, err)
		}
	}
}

func expect(client *demoClient, typ string) internal.Response {
	select {
	case resp := <-client.messages:
		if resp.Type != typ {
			log.Fatalf("%s expected %s, got %s (%s)", client.id, typ, resp.Type, resp.Reason)
		}
		return resp
	case <-time.After(2 * time.Second):
		log.Fatalf("%s timed out waiting for %s", client.id, typ)
		return internal.Response{}
	}
}
