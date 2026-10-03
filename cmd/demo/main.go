package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"io"
	"log"
	"net"
	"os"
	"path/filepath"
	"time"

	"multiplayer-session-service/internal"
)

type client struct {
	id      string
	conn    net.Conn
	reader  *bufio.Reader
	session string
	room    string
	status  string
	result  internal.Response
}

func main() {
	statePath := flag.String("state", filepath.Join(os.TempDir(), "multiplayer-session-demo.json"), "state file")
	proposalTTL := flag.Duration("proposal-ttl", 400*time.Millisecond, "proposal TTL")
	emptyTTL := flag.Duration("empty-ttl", 800*time.Millisecond, "vacancy TTL")
	reserveTTL := flag.Duration("reserve-ttl", 500*time.Millisecond, "disconnect seat reservation TTL")
	flag.Parse()
	_ = os.Remove(*statePath)

	addr, shutdown := startServer(*statePath, *proposalTTL, *emptyTTL, *reserveTTL)

	p1 := dial("p1", addr)
	p2 := dial("p2", addr)
	p3 := dial("p3", addr)
	defer closeAll(p1, p2, p3)

	log.Println("phase 1: final confirmation converges for every participant")
	p1.queueFragmented(10, 2)
	p2.queueMerged(11, 2)
	first := p1.expect("invited")
	if room := p2.expect("invited"); room.RoomID != first.RoomID {
		log.Fatalf("p1 and p2 received different rooms: %s %s", first.RoomID, room.RoomID)
	}
	p1.confirmMerged(first.RoomID)
	if resp := p1.expect("room_state"); resp.Status != internal.StatusProposed {
		log.Fatalf("p1 non-final confirm = %#v", resp)
	}
	p2.confirmFragmented(first.RoomID)
	if resp := p1.expect("confirmed"); resp.Status != internal.StatusActive {
		log.Fatalf("p1 final decision broadcast = %#v", resp)
	}
	finalP2 := p2.expect("confirmed")
	if finalP2.Status != internal.StatusActive {
		log.Fatalf("matched room was not activated: %#v", finalP2)
	}
	p1.confirm(first.RoomID)
	if resp := p1.expect("confirmed"); resp.Status != internal.StatusActive {
		log.Fatalf("duplicate confirm regressed the room: %#v", resp)
	}

	log.Println("phase 2: brief disconnect reserves the confirmed seat")
	p3.queue(12, 2)
	p3.expect("queued")
	oldSession := p2.session
	_ = p2.conn.Close()
	time.Sleep(120 * time.Millisecond)
	p3.snapshot()
	if snap := p3.expect("snapshot"); !contains(snap.Waiting, "p3") {
		log.Fatalf("reserved seat was refilled during grace period: %#v", snap)
	}
	p2.reconnect(addr)
	if resumed := p2.result; resumed.Type != "resumed" || resumed.RoomID != first.RoomID || resumed.Status != internal.StatusActive {
		log.Fatalf("p2 did not resume its reserved seat: %#v", resumed)
	}
	p2.sendAs(oldSession, map[string]interface{}{"type": "confirm", "room_id": first.RoomID})
	if resp := p2.expect("error"); resp.Reason != "stale_session" {
		log.Fatalf("stale connection confirm reached the new identity: %#v", resp)
	}

	log.Println("phase 3: reservation expires and a waiting player backfills")
	_ = p2.conn.Close()
	time.Sleep(*reserveTTL + 300*time.Millisecond)
	refill := p3.expect("invited")
	if refill.RoomID != first.RoomID {
		log.Fatalf("p3 was offered %s instead of %s", refill.RoomID, first.RoomID)
	}
	p3.confirmFragmented(refill.RoomID)
	if resp := p3.expect("confirmed"); resp.Status != internal.StatusActive {
		log.Fatalf("refill confirmation = %#v", resp)
	}
	for {
		resp := p1.expect("confirmed")
		if contains(resp.Members, "p3") && !contains(resp.Members, "p2") {
			break
		}
		if !contains(resp.Members, "p2") {
			log.Fatalf("p1 sees unexpected seats: %#v", resp)
		}
	}
	p2.reconnect(addr)
	if p2.result.Type != "hello" {
		log.Fatalf("expired seat still resumes: %#v", p2.result)
	}
	p2.confirm(first.RoomID)
	if resp := p2.expect("error"); resp.Reason != "not_invited" {
		log.Fatalf("expired member confirm = %#v", resp)
	}
	p2.resultQuery()
	if resp := p2.expect("result"); resp.Status != internal.StatusCanceled {
		log.Fatalf("expired member result = %#v", resp)
	}

	log.Println("phase 4: restart keeps the final decision explainable")
	shutdown()
	addr, shutdown = startServer(*statePath, *proposalTTL, *emptyTTL, *reserveTTL)
	p1.reconnect(addr)
	p3.reconnect(addr)
	resumedP1 := p1.expect("resumed")
	resumedP3 := p3.expect("resumed")
	if resumedP1.RoomID != resumedP3.RoomID || resumedP1.RoomID != first.RoomID {
		log.Fatalf("restarted room = %s %s, want %s", resumedP1.RoomID, resumedP3.RoomID, first.RoomID)
	}
	p1.resultQuery()
	if resp := p1.expect("result"); resp.Status != internal.StatusActive {
		log.Fatalf("persisted final decision = %#v", resp)
	}

	log.Println("phase 5: old invitations cannot reach rematched rooms")
	p4 := dial("p4", addr)
	p5 := dial("p5", addr)
	p6 := dial("p6", addr)
	defer closeAll(p4, p5, p6)
	p4.queue(30, 2)
	p5.queue(31, 2)
	old := p4.expect("invited")
	p5.expect("invited")
	p5.reject(old.RoomID)
	p5.expect("rejected")
	time.Sleep(*proposalTTL + 150*time.Millisecond)

	p6.queue(40, 2)
	p5.queue(41, 2)
	newRoom := p5.expect("invited")
	p6.expect("invited")
	if newRoom.RoomID == old.RoomID || newRoom.RoomID == first.RoomID {
		log.Fatalf("new room reused old id: old=%s new=%s", old.RoomID, newRoom.RoomID)
	}
	p4.confirm(old.RoomID)
	if resp := p4.expect("error"); resp.Reason != "not_invited" {
		log.Fatalf("old invitation affected new matching: %#v", resp)
	}
	p5.confirmFragmented(newRoom.RoomID)
	p6.confirmMerged(newRoom.RoomID)
	if resp := p5.expect("confirmed"); resp.Status != internal.StatusActive {
		log.Fatalf("p5 final decision = %#v", resp)
	}
	if resp := p6.expect("confirmed"); resp.Status != internal.StatusActive {
		log.Fatalf("p6 final decision = %#v", resp)
	}

	log.Println("closing while a fresh player is waiting must produce a final result")
	p7 := dial("p7", addr)
	defer p7.close()
	p7.queue(99, 2)
	p7.expect("queued")
	closed := make(chan error, 1)
	go func() { closed <- shutdownServer(addr, *statePath, *proposalTTL, *emptyTTL, *reserveTTL, shutdown) }()
	if resp := p7.expect("canceled"); resp.Reason != "server_shutdown" {
		log.Fatalf("waiting player result = %#v", resp)
	}
	if err := <-closed; err != nil {
		log.Fatal(err)
	}
	log.Printf("demo complete: success room %s contains %v", resumedP1.RoomID, resumedP1.Members)
}

func startServer(statePath string, proposalTTL, emptyTTL, reserveTTL time.Duration) (string, func()) {
	engine, err := internal.NewEngine(internal.NewStore(statePath), nil)
	if err != nil {
		log.Fatal(err)
	}
	engine.SetTimeouts(proposalTTL, emptyTTL, reserveTTL)
	server, err := internal.NewServer(engine, "127.0.0.1:0")
	if err != nil {
		log.Fatal(err)
	}
	go server.Serve()
	return server.Address(), func() { _ = server.Close() }
}

func shutdownServer(_, _ string, _, _, _ time.Duration, shutdown func()) error {
	shutdown()
	return nil
}

func contains(values []string, want string) bool {
	for _, value := range values {
		if value == want {
			return true
		}
	}
	return false
}

func dial(id, addr string) *client {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	c := &client{id: id, conn: conn, reader: bufio.NewReader(conn)}
	c.hello()
	return c
}

func (c *client) reconnect(addr string) {
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		log.Fatal(err)
	}
	c.conn = conn
	c.reader = bufio.NewReader(conn)
	c.room = ""
	c.status = ""
	c.hello()
}

func (c *client) send(value map[string]interface{}) {
	value["session_id"] = c.session
	value["player_id"] = c.id
	data, err := json.Marshal(value)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := c.conn.Write(append(data, '\n')); err != nil {
		log.Fatal(err)
	}
}

func (c *client) sendAs(session string, value map[string]interface{}) {
	value["session_id"] = session
	value["player_id"] = c.id
	data, err := json.Marshal(value)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := c.conn.Write(append(data, '\n')); err != nil {
		log.Fatal(err)
	}
}

func (c *client) sendFragment(value map[string]interface{}, cut int) {
	value["session_id"] = c.session
	value["player_id"] = c.id
	data, err := json.Marshal(value)
	if err != nil {
		log.Fatal(err)
	}
	data = append(data, '\n')
	if _, err := c.conn.Write(data[:cut]); err != nil {
		log.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := c.conn.Write(data[cut:]); err != nil {
		log.Fatal(err)
	}
}

func (c *client) sendCombined(first, second map[string]interface{}) {
	first["session_id"] = c.session
	first["player_id"] = c.id
	second["session_id"] = c.session
	second["player_id"] = c.id
	a, _ := json.Marshal(first)
	b, _ := json.Marshal(second)
	if _, err := c.conn.Write(append(append(a, '\n'), append(b, '\n')...)); err != nil {
		log.Fatal(err)
	}
}

func (c *client) hello() {
	c.send(map[string]interface{}{"type": "hello"})
	c.result = c.expect("hello", "resumed")
	c.session = c.result.SessionID
}

func (c *client) queue(level, size int) {
	c.send(map[string]interface{}{"type": "queue", "mode": "ranked", "level": level, "room_size": size})
}

func (c *client) queueFragmented(level, size int) {
	c.sendFragment(map[string]interface{}{"type": "queue", "mode": "ranked", "level": level, "room_size": size}, 15)
}

func (c *client) queueMerged(level, size int) {
	time.Sleep(20 * time.Millisecond)
	c.sendCombined(
		map[string]interface{}{"type": "queue", "mode": "ranked", "level": level, "room_size": size},
		map[string]interface{}{"type": "snapshot"},
	)
}

func (c *client) confirm(roomID string) {
	c.send(map[string]interface{}{"type": "confirm", "room_id": roomID})
}

func (c *client) confirmFragmented(roomID string) {
	c.sendFragment(map[string]interface{}{"type": "confirm", "room_id": roomID}, 5)
}

func (c *client) confirmMerged(roomID string) {
	c.sendCombined(
		map[string]interface{}{"type": "confirm", "room_id": roomID},
		map[string]interface{}{"type": "snapshot"},
	)
}

func (c *client) reject(roomID string) {
	c.send(map[string]interface{}{"type": "reject", "room_id": roomID})
}

func (c *client) snapshot() {
	c.send(map[string]interface{}{"type": "snapshot"})
}

func (c *client) resultQuery() {
	c.send(map[string]interface{}{"type": "result"})
}

func (c *client) expect(kinds ...string) internal.Response {
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	for {
		line, err := c.reader.ReadBytes('\n')
		if len(line) > 0 {
			var resp internal.Response
			if json.Unmarshal(line, &resp) != nil {
				log.Fatalf("%s received bad JSON %q", c.id, line)
			}
			if resp.SessionID != "" {
				c.session = resp.SessionID
			}
			if resp.RoomID != "" {
				c.room = resp.RoomID
			}
			if resp.Status != "" {
				c.status = resp.Status
			}
			for _, kind := range kinds {
				if resp.Type == kind {
					c.result = resp
					return resp
				}
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) && len(line) == 0 {
				log.Fatalf("%s closed while waiting for %v", c.id, kinds)
			}
			log.Fatalf("%s waited for %v: %v", c.id, kinds, err)
		}
	}
}

func (c *client) close() { _ = c.conn.Close() }
func closeAll(clients ...*client) {
	for _, client := range clients {
		client.close()
	}
}
