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
	"sort"
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

	log.Println("== phase 1: match, confirm convergence, duplicate confirms ==")
	p1 := dial("p1", addr)
	p2 := dial("p2", addr)
	p3 := dial("p3", addr)
	defer closeAll(p1, p2, p3)

	p1.queueFragmented(10, 2)
	p2.queueMerged(11, 2)
	first := p1.expect("invited")
	if room := p2.expect("invited"); room.RoomID != first.RoomID {
		log.Fatalf("p1 and p2 received different rooms: %s %s", first.RoomID, room.RoomID)
	}
	p1.confirmMerged(first.RoomID)
	if resp := p1.expect("confirmed"); resp.Status != internal.StatusProposed {
		log.Fatalf("p1 first confirmation = %#v", resp)
	}
	p1.confirm(first.RoomID)
	if resp := p1.expect("confirmed"); resp.Status != internal.StatusProposed {
		log.Fatalf("p1 duplicate confirmation = %#v", resp)
	}
	p2.confirmFragmented(first.RoomID)
	p1.expectStatus("confirmed", internal.StatusActive)
	p2.expectStatus("confirmed", internal.StatusActive)
	p1.expectRoomState(internal.StatusActive, nil, "p1", "p2")
	log.Printf("room %s converged: every participant saw the final decision", first.RoomID)

	log.Println("== phase 2: brief disconnect keeps the confirmed seat reserved ==")
	p3.queue(12, 2)
	p3.expect("queued")
	oldP2Session := p2.session
	_ = p2.conn.Close()
	time.Sleep(120 * time.Millisecond)
	p1.expectRoomState(internal.StatusActive, []string{"p2"}, "p1", "p2")
	p3.expectNone(200*time.Millisecond, "queued")
	p2.reconnect(addr)
	resumed := p2.expect("resumed")
	if resumed.RoomID != first.RoomID || resumed.Status != internal.StatusActive {
		log.Fatalf("p2 resume = %#v", resumed)
	}
	p3.expectNone(200*time.Millisecond, "queued")
	log.Printf("p2 reclaimed the reserved seat in %s before %s", first.RoomID, *reserveTTL)

	log.Println("== phase 3: reservation expires, waiting player backfills, late acts are rejected ==")
	_ = p2.conn.Close()
	time.Sleep(*reserveTTL + 250*time.Millisecond)
	refill := p3.expect("invited")
	if refill.RoomID != first.RoomID {
		log.Fatalf("p3 was offered %s instead of %s", refill.RoomID, first.RoomID)
	}
	p3.confirmFragmented(refill.RoomID)
	p3.expectStatus("confirmed", internal.StatusActive)
	p1.expectRoomState(internal.StatusActive, nil, "p1", "p3")

	p2.reconnect(addr)
	if resp := p2.expect("hello"); resp.RoomID != "" {
		log.Fatalf("expired seat still resumed: %#v", resp)
	}
	p2.confirmWithSession(oldP2Session, first.RoomID)
	if resp := p2.expect("error"); resp.Reason != "stale_session" {
		log.Fatalf("old connection confirm = %#v", resp)
	}
	p2.confirm(first.RoomID)
	if resp := p2.expect("error"); resp.Reason != "not_invited" {
		log.Fatalf("late confirm after expiry = %#v", resp)
	}
	p1.expectRoomState(internal.StatusActive, nil, "p1", "p3")
	log.Printf("seat in %s moved p2 -> p3; stale and late confirms changed nothing", first.RoomID)

	log.Println("== phase 4: a fully vacated finished room is dissolved, never reopened ==")
	_ = p1.conn.Close()
	_ = p3.conn.Close()
	time.Sleep(120 * time.Millisecond)
	p4 := dial("p4", addr)
	p5 := dial("p5", addr)
	defer closeAll(p4, p5)
	p4.queue(11, 2)
	p4.expect("queued")
	p4.expectNone(*reserveTTL+150*time.Millisecond, "queued")
	time.Sleep(250 * time.Millisecond)
	p4.expectNone(200*time.Millisecond, "queued")
	if snap := p4.snapshot(); contains(snap.Members, first.RoomID) {
		log.Fatalf("finished room %s was reopened: %#v", first.RoomID, snap)
	}
	p5.queue(12, 2)
	second := p4.expect("invited")
	p5.expect("invited")
	if second.RoomID == first.RoomID {
		log.Fatalf("dissolved room id reused: %s", second.RoomID)
	}
	p4.confirm(second.RoomID)
	p5.confirmFragmented(second.RoomID)
	p4.expectStatus("confirmed", internal.StatusActive)
	p5.expectStatus("confirmed", internal.StatusActive)
	log.Printf("waiters formed fresh room %s instead of inheriting %s", second.RoomID, first.RoomID)

	log.Println("== phase 5: restart keeps explaining the final decision ==")
	shutdown()
	addr, shutdown = startServer(*statePath, *proposalTTL, *emptyTTL, *reserveTTL)
	defer shutdown()
	p4.reconnect(addr)
	p5.reconnect(addr)
	resumedP4 := p4.expect("resumed")
	resumedP5 := p5.expect("resumed")
	if resumedP4.RoomID != second.RoomID || resumedP5.RoomID != second.RoomID ||
		resumedP4.Status != internal.StatusActive || resumedP5.Status != internal.StatusActive {
		log.Fatalf("restart resume = %#v %#v", resumedP4, resumedP5)
	}
	results := readResults(*statePath)
	log.Printf("demo complete: room %s holds %v; persisted final results %v", second.RoomID, resumedP4.Members, results)
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

func readResults(statePath string) map[string]string {
	data, err := os.ReadFile(statePath)
	if err != nil {
		log.Fatal(err)
	}
	var saved struct {
		Results map[string]string `json:"results"`
	}
	if err := json.Unmarshal(data, &saved); err != nil {
		log.Fatal(err)
	}
	return saved.Results
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
	c.sendRaw(c.session, value)
}

func (c *client) sendRaw(session string, value map[string]interface{}) {
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

func (c *client) confirmWithSession(session, roomID string) {
	c.sendRaw(session, map[string]interface{}{"type": "confirm", "room_id": roomID})
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

func (c *client) snapshot() internal.Response {
	c.send(map[string]interface{}{"type": "snapshot"})
	return c.expect("snapshot")
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

func (c *client) expectStatus(kind, status string) internal.Response {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp := c.expect(kind)
		if resp.Status == status {
			return resp
		}
	}
	log.Fatalf("%s never saw %s with status %s", c.id, kind, status)
	return internal.Response{}
}

func (c *client) expectRoomState(status string, reserved []string, members ...string) internal.Response {
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		resp := c.expect("room_state")
		if resp.Status == status && sameStrings(resp.Reserved, reserved) && sameStrings(resp.Members, members) {
			return resp
		}
	}
	log.Fatalf("%s never saw room_state status=%s reserved=%v members=%v", c.id, status, reserved, members)
	return internal.Response{}
}

func (c *client) expectNone(window time.Duration, allowed ...string) {
	_ = c.conn.SetReadDeadline(time.Now().Add(window))
	for {
		line, err := c.reader.ReadBytes('\n')
		if len(line) > 0 {
			var resp internal.Response
			if json.Unmarshal(line, &resp) != nil {
				log.Fatalf("%s received bad JSON %q", c.id, line)
			}
			if !contains(allowed, resp.Type) {
				log.Fatalf("%s unexpectedly received %#v", c.id, resp)
			}
		}
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Fatalf("%s closed during quiet window", c.id)
			}
			if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
				return
			}
			log.Fatalf("%s quiet window: %v", c.id, err)
		}
	}
}

func sameStrings(a, b []string) bool {
	as := append([]string(nil), a...)
	bs := append([]string(nil), b...)
	sort.Strings(as)
	sort.Strings(bs)
	if len(as) != len(bs) {
		return false
	}
	for i := range as {
		if as[i] != bs[i] {
			return false
		}
	}
	return true
}

func contains(list []string, want string) bool {
	for _, item := range list {
		if item == want {
			return true
		}
	}
	return false
}

func (c *client) close() { _ = c.conn.Close() }
func closeAll(clients ...*client) {
	for _, client := range clients {
		client.close()
	}
}
