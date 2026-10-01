package internal

import (
	"bufio"
	"encoding/json"
	"net"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func skipWithoutListen(t *testing.T, err error) {
	t.Helper()
	if err == nil {
		return
	}
	if strings.Contains(err.Error(), "operation not permitted") || strings.Contains(err.Error(), "permission denied") {
		t.Skipf("TCP listen is unavailable in this sandbox: %v", err)
	}
	t.Fatal(err)
}

type tcpClient struct {
	t       *testing.T
	id      string
	conn    net.Conn
	reader  *bufio.Reader
	session string
}

func startTestServer(t *testing.T, proposalTTL, emptyTTL time.Duration) (*Server, *Engine) {
	t.Helper()
	engine, err := NewEngine(NewStore(filepath.Join(t.TempDir(), "state.json")), time.Now)
	if err != nil {
		t.Fatal(err)
	}
	engine.SetTimeouts(proposalTTL, emptyTTL)
	server, err := NewServer(engine, "127.0.0.1:0")
	skipWithoutListen(t, err)
	t.Cleanup(func() { _ = server.Close() })
	go server.Serve()
	return server, engine
}

func dialTCP(t *testing.T, addr, id string) *tcpClient {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	c := &tcpClient{t: t, id: id, conn: conn, reader: bufio.NewReader(conn)}
	c.hello()
	return c
}

func (c *tcpClient) hello() {
	c.writeLine(marshalLine(c.t, map[string]interface{}{"type": "hello", "player_id": c.id}))
	resp := c.readUntil("hello", "resumed")
	c.session = resp.SessionID
}

func marshalLine(t *testing.T, value map[string]interface{}) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return append(data, '\n')
}

func (c *tcpClient) request(value map[string]interface{}) {
	value["session_id"] = c.session
	value["player_id"] = c.id
	c.writeLine(marshalLine(c.t, value))
}

func (c *tcpClient) requestFragments(value map[string]interface{}, cut int) {
	value["session_id"] = c.session
	value["player_id"] = c.id
	data := marshalLine(c.t, value)
	if _, err := c.conn.Write(data[:cut]); err != nil {
		c.t.Fatal(err)
	}
	time.Sleep(10 * time.Millisecond)
	if _, err := c.conn.Write(data[cut:]); err != nil {
		c.t.Fatal(err)
	}
}

func (c *tcpClient) writeCombined(first, second map[string]interface{}) {
	first["session_id"] = c.session
	first["player_id"] = c.id
	second["session_id"] = c.session
	second["player_id"] = c.id
	data := append(marshalLine(c.t, first), marshalLine(c.t, second)...)
	c.writeLine(data)
}

func (c *tcpClient) writeLine(data []byte) {
	if _, err := c.conn.Write(data); err != nil {
		c.t.Fatal(err)
	}
}

func (c *tcpClient) readUntil(kinds ...string) Response {
	c.t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for {
		_ = c.conn.SetReadDeadline(deadline)
		line, err := c.reader.ReadBytes('\n')
		if len(line) == 0 && err != nil {
			c.t.Fatalf("%s closed before %v: %v", c.id, kinds, err)
		}
		var resp Response
		if json.Unmarshal(line, &resp) != nil {
			c.t.Fatalf("bad response %q: %v", line, err)
		}
		if resp.SessionID != "" {
			c.session = resp.SessionID
		}
		for _, kind := range kinds {
			if resp.Type == kind {
				return resp
			}
		}
		if err != nil {
			c.t.Fatalf("%s reached EOF before %v", c.id, kinds)
		}
	}
}

func (c *tcpClient) close() { _ = c.conn.Close() }

func TestTCPFragmentedAndMergedMessagesConfirmExitRefill(t *testing.T) {
	server, engine := startTestServer(t, time.Second, 2*time.Second)
	p1 := dialTCP(t, server.Address(), "p1")
	p2 := dialTCP(t, server.Address(), "p2")
	p3 := dialTCP(t, server.Address(), "p3")

	p1.requestFragments(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 10, "room_size": 2}, 13)
	p2.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 11, "room_size": 2})
	first := p1.readUntil("invited")
	second := p2.readUntil("invited")
	if first.RoomID != second.RoomID || first.RoomID != "room-1" {
		t.Fatalf("unexpected invitations: %#v %#v", first, second)
	}
	p1.writeCombined(
		map[string]interface{}{"type": "confirm", "room_id": first.RoomID},
		map[string]interface{}{"type": "snapshot"},
	)
	p2.requestFragments(map[string]interface{}{"type": "confirm", "room_id": second.RoomID}, 7)
	if resp := p1.readUntil("confirmed"); resp.Status != StatusActive {
		t.Fatalf("p1 final confirm = %#v", resp)
	}
	if resp := p2.readUntil("confirmed"); resp.Status != StatusActive {
		t.Fatalf("p2 final confirm = %#v", resp)
	}

	p3.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 12, "room_size": 2})
	p3.readUntil("queued")
	p2.close()
	p1.readUntil("room_state")
	refill := p3.readUntil("invited")
	if refill.RoomID != first.RoomID || refill.Reason != StatusInvited {
		t.Fatalf("p3 refill invitation = %#v", refill)
	}
	p3.requestFragments(map[string]interface{}{"type": "confirm", "room_id": refill.RoomID}, 3)
	if resp := p3.readUntil("confirmed"); resp.Status != StatusActive {
		t.Fatalf("p3 refill confirm = %#v", resp)
	}
	time.Sleep(80 * time.Millisecond)
	if room := engine.rooms[first.RoomID]; room == nil || room.Status != StatusActive || room.Members["p3"] == nil || room.Members["p2"] != nil {
		t.Fatalf("refilled room = %#v", room)
	}
	if engine.Result("p1") != StatusActive || engine.Result("p3") != StatusActive {
		t.Fatalf("results = p1:%q p3:%q", engine.Result("p1"), engine.Result("p3"))
	}
}

func TestTCPCancelledWaitersAreWokenOnClose(t *testing.T) {
	server, engine := startTestServer(t, time.Second, 2*time.Second)
	p1 := dialTCP(t, server.Address(), "p1")
	p2 := dialTCP(t, server.Address(), "p2")
	p1.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 1, "room_size": 2})
	p2.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 10, "room_size": 2})
	p1.readUntil("queued")
	p2.readUntil("queued")

	closed := make(chan error, 1)
	go func() { closed <- server.Close() }()
	resp1 := p1.readUntil("canceled")
	resp2 := p2.readUntil("canceled")
	if resp1.Reason != "server_shutdown" || resp2.Reason != "server_shutdown" {
		t.Fatalf("shutdown responses = %#v %#v", resp1, resp2)
	}
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("server close waited for disconnected clients")
	}
	if len(engine.queue) != 0 || engine.Result("p1") != StatusCanceled || engine.Result("p2") != StatusCanceled {
		t.Fatalf("state after close = queue:%v results:%v %v", engine.queue, engine.Result("p1"), engine.Result("p2"))
	}
}

func TestTCPOldInvitationCannotConfirmNewRoom(t *testing.T) {
	server, _ := startTestServer(t, 60*time.Millisecond, time.Second)
	p1 := dialTCP(t, server.Address(), "p1")
	p2 := dialTCP(t, server.Address(), "p2")
	p1.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 10, "room_size": 2})
	p2.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 11, "room_size": 2})
	old := p1.readUntil("invited")
	p2.readUntil("invited")
	p2.request(map[string]interface{}{"type": "reject", "room_id": old.RoomID})
	if resp := p2.readUntil("rejected"); resp.RoomID != old.RoomID {
		t.Fatalf("reject response = %#v", resp)
	}
	time.Sleep(150 * time.Millisecond)

	p3 := dialTCP(t, server.Address(), "p3")
	p4 := dialTCP(t, server.Address(), "p4")
	p3.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 20, "room_size": 2})
	p4.request(map[string]interface{}{"type": "queue", "mode": "ranked", "level": 21, "room_size": 2})
	newRoom := p3.readUntil("invited")
	p4.readUntil("invited")
	if newRoom.RoomID == old.RoomID || newRoom.RoomID != "room-2" {
		t.Fatalf("new room id = %q old = %q", newRoom.RoomID, old.RoomID)
	}
	p1.request(map[string]interface{}{"type": "confirm", "room_id": old.RoomID})
	resp := p1.readUntil("error")
	if resp.Reason != "not_invited" {
		t.Fatalf("old invite result = %#v", resp)
	}
	p3.request(map[string]interface{}{"type": "confirm", "room_id": newRoom.RoomID})
	if resp := p3.readUntil("confirmed"); resp.Status != StatusProposed {
		t.Fatalf("new room accepted old confirmation: %#v", resp)
	}
}
