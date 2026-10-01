package internal

import (
	"bufio"
	"encoding/json"
	"io"
	"net"
	"path/filepath"
	"testing"
	"time"
)

type testClient struct {
	id       string
	t        *testing.T
	conn     net.Conn
	session  string
	room     string
	reader   *bufio.Reader
	response Response
}

func newPipeServer(t *testing.T) *Server {
	t.Helper()
	engine, err := NewEngine(NewStore(filepath.Join(t.TempDir(), "state.json")), nil)
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(engine, nil, 20*time.Millisecond)
	t.Cleanup(func() { _ = server.Close() })
	return server
}

func newPipeClient(t *testing.T, server *Server, id string) *testClient {
	t.Helper()
	clientConn, serverConn := net.Pipe()
	t.Cleanup(func() { _ = clientConn.Close() })
	server.ServeConn(serverConn)
	client := &testClient{id: id, t: t, conn: clientConn, reader: bufio.NewReader(clientConn)}
	client.writeLine(`{"type":"hello","player_id":"` + id + `"}`)
	client.read(t)
	if client.response.Type != "hello" || client.response.SessionID == "" {
		t.Fatalf("%s hello = %#v", id, client.response)
	}
	client.session = client.response.SessionID
	return client
}

func (c *testClient) writeLine(line string) {
	if _, err := io.WriteString(c.conn, line+"\n"); err != nil {
		c.t.Fatalf("%s write: %v", c.id, err)
	}
}

func (c *testClient) writeParts(parts ...string) {
	for _, part := range parts {
		if _, err := io.WriteString(c.conn, part); err != nil {
			c.t.Fatalf("%s write part: %v", c.id, err)
		}
	}
}

func (c *testClient) read(t *testing.T) {
	t.Helper()
	_ = c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	line, err := c.reader.ReadBytes('\n')
	if err != nil {
		t.Fatalf("%s read: %v", c.id, err)
	}
	if err := json.Unmarshal(line, &c.response); err != nil {
		t.Fatalf("%s response %q: %v", c.id, line, err)
	}
	if c.response.RoomID != "" {
		c.room = c.response.RoomID
	}
}

func (c *testClient) readUntil(t *testing.T, typ string) {
	t.Helper()
	for {
		c.read(t)
		if c.response.Type == typ {
			return
		}
	}
}

func (c *testClient) request(kind string) {
	line := `{"type":"` + kind + `","session_id":"` + c.session + `","player_id":"` + c.id + `"`
	if c.room != "" {
		line += `,"room_id":"` + c.room + `"`
	}
	line += `}`
	c.writeLine(line)
}

func TestTCPFragmentedMessagesDisconnectAndRefill(t *testing.T) {
	server := newPipeServer(t)
	p1 := newPipeClient(t, server, "p1")
	p2 := newPipeClient(t, server, "p2")
	p3 := newPipeClient(t, server, "p3")

	queueP1 := `{"type":"queue","session_id":"` + p1.session + `","player_id":"p1","mode":"ranked","level":10,"room_size":2}`
	p1.writeParts(queueP1[:20], queueP1[20:], "\n")
	p1.readUntil(t, "queued")

	queueP2 := `{"type":"queue","session_id":"` + p2.session + `","player_id":"p2","mode":"ranked","level":11,"room_size":2}`
	p2.writeParts(queueP2, "\n")
	p2.readUntil(t, "invited")
	confirmP2 := `{"type":"confirm","session_id":"` + p2.session + `","player_id":"p2","room_id":"` + p2.room + `"}`
	p2.writeParts(confirmP2[:25], confirmP2[25:], "\n")
	p1.readUntil(t, "invited")
	p2.readUntil(t, "confirmed")
	p3.writeLine(`{"type":"queue","session_id":"` + p3.session + `","player_id":"p3","mode":"ranked","level":12,"room_size":2}`)
	p3.readUntil(t, "queued")
	p1.writeLine(`{"type":"confirm","session_id":"` + p1.session + `","player_id":"p1","room_id":"` + p1.room + `"}`)
	p1.readUntil(t, "confirmed")

	if err := p2.conn.Close(); err != nil {
		t.Fatal(err)
	}
	p1.readUntil(t, "room_state")
	p3.readUntil(t, "invited")
	if p3.room != p1.room {
		t.Fatalf("refill room = %s, want %s", p3.room, p1.room)
	}
	confirm := `{"type":"confirm","session_id":"` + p3.session + `","player_id":"p3","room_id":"` + p3.room + `"}`
	p3.writeParts(confirm[:30], confirm[30:], "\n")
	p3.readUntil(t, "confirmed")
	if p3.response.Status != StatusActive {
		t.Fatalf("refill confirmation = %#v", p3.response)
	}

	if record := server.engine.SessionRecord(p2.session); record.Result != StatusActive {
		t.Fatalf("p2 result = %#v", record)
	}
	if record := server.engine.SessionRecord(p3.session); record.Result != StatusActive {
		t.Fatalf("p3 result = %#v", record)
	}
	if len(server.engine.queue) != 0 || len(server.engine.rooms[p1.room].Members) != 2 {
		t.Fatalf("unresolved queue or room: queue=%#v room=%#v", server.engine.queue, server.engine.rooms[p1.room])
	}
}

func TestOldInvitationCannotConfirmNewRoom(t *testing.T) {
	engine, now := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p1", 1, 2), s1)
	first := engine.Handle(queueReq("p2", 2, 2), s2)
	if first.Type != "invited" {
		t.Fatalf("first match = %#v", first)
	}
	if resp := engine.Handle(Request{Type: "reject", PlayerID: "p1", RoomID: first.RoomID}, s1); resp.Type != "rejected" {
		t.Fatalf("reject = %#v", resp)
	}
	second := engine.Handle(queueReq("p3", 2, 2), s3)
	if second.Type != "invited" || second.RoomID == first.RoomID {
		t.Fatalf("new match = %#v", second)
	}
	resp := engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: first.RoomID}, s2)
	if resp.Reason != "not_invited" {
		t.Fatalf("old invitation confirmed new room: %#v", resp)
	}
	if room := engine.rooms[second.RoomID]; room == nil || room.Members["p2"].Status != StatusInvited {
		t.Fatalf("new room changed by stale invite: %#v", room)
	}
	_ = now
}

func TestServerCloseUnblocksQueuedConnectionAndLoop(t *testing.T) {
	engine, err := NewEngine(NewStore(filepath.Join(t.TempDir(), "state.json")), nil)
	if err != nil {
		t.Fatal(err)
	}
	server := newServer(engine, nil, time.Hour)
	clientConn, serverConn := net.Pipe()
	server.ServeConn(serverConn)
	_, _ = io.WriteString(clientConn, `{"type":"hello","player_id":"lone"}`+"\n")
	reader := bufio.NewReader(clientConn)
	helloLine, err := reader.ReadBytes('\n')
	if err != nil {
		t.Fatal(err)
	}
	var hello Response
	if err := json.Unmarshal(helloLine, &hello); err != nil {
		t.Fatal(err)
	}
	_, _ = io.WriteString(clientConn, `{"type":"queue","session_id":"`+hello.SessionID+`","player_id":"lone","mode":"ranked","level":1,"room_size":2}`+"\n")
	if _, err := reader.ReadBytes('\n'); err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() {
		_, err := reader.ReadBytes('\n')
		done <- err
	}()
	if err := server.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("connection stayed open after server close")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("queued connection was not unblocked by shutdown")
	}
	_ = clientConn.Close()
}
