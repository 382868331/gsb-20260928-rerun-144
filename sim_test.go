package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"
)

// vclient is a local virtual client speaking the wire protocol over an
// in-memory pipe (the sandbox forbids TCP sockets in tests).
type vclient struct {
	t      *testing.T
	conn   net.Conn
	enc    *json.Encoder
	mu     sync.Mutex
	inbox  []OutMessage
	notify chan struct{}
}

type testEnv struct {
	srv  *Server
	dir  string
	stop func()
}

func startServer(t *testing.T, dir string) *testEnv {
	t.Helper()
	srv := NewServer(NewStore(dir), 2, 400*time.Millisecond)
	if err := srv.checkInvariants(); err != nil {
		t.Fatalf("restart invariant: %v", err)
	}
	go srv.Run()
	env := &testEnv{srv: srv, dir: dir}
	env.stop = func() { srv.Stop() }
	t.Cleanup(env.stop)
	return env
}

func (e *testEnv) newClient(t *testing.T, playerID string) *vclient {
	t.Helper()
	serverEnd, clientEnd := net.Pipe()
	go serveConn(e.srv, serverEnd, e.srv.newSession())
	c := &vclient{t: t, conn: clientEnd, enc: json.NewEncoder(clientEnd), notify: make(chan struct{}, 64)}
	go func() {
		sc := bufio.NewScanner(clientEnd)
		for sc.Scan() {
			var m OutMessage
			if json.Unmarshal(sc.Bytes(), &m) == nil {
				c.mu.Lock()
				c.inbox = append(c.inbox, m)
				c.mu.Unlock()
				c.notify <- struct{}{}
			}
		}
	}()
	c.send(InMessage{Type: "hello", PlayerID: playerID})
	c.waitFor("welcome", 2*time.Second)
	return c
}

func (c *vclient) send(m InMessage) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if err := c.enc.Encode(m); err != nil {
		c.t.Fatalf("send %s: %v", m.Type, err)
	}
}

func (c *vclient) waitFor(typ string, d time.Duration) OutMessage {
	c.t.Helper()
	deadline := time.Now().Add(d)
	seen := 0
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for i := seen; i < len(c.inbox); i++ {
			if c.inbox[i].Type == typ {
				m := c.inbox[i]
				c.inbox = append(c.inbox[:i], c.inbox[i+1:]...)
				c.mu.Unlock()
				return m
			}
		}
		seen = len(c.inbox)
		c.mu.Unlock()
		select {
		case <-c.notify:
		case <-time.After(time.Until(deadline)):
		}
	}
	c.t.Fatalf("timeout waiting for %s", typ)
	return OutMessage{}
}

func (c *vclient) expectNo(typ string, d time.Duration) {
	c.t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		c.mu.Lock()
		for _, m := range c.inbox {
			if m.Type == typ {
				c.mu.Unlock()
				c.t.Fatalf("unexpected %s", typ)
			}
		}
		c.mu.Unlock()
		select {
		case <-c.notify:
		case <-time.After(time.Until(deadline)):
		}
	}
}

func (c *vclient) close() { c.conn.Close() }

// 1. Conditional matchmaking + confirm join.
func TestMatchAndConfirm(t *testing.T) {
	env := startServer(t, t.TempDir())
	a := env.newClient(t, "alice")
	b := env.newClient(t, "bob")
	c := env.newClient(t, "carol")

	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	a.expectNo("match_offer", 150*time.Millisecond) // condition not met yet

	b.send(InMessage{Type: "enqueue", Mode: "ranked"}) // different mode: no match
	b.waitFor("queued", time.Second)
	a.expectNo("match_offer", 150*time.Millisecond)

	c.send(InMessage{Type: "enqueue", Mode: "duo"})
	c.waitFor("queued", time.Second)
	oa := a.waitFor("match_offer", time.Second)
	oc := c.waitFor("match_offer", time.Second)
	if oa.RoomID != oc.RoomID {
		t.Fatal("alice and carol should share a room")
	}
	b.expectNo("match_offer", 150*time.Millisecond)

	a.send(InMessage{Type: "confirm", RoomID: oa.RoomID, Token: oa.Token})
	a.waitFor("confirm_ok", time.Second)
	c.send(InMessage{Type: "confirm", RoomID: oc.RoomID, Token: oc.Token})
	c.waitFor("confirm_ok", time.Second)
	a.waitFor("room_confirmed", time.Second)
	c.waitFor("room_confirmed", time.Second)
}

// 2. A player cannot hold seats in two confirmed rooms.
func TestNoDoubleConfirmedRoom(t *testing.T) {
	env := startServer(t, t.TempDir())
	a := env.newClient(t, "alice")
	b := env.newClient(t, "bob")
	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	b.send(InMessage{Type: "enqueue", Mode: "duo"})
	b.waitFor("queued", time.Second)
	oa := a.waitFor("match_offer", time.Second)
	ob := b.waitFor("match_offer", time.Second)
	a.send(InMessage{Type: "confirm", RoomID: oa.RoomID, Token: oa.Token})
	a.waitFor("confirm_ok", time.Second)
	b.send(InMessage{Type: "confirm", RoomID: ob.RoomID, Token: ob.Token})
	b.waitFor("confirm_ok", time.Second)
	a.waitFor("room_confirmed", time.Second)

	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	m := a.waitFor("error", time.Second)
	if m.Reason != "already_in_room:"+oa.RoomID {
		t.Fatalf("want already_in_room, got %s", m.Reason)
	}
}

// 3. Disconnect after match releases the seat and notifies the waiting peer.
func TestDisconnectReleasesAndNotifies(t *testing.T) {
	env := startServer(t, t.TempDir())
	a := env.newClient(t, "alice")
	b := env.newClient(t, "bob")
	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	b.send(InMessage{Type: "enqueue", Mode: "duo"})
	b.waitFor("queued", time.Second)
	a.waitFor("match_offer", time.Second)
	b.waitFor("match_offer", time.Second)

	b.close() // bob drops before confirming

	m := a.waitFor("match_aborted", time.Second)
	if m.Reason != "player_disconnected:bob" {
		t.Fatalf("unexpected reason %s", m.Reason)
	}
	a.waitFor("queued", time.Second) // alice is back in the queue

	// A new player can take the released seat.
	c := env.newClient(t, "carol")
	c.send(InMessage{Type: "enqueue", Mode: "duo"})
	c.waitFor("queued", time.Second)
	oa := a.waitFor("match_offer", time.Second)
	oc := c.waitFor("match_offer", time.Second)
	if oa.RoomID != oc.RoomID {
		t.Fatal("alice and carol should be matched after release")
	}
}

// 4. Cancel racing with matchmaking has a fixed, serialized outcome.
func TestCancelVsMatchDeterministic(t *testing.T) {
	env := startServer(t, t.TempDir())
	a := env.newClient(t, "alice")

	// Case A: cancel while still queued -> cancel wins.
	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	a.send(InMessage{Type: "cancel"})
	m := a.waitFor("cancelled", time.Second)
	if !m.OK {
		t.Fatal("cancel should succeed while queued")
	}

	// Case B: match happens first -> cancel loses with already_matched.
	b := env.newClient(t, "bob")
	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	b.send(InMessage{Type: "enqueue", Mode: "duo"})
	b.waitFor("queued", time.Second)
	a.waitFor("match_offer", time.Second)
	a.send(InMessage{Type: "cancel"})
	m = a.waitFor("cancel_failed", time.Second)
	if m.Reason != "already_matched" {
		t.Fatalf("want already_matched, got %s", m.Reason)
	}
}

// 5. An old connection cannot confirm on behalf of a new session.
func TestStaleSessionCannotConfirm(t *testing.T) {
	env := startServer(t, t.TempDir())
	old := env.newClient(t, "alice")
	b := env.newClient(t, "bob")
	old.send(InMessage{Type: "enqueue", Mode: "duo"})
	old.waitFor("queued", time.Second)
	b.send(InMessage{Type: "enqueue", Mode: "duo"})
	b.waitFor("queued", time.Second)
	offer := old.waitFor("match_offer", time.Second)
	ob := b.waitFor("match_offer", time.Second)

	// Alice reconnects: new session supersedes the old connection.
	fresh := env.newClient(t, "alice")

	// The old connection tries to confirm with its (now stale) token.
	old.send(InMessage{Type: "confirm", RoomID: offer.RoomID, Token: offer.Token})
	m := old.waitFor("error", time.Second)
	if m.Reason != "stale_session" {
		t.Fatalf("want stale_session, got %s", m.Reason)
	}

	// The new connection also cannot reuse the old token.
	fresh.send(InMessage{Type: "confirm", RoomID: offer.RoomID, Token: offer.Token})
	m = fresh.waitFor("confirm_failed", time.Second)
	if m.Reason != "stale_token" {
		t.Fatalf("want stale_token, got %s", m.Reason)
	}

	// Bob's confirm alone cannot complete the room; it times out and the
	// still-connected member is released and notified.
	b.send(InMessage{Type: "confirm", RoomID: ob.RoomID, Token: ob.Token})
	b.waitFor("confirm_ok", time.Second)
	b.waitFor("match_aborted", 2*time.Second)
	b.waitFor("queued", time.Second)
}

// 6. Persist state, restart, verify no duplicate seats or stuck players.
func TestRestartPersistence(t *testing.T) {
	dir := t.TempDir()
	env := startServer(t, dir)

	a := env.newClient(t, "alice")
	b := env.newClient(t, "bob")
	c := env.newClient(t, "carol")
	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	b.send(InMessage{Type: "enqueue", Mode: "duo"})
	b.waitFor("queued", time.Second)
	oa := a.waitFor("match_offer", time.Second)
	ob := b.waitFor("match_offer", time.Second)
	a.send(InMessage{Type: "confirm", RoomID: oa.RoomID, Token: oa.Token})
	a.waitFor("confirm_ok", time.Second)
	b.send(InMessage{Type: "confirm", RoomID: ob.RoomID, Token: ob.Token})
	b.waitFor("confirm_ok", time.Second)
	a.waitFor("room_confirmed", time.Second)
	c.send(InMessage{Type: "enqueue", Mode: "duo"})
	c.waitFor("queued", time.Second)

	// Stop everything: simulate a crash+restart.
	a.close()
	b.close()
	c.close()
	env.stop()
	time.Sleep(100 * time.Millisecond)

	if _, err := NewStore(dir).Load(); err != nil {
		t.Fatalf("state file missing: %v", err)
	}

	env2 := startServer(t, dir)
	if err := env2.srv.checkInvariants(); err != nil {
		t.Fatalf("post-restart invariant: %v", err)
	}

	// Confirmed room survives; alice is still bound to exactly one room.
	a2 := env2.newClient(t, "alice")
	a2.send(InMessage{Type: "status"})
	st := a2.waitFor("status", time.Second)
	if st.Reason != StatusInRoom || st.RoomID != oa.RoomID {
		t.Fatalf("alice should resume in room %s, got %s/%s", oa.RoomID, st.Reason, st.RoomID)
	}
	a2.send(InMessage{Type: "enqueue", Mode: "duo"})
	if m := a2.waitFor("error", time.Second); m.Reason != "already_in_room:"+oa.RoomID {
		t.Fatalf("duplicate seat possible: %s", m.Reason)
	}

	// Carol was only queued: not stuck, free to enqueue again.
	c2 := env2.newClient(t, "carol")
	c2.send(InMessage{Type: "status"})
	st = c2.waitFor("status", time.Second)
	if st.Reason != StatusIdle {
		t.Fatalf("carol stuck in status %s", st.Reason)
	}
	c2.send(InMessage{Type: "enqueue", Mode: "duo"})
	c2.waitFor("queued", time.Second)

	// Nobody else holds a confirmed seat: exactly one room with 2 members.
	confirmed := 0
	for _, r := range env2.srv.rooms {
		if r.State == RoomConfirmed {
			confirmed++
			if len(r.Slots) != 2 {
				t.Fatalf("room %s has %d slots", r.ID, len(r.Slots))
			}
		}
	}
	if confirmed != 1 {
		t.Fatalf("want 1 confirmed room, got %d", confirmed)
	}
	fmt.Println("restart check passed")
}

// 7. Confirm timeout releases seats and notifies the waiting players.
func TestConfirmTimeout(t *testing.T) {
	env := startServer(t, t.TempDir())
	a := env.newClient(t, "alice")
	b := env.newClient(t, "bob")
	a.send(InMessage{Type: "enqueue", Mode: "duo"})
	a.waitFor("queued", time.Second)
	b.send(InMessage{Type: "enqueue", Mode: "duo"})
	b.waitFor("queued", time.Second)
	oa := a.waitFor("match_offer", time.Second)
	b.waitFor("match_offer", time.Second)
	a.send(InMessage{Type: "confirm", RoomID: oa.RoomID, Token: oa.Token})
	a.waitFor("confirm_ok", time.Second)
	// bob never confirms -> deadline fires
	a.waitFor("match_aborted", 2*time.Second)
	a.waitFor("queued", time.Second)
	b.waitFor("match_aborted", 2*time.Second)
	b.waitFor("queued", time.Second)
}
