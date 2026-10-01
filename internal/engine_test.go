package internal

import (
	"path/filepath"
	"testing"
	"time"
)

func testEngine(t *testing.T) (*Engine, *time.Time) {
	t.Helper()
	now := time.Date(2026, 9, 29, 12, 0, 0, 0, time.UTC)
	store := NewStore(filepath.Join(t.TempDir(), "state.json"))
	engine, err := NewEngine(store, func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	engine.proposalTTL = 10 * time.Second
	engine.emptyTTL = 15 * time.Second
	return engine, &now
}

func queueReq(player string, level, size int) Request {
	return Request{Type: "queue", PlayerID: player, Mode: "ranked", Level: level, RoomSize: size}
}

func TestMatchConfirmDisconnectRefillAndRestart(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2", "p3"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	if resp := engine.Handle(queueReq("p1", 10, 2), sessions["p1"]); resp.Type != "queued" {
		t.Fatalf("p1 initial queue = %s %s", resp.Type, resp.Reason)
	}
	p2 := engine.Handle(queueReq("p2", 11, 2), sessions["p2"])
	if p2.Type != "invited" || p2.RoomID == "" {
		t.Fatalf("p2 not invited: %#v", p2)
	}
	roomID := p2.RoomID
	if resp := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: roomID}, sessions["p1"]); resp.Status != StatusProposed {
		t.Fatalf("p1 confirm status = %s", resp.Status)
	}
	if resp := engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: roomID}, sessions["p2"]); resp.Status != StatusActive {
		t.Fatalf("room status = %s", resp.Status)
	}
	if resp := engine.Handle(queueReq("p3", 12, 2), sessions["p3"]); resp.Type != "queued" {
		t.Fatalf("p3 queue = %s", resp.Type)
	}
	engine.Disconnect(sessions["p1"])
	room := engine.rooms[roomID]
	if room == nil || room.Members["p3"] == nil || room.Members["p3"].Status != StatusInvited {
		t.Fatalf("p3 was not offered released slot: %#v", room)
	}
	if resp := engine.Handle(Request{Type: "confirm", PlayerID: "p3", RoomID: roomID}, sessions["p3"]); resp.Status != StatusActive {
		t.Fatalf("refilled room status = %s %s", resp.Status, resp.Reason)
	}

	restarted, err := NewEngine(engine.store, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	restarted.proposalTTL = 10 * time.Second
	restarted.emptyTTL = 15 * time.Second
	restored := restarted.rooms[roomID]
	if restored == nil || restored.Status != StatusActive || restored.Members["p3"] == nil || restored.Members["p1"] != nil {
		t.Fatalf("restored room = %#v", restored)
	}
}

func TestStaleSessionCannotConfirm(t *testing.T) {
	engine, _ := testEngine(t)
	old, _ := engine.Hello("p1")
	newSession, _ := engine.Hello("p1")
	if old == newSession {
		t.Fatal("session was not replaced")
	}
	resp := engine.Handle(queueReq("p1", 1, 2), old)
	if resp.Reason != "stale_session" {
		t.Fatalf("old session result = %#v", resp)
	}
}

func TestOldInvitationCannotConfirmRematchedRoom(t *testing.T) {
	engine, now := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	engine.Handle(queueReq("p1", 10, 2), s1)
	first := engine.Handle(queueReq("p2", 11, 2), s2)
	if first.Type != "invited" || first.RoomID != "room-1" {
		t.Fatalf("first invitation = %#v", first)
	}
	engine.Handle(Request{Type: "reject", PlayerID: "p2", RoomID: first.RoomID}, s2)
	*now = now.Add(11 * time.Second)
	engine.Tick()

	s3, _ := engine.Hello("p3")
	s4, _ := engine.Hello("p4")
	engine.Handle(queueReq("p3", 20, 2), s3)
	second := engine.Handle(queueReq("p4", 21, 2), s4)
	if second.Type != "invited" || second.RoomID != "room-2" {
		t.Fatalf("second invitation = %#v", second)
	}
	resp := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: first.RoomID}, s1)
	if resp.Type != "error" || resp.Reason != "not_invited" {
		t.Fatalf("old invite confirmation = %#v", resp)
	}
	if room := engine.rooms[second.RoomID]; room.Members["p1"] != nil {
		t.Fatalf("old invitation inserted p1 into new room: %#v", room.Members)
	}
}

func TestResultsPersistAndShutdownClearsWaiters(t *testing.T) {
	engine, now := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p1", 10, 2), s1)
	room := engine.Handle(queueReq("p2", 11, 2), s2)
	engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: room.RoomID}, s1)
	engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: room.RoomID}, s2)
	engine.Handle(queueReq("p3", 20, 2), s3)
	canceled := engine.Shutdown()
	if len(canceled) != 1 || canceled[0] != "p3" {
		t.Fatalf("shutdown canceled = %v", canceled)
	}
	if len(engine.queue) != 0 || engine.Result("p3") != StatusCanceled {
		t.Fatalf("waiting state after shutdown: queue=%v result=%q", engine.queue, engine.Result("p3"))
	}

	restarted, err := NewEngine(engine.store, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Result("p1") != StatusActive || restarted.Result("p2") != StatusActive {
		t.Fatalf("persisted results = p1:%q p2:%q", restarted.Result("p1"), restarted.Result("p2"))
	}
	if restarted.nextRoom != 2 || len(restarted.queue) != 0 {
		t.Fatalf("restart state = nextRoom:%d queue:%v", restarted.nextRoom, restarted.queue)
	}
}

func TestProposalTimeoutReturnsPlayersToQueue(t *testing.T) {
	engine, now := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	engine.Handle(queueReq("p1", 5, 2), s1)
	invited := engine.Handle(queueReq("p2", 6, 2), s2)
	if invited.Type != "invited" {
		t.Fatalf("invite = %#v", invited)
	}
	engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: invited.RoomID}, s1)
	*now = now.Add(11 * time.Second)
	engine.Tick()
	newRoom := engine.rooms["room-2"]
	if newRoom == nil || newRoom.Status != StatusProposed {
		t.Fatalf("timed out players were not rematched: %#v", engine.rooms)
	}
	if newRoom.Members["p1"] == nil || newRoom.Members["p2"] == nil {
		t.Fatalf("new room missing players: %#v", newRoom.Members)
	}
}

func TestIncompatibleEarlierPlayerDoesNotBlockLaterMatch(t *testing.T) {
	engine, _ := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p1", 1, 2), s1)
	engine.Handle(queueReq("p2", 10, 2), s2)
	resp := engine.Handle(queueReq("p3", 11, 2), s3)
	if resp.Type != "invited" {
		t.Fatalf("compatible later players not matched: %#v", resp)
	}
}
