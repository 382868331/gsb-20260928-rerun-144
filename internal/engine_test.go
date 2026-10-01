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
	if record := restarted.SessionRecord(sessions["p3"]); record.Result != StatusActive || record.RoomID != roomID {
		t.Fatalf("restored session record = %#v", record)
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

func TestRefilledInvitationGetsFreshDeadline(t *testing.T) {
	engine, now := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p1", 10, 2), s1)
	roomID := engine.Handle(queueReq("p2", 11, 2), s2).RoomID
	engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: roomID}, s1)
	engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: roomID}, s2)
	engine.Handle(queueReq("p3", 12, 2), s3)
	originalDeadline := engine.rooms[roomID].DeadlineUnixMS
	engine.Disconnect(s2)
	refillDeadline := engine.rooms[roomID].DeadlineUnixMS
	if refillDeadline <= originalDeadline {
		t.Fatalf("refill deadline %d was not refreshed from %d", refillDeadline, originalDeadline)
	}
	if refillDeadline != now.Add(10*time.Second).UnixMilli() {
		t.Fatalf("refill deadline = %d", refillDeadline)
	}
}
