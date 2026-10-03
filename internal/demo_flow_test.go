package internal

import (
	"path/filepath"
	"testing"
	"time"
)

func TestDemoFlowSimulation(t *testing.T) {
	now := time.Date(2026, 10, 4, 0, 0, 0, 0, time.UTC)
	nowPtr := &now
	newEngine := func() *Engine {
		e, err := NewEngine(NewStore(filepath.Join(t.TempDir(), "state.json")), func() time.Time { return *nowPtr })
		if err != nil {
			t.Fatal(err)
		}
		e.SetTimeouts(400*time.Millisecond, 800*time.Millisecond, 500*time.Millisecond)
		return e
	}
	e := newEngine()
	s := map[string]string{}
	hello := func(p string) { s[p], _ = e.Hello(p) }
	hello("p1")
	hello("p2")
	hello("p3")
	e.Handle(queueReq("p1", 10, 2), s["p1"])
	inv := e.Handle(queueReq("p2", 11, 2), s["p2"])
	room1 := inv.RoomID
	e.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: room1}, s["p1"])
	dup := e.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: room1}, s["p1"])
	if dup.Type != "confirmed" || dup.Status != StatusProposed {
		t.Fatalf("dup = %#v", dup)
	}
	fin := e.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: room1}, s["p2"])
	if fin.Status != StatusActive {
		t.Fatalf("final = %#v", fin)
	}
	e.Handle(queueReq("p3", 12, 2), s["p3"])
	e.Disconnect(s["p2"])
	e.Tick()
	if e.rooms[room1].Members["p3"] != nil {
		t.Fatal("reserved seat filled early")
	}
	if st := e.PublicRoomStates()["p1"]; len(st.Reserved) != 1 || st.Reserved[0] != "p2" || st.Status != StatusActive {
		t.Fatalf("p1 state = %#v", st)
	}
	// reconnect within reserve
	if _, resp := e.Hello("p2"); resp.Type != "resumed" || resp.Status != StatusActive {
		t.Fatalf("resume = %#v", resp)
	}
	// disconnect again, expire
	e.Disconnect(e.CurrentSession("p2"))
	*nowPtr = nowPtr.Add(600 * time.Millisecond)
	e.Tick()
	if e.rooms[room1].Members["p3"] == nil {
		t.Fatal("p3 not backfilled after expiry")
	}
	e.Handle(Request{Type: "confirm", PlayerID: "p3", RoomID: room1}, s["p3"])
	if e.rooms[room1].Status != StatusActive {
		t.Fatal("room not active after refill confirm")
	}
	// p2 late acts
	if _, resp := e.Hello("p2"); resp.Type == "resumed" {
		t.Fatal("p2 resumed expired seat")
	}
	if resp := e.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: room1}, s["p2"]); resp.Type != "error" {
		t.Fatalf("late confirm = %#v", resp)
	}
	// phase 4: both leave, room dissolves
	e.Disconnect(e.CurrentSession("p1"))
	e.Disconnect(e.CurrentSession("p3"))
	hello("p4")
	hello("p5")
	e.Handle(queueReq("p4", 11, 2), s["p4"])
	e.Tick()
	if e.rooms[room1].Members["p4"] != nil {
		t.Fatal("p4 joined reserved room")
	}
	*nowPtr = nowPtr.Add(600 * time.Millisecond)
	e.Tick()
	if e.rooms[room1] != nil {
		t.Fatal("finished room reopened")
	}
	inv2 := e.Handle(queueReq("p5", 12, 2), s["p5"])
	if inv2.Type != "invited" || inv2.RoomID == room1 {
		t.Fatalf("rematch = %#v", inv2)
	}
	e.Handle(Request{Type: "confirm", PlayerID: "p4", RoomID: inv2.RoomID}, s["p4"])
	e.Handle(Request{Type: "confirm", PlayerID: "p5", RoomID: inv2.RoomID}, s["p5"])
	// restart
	e2, err := NewEngine(e.store, func() time.Time { return *nowPtr })
	if err != nil {
		t.Fatal(err)
	}
	if e2.rooms[inv2.RoomID] == nil || e2.rooms[inv2.RoomID].Status != StatusActive {
		t.Fatal("restart lost active room")
	}
	if _, resp := e2.Hello("p4"); resp.Type != "resumed" || resp.Status != StatusActive {
		t.Fatalf("restart resume = %#v", resp)
	}
	if e2.Result("p1") != StatusActive || e2.Result("p2") != StatusActive {
		t.Fatalf("results = %q %q", e2.Result("p1"), e2.Result("p2"))
	}
}
