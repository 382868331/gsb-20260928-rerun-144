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
	engine.reserveTTL = 5 * time.Second
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
	if room == nil || room.Members["p1"] == nil || room.Members["p1"].ReservedUntilUnixMS == 0 {
		t.Fatalf("confirmed disconnect was not reserved: %#v", room)
	}
	if room.Members["p3"] != nil {
		t.Fatalf("reserved seat was backfilled early: %#v", room.Members)
	}
	*now = now.Add(6 * time.Second)
	engine.Tick()
	room = engine.rooms[roomID]
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

func activeRoom(t *testing.T, engine *Engine, players ...string) (string, map[string]string) {
	t.Helper()
	sessions := map[string]string{}
	for _, player := range players {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := ""
	for i, player := range players {
		resp := engine.Handle(queueReq(player, 10+i, len(players)), sessions[player])
		if i < len(players)-1 {
			continue
		}
		if resp.Type != "invited" {
			t.Fatalf("last player not invited: %#v", resp)
		}
		roomID = resp.RoomID
	}
	for _, player := range players {
		engine.Handle(Request{Type: "confirm", PlayerID: player, RoomID: roomID}, sessions[player])
	}
	if room := engine.rooms[roomID]; room == nil || room.Status != StatusActive {
		t.Fatalf("room not active: %#v", room)
	}
	return roomID, sessions
}

func TestDuplicateConfirmConvergesToFinalStatus(t *testing.T) {
	engine, _ := testEngine(t)
	roomID, sessions := activeRoom(t, engine, "p1", "p2")
	resp := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: roomID}, sessions["p1"])
	if resp.Type != "confirmed" || !resp.OK || resp.Status != StatusActive {
		t.Fatalf("duplicate confirm = %#v", resp)
	}
	if room := engine.rooms[roomID]; room.Status != StatusActive {
		t.Fatalf("duplicate confirm regressed room: %#v", room)
	}
}

func TestReconnectWithinReserveKeepsSeat(t *testing.T) {
	engine, now := testEngine(t)
	roomID, sessions := activeRoom(t, engine, "p1", "p2")
	engine.Disconnect(sessions["p1"])
	room := engine.rooms[roomID]
	if room.Members["p1"] == nil || room.Members["p1"].ReservedUntilUnixMS == 0 {
		t.Fatalf("seat not reserved: %#v", room.Members)
	}
	*now = now.Add(2 * time.Second)
	engine.Tick()
	if engine.rooms[roomID].Members["p1"] == nil {
		t.Fatal("reserved seat released before deadline")
	}
	oldSession := sessions["p1"]
	newSession, resp := engine.Hello("p1")
	if resp.Type != "resumed" || resp.RoomID != roomID || resp.Status != StatusActive {
		t.Fatalf("resume = %#v", resp)
	}
	if engine.rooms[roomID].Members["p1"].ReservedUntilUnixMS != 0 {
		t.Fatal("reservation not cleared on reconnect")
	}
	engine.Disconnect(oldSession)
	if engine.rooms[roomID].Members["p1"] == nil {
		t.Fatal("late disconnect of old connection removed reconnected player")
	}
	*now = now.Add(10 * time.Second)
	engine.Tick()
	if engine.rooms[roomID].Members["p1"] == nil {
		t.Fatal("seat released after successful reconnect")
	}
	_ = newSession
}

func TestReservationExpiryReleasesSeatToWaiter(t *testing.T) {
	engine, now := testEngine(t)
	roomID, sessions := activeRoom(t, engine, "p1", "p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p3", 11, 2), s3)
	engine.Disconnect(sessions["p2"])
	engine.Tick()
	if engine.rooms[roomID].Members["p3"] != nil {
		t.Fatal("reserved seat backfilled before expiry")
	}
	*now = now.Add(6 * time.Second)
	engine.Tick()
	room := engine.rooms[roomID]
	if room.Members["p2"] != nil || room.Members["p3"] == nil || room.Members["p3"].Status != StatusInvited {
		t.Fatalf("expired seat not backfilled: %#v", room.Members)
	}
	if _, resp := engine.Hello("p2"); resp.Type == "resumed" {
		t.Fatal("expired seat still resumed")
	}
	resp := engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: roomID}, sessions["p2"])
	if resp.Type != "error" {
		t.Fatalf("late confirm after expiry = %#v", resp)
	}
	if engine.rooms[roomID].Members["p3"] == nil {
		t.Fatal("late confirm affected new occupant")
	}
}

func TestCompletedRoomIsNotReopenedAfterReservationsExpire(t *testing.T) {
	engine, now := testEngine(t)
	roomID, sessions := activeRoom(t, engine, "p1", "p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p3", 11, 2), s3)
	engine.Disconnect(sessions["p1"])
	engine.Disconnect(sessions["p2"])
	engine.Tick()
	if engine.rooms[roomID].Members["p3"] != nil {
		t.Fatal("waiter joined a fully reserved room")
	}
	*now = now.Add(6 * time.Second)
	engine.Tick()
	if engine.rooms[roomID] != nil {
		t.Fatalf("completed room was reopened: %#v", engine.rooms[roomID])
	}
	if !engine.queued("p3") {
		t.Fatal("waiter lost after room dissolution")
	}
	if engine.Result("p1") != StatusActive || engine.Result("p2") != StatusActive {
		t.Fatalf("final decision erased: %q %q", engine.Result("p1"), engine.Result("p2"))
	}
}

func TestStaleSessionCannotActAfterReconnect(t *testing.T) {
	engine, _ := testEngine(t)
	roomID, sessions := activeRoom(t, engine, "p1", "p2")
	engine.Disconnect(sessions["p1"])
	oldSession := sessions["p1"]
	engine.Hello("p1")
	resp := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: roomID}, oldSession)
	if resp.Reason != "stale_session" {
		t.Fatalf("old session confirm = %#v", resp)
	}
	resp = engine.Handle(Request{Type: "cancel", PlayerID: "p1"}, oldSession)
	if resp.Reason != "stale_session" {
		t.Fatalf("old session cancel = %#v", resp)
	}
	if engine.rooms[roomID].Members["p1"] == nil {
		t.Fatal("old connection acted on new identity")
	}
}
