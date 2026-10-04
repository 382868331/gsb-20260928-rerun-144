package internal

import (
	"os"
	"path/filepath"
	"strings"
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
	engine.reserveTTL = 2 * time.Second
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
	if room == nil || room.Members["p1"] == nil || !room.Members["p1"].Disconnected {
		t.Fatalf("confirmed seat was not reserved on disconnect: %#v", room)
	}
	if room.Members["p3"] != nil {
		t.Fatalf("reserved seat was handed out before expiry: %#v", room)
	}
	*now = now.Add(3 * time.Second)
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

func activeRoom(t *testing.T, engine *Engine, sessions map[string]string, players ...string) string {
	t.Helper()
	var roomID string
	for i, player := range players {
		resp := engine.Handle(queueReq(player, 10+i, 2), sessions[player])
		if resp.RoomID != "" {
			roomID = resp.RoomID
		}
	}
	for _, player := range players {
		engine.Handle(Request{Type: "confirm", PlayerID: player, RoomID: roomID}, sessions[player])
	}
	if room := engine.rooms[roomID]; room == nil || room.Status != StatusActive {
		t.Fatalf("room not active: %#v", engine.rooms[roomID])
	}
	return roomID
}

func TestFinalConfirmConvergesAndDuplicateIsIdempotent(t *testing.T) {
	engine, _ := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	engine.Handle(queueReq("p1", 10, 2), s1)
	invited := engine.Handle(queueReq("p2", 11, 2), s2)
	first := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: invited.RoomID}, s1)
	if first.Type != "room_state" || first.Status != StatusProposed {
		t.Fatalf("non-final confirm = %#v", first)
	}
	final := engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: invited.RoomID}, s2)
	if final.Type != "confirmed" || final.Status != StatusActive {
		t.Fatalf("final confirm = %#v", final)
	}
	states := engine.PublicRoomStates()
	if states["p1"].Type != "confirmed" || states["p1"].Status != StatusActive ||
		states["p2"].Type != "confirmed" || states["p2"].Status != StatusActive {
		t.Fatalf("participants see different decisions: %#v", states)
	}
	dup := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: invited.RoomID}, s1)
	if dup.Type != "confirmed" || dup.Status != StatusActive {
		t.Fatalf("duplicate confirm regressed the room: %#v", dup)
	}
}

func TestReservedSeatSurvivesReconnectAndIgnoresStaleDisconnect(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := activeRoom(t, engine, sessions, "p1", "p2")
	engine.Disconnect(sessions["p1"])
	resumedSession, resp := engine.Hello("p1")
	if resp.Type != "resumed" || resp.RoomID != roomID || resp.Status != StatusActive {
		t.Fatalf("resume response = %#v", resp)
	}
	engine.Disconnect(sessions["p1"])
	room := engine.rooms[roomID]
	if room.Members["p1"] == nil || room.Members["p1"].Disconnected {
		t.Fatalf("stale disconnect touched the new identity: %#v", room.Members["p1"])
	}
	engine.Disconnect(resumedSession)
	if room.Members["p1"] == nil || !room.Members["p1"].Disconnected {
		t.Fatalf("current disconnect did not reserve the seat: %#v", room.Members["p1"])
	}
	*now = now.Add(3 * time.Second)
	engine.Tick()
	if engine.rooms[roomID].Members["p1"] != nil {
		t.Fatalf("reserved seat did not expire: %#v", engine.rooms[roomID].Members)
	}
	if engine.Result("p1") != StatusCanceled {
		t.Fatalf("expired seat result = %q", engine.Result("p1"))
	}
}

func TestCanceledRoomIsNotRefilled(t *testing.T) {
	engine, _ := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	s3, _ := engine.Hello("p3")
	engine.Handle(queueReq("p1", 10, 2), s1)
	engine.Handle(queueReq("p2", 11, 2), s2)
	engine.Handle(queueReq("p3", 20, 2), s3)
	engine.Shutdown()
	s4, _ := engine.Hello("p4")
	if resp := engine.Handle(queueReq("p4", 10, 2), s4); resp.Type != "queued" {
		t.Fatalf("new player was placed into a canceled room: %#v", resp)
	}
	engine.Tick()
	for id, room := range engine.rooms {
		if room.Status == StatusCanceled && room.Members["p4"] != nil {
			t.Fatalf("canceled room %s reopened by fill: %#v", id, room.Members)
		}
	}
}

func findSeat(resp Response, player string) SeatState {
	for _, seat := range resp.Seats {
		if seat.PlayerID == player {
			return seat
		}
	}
	return SeatState{Status: "missing"}
}

func TestSeatStatesShowOnlineReservedVacantAndConfirmed(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2", "p3"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := activeRoom(t, engine, sessions, "p1", "p2")
	engine.Handle(queueReq("p3", 12, 2), sessions["p3"])

	engine.Disconnect(sessions["p1"])
	states := engine.PublicRoomStates()
	p1Seat := findSeat(states["p1"], "p1")
	if p1Seat.Status != StatusReserved || p1Seat.Online || p1Seat.DeadlineMS != now.UnixMilli()+engine.reserveTTL.Milliseconds() {
		t.Fatalf("reserved seat state = %#v", p1Seat)
	}
	p2Seat := findSeat(states["p2"], "p2")
	if p2Seat.Status != StatusConfirmed || !p2Seat.Online {
		t.Fatalf("online confirmed seat = %#v", p2Seat)
	}
	if resp := states["p3"]; resp.Type != "queued" {
		t.Fatalf("waiting player = %#v", resp)
	}

	*now = now.Add(3 * time.Second)
	engine.Tick()
	states = engine.PublicRoomStates()
	p3Seat := findSeat(states["p3"], "p3")
	if p3Seat.Status != StatusInvited || !p3Seat.Online || p3Seat.DeadlineMS == 0 {
		t.Fatalf("backfill invitation seat = %#v", p3Seat)
	}
	final := engine.Handle(Request{Type: "confirm", PlayerID: "p3", RoomID: roomID}, sessions["p3"])
	if final.Type != "confirmed" || final.DeadlineMS != 0 {
		t.Fatalf("backfill confirm = %#v", final)
	}
	room := engine.rooms[roomID]
	if room.DeadlineUnixMS != 0 {
		t.Fatalf("stale proposal deadline kept after refill: %d", room.DeadlineUnixMS)
	}
	data, err := os.ReadFile(engine.store.path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), "deadline_unix_ms\": 0") {
		t.Fatalf("persisted state kept cleared deadline field: %s", data)
	}

	engine.Disconnect(sessions["p3"])
	*now = now.Add(3 * time.Second)
	engine.Tick()
	states = engine.PublicRoomStates()
	if vacant := findVacantSeat(states["p2"]); vacant.Status != StatusVacant {
		t.Fatalf("released seat without a waiter was not vacant: %#v", states["p2"].Seats)
	}

	restarted, err := NewEngine(engine.store, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetTimeouts(10*time.Second, 15*time.Second, 2*time.Second)
	restored := restarted.rooms[roomID]
	if restored == nil || restored.DeadlineUnixMS != 0 || restored.Status != StatusActive {
		t.Fatalf("restored room kept stale deadline: %#v", restored)
	}
}

func findVacantSeat(resp Response) SeatState {
	for _, seat := range resp.Seats {
		if seat.Status == StatusVacant {
			return seat
		}
	}
	return SeatState{Status: "missing"}
}

func TestRestartKeepsReservationBoundary(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2", "p3"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := activeRoom(t, engine, sessions, "p1", "p2")
	engine.Handle(queueReq("p3", 12, 2), sessions["p3"])
	disconnectAt := now.Add(time.Second)
	*now = disconnectAt
	engine.Disconnect(sessions["p1"])

	restartedAt := disconnectAt.Add(500 * time.Millisecond)
	restarted, err := NewEngine(engine.store, func() time.Time { return restartedAt })
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetTimeouts(10*time.Second, 15*time.Second, 2*time.Second)
	room := restarted.rooms[roomID]
	if room == nil || room.Members["p1"] == nil || !room.Members["p1"].Disconnected {
		t.Fatalf("reserved seat lost on restart: %#v", room)
	}
	if room.Members["p1"].DisconnectedSinceUnixMS != disconnectAt.UnixMilli() {
		t.Fatalf("reservation boundary moved on restart: %#v", room.Members["p1"])
	}
	_, helloResp := restarted.Hello("p1")
	if helloResp.Type != "resumed" || helloResp.Status != StatusActive {
		t.Fatalf("reserved-seat reconnect = %#v", helloResp)
	}
}

func TestDuplicateConfirmInterleavedWithBackfillKeepsRoomActive(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2", "p3"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := activeRoom(t, engine, sessions, "p1", "p2")
	engine.Handle(queueReq("p3", 12, 2), sessions["p3"])
	engine.Disconnect(sessions["p2"])
	*now = now.Add(3 * time.Second)
	engine.Tick()
	engine.Handle(Request{Type: "confirm", PlayerID: "p3", RoomID: roomID}, sessions["p3"])

	oldSession := sessions["p2"]
	newSession, _ := engine.Hello("p2")
	if resp := engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: roomID}, oldSession); resp.Reason != "stale_session" {
		t.Fatalf("stale session after refill = %#v", resp)
	}
	if resp := engine.Handle(Request{Type: "confirm", PlayerID: "p2", RoomID: roomID}, newSession); resp.Type != "error" || resp.Reason != "not_invited" {
		t.Fatalf("new session without seat acted on room: %#v", resp)
	}
	dup := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: roomID}, sessions["p1"])
	if dup.Type != "confirmed" || dup.Status != StatusActive {
		t.Fatalf("duplicate confirm regressed room: %#v", dup)
	}
	room := engine.rooms[roomID]
	if room.Status != StatusActive || room.Members["p2"] != nil || room.Members["p3"] == nil || room.DeadlineUnixMS != 0 {
		t.Fatalf("final room state = %#v", room)
	}
}
