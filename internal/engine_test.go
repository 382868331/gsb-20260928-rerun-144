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

func findSeat(t *testing.T, seats []SeatState, player string) SeatState {
	t.Helper()
	for _, seat := range seats {
		if seat.PlayerID == player {
			return seat
		}
	}
	t.Fatalf("seat for %s not found: %#v", player, seats)
	return SeatState{}
}

func TestSeatStatesExplainReservationAndBackfill(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2", "p3"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := activeRoom(t, engine, sessions, "p1", "p2")
	engine.Handle(queueReq("p3", 12, 2), sessions["p3"])

	engine.Disconnect(sessions["p2"])
	states := engine.PublicRoomStates()
	p1View := states["p1"]
	if p1View.Type != "room_state" || p1View.Status != StatusActive {
		t.Fatalf("reserved room should not look fully confirmed to p1: %#v", p1View)
	}
	p2Seat := findSeat(t, p1View.Seats, "p2")
	if p2Seat.State != SeatReserved || p2Seat.DeadlineMS <= now.UnixMilli() {
		t.Fatalf("p2 seat not marked reserved with deadline: %#v", p2Seat)
	}
	if p1Seat := findSeat(t, p1View.Seats, "p1"); p1Seat.State != SeatOnline {
		t.Fatalf("p1 seat = %#v", p1Seat)
	}
	if len(p1View.Members) != 1 || p1View.Members[0] != "p1" {
		t.Fatalf("confirmed member list hides offline seat: %#v", p1View.Members)
	}

	// Reconnect within the reservation window restores the same seat.
	resumedSession, resumed := engine.Hello("p2")
	sessions["p2"] = resumedSession
	if resumed.Type != "resumed" || findSeat(t, resumed.Seats, "p2").State != SeatOnline {
		t.Fatalf("resume did not restore seat: %#v", resumed)
	}
	states = engine.PublicRoomStates()
	if findSeat(t, states["p1"].Seats, "p2").State != SeatOnline {
		t.Fatalf("p1 still sees p2 as reserved: %#v", states["p1"].Seats)
	}

	// Expiry releases the seat and the waiting player receives an explicit slot.
	engine.Disconnect(resumedSession)
	*now = now.Add(3 * time.Second)
	engine.Tick()
	states = engine.PublicRoomStates()
	p3View := states["p3"]
	if p3View.Type != "invited" || p3View.RoomID != roomID {
		t.Fatalf("p3 refill view = %#v", p3View)
	}
	p3Seat := findSeat(t, p3View.Seats, "p3")
	if p3Seat.State != SeatInvited || p3Seat.DeadlineMS == 0 {
		t.Fatalf("p3 seat not marked invited with deadline: %#v", p3Seat)
	}
	p1View = states["p1"]
	if p1View.Type != "room_state" || p1View.DeadlineMS != 0 {
		t.Fatalf("online member should not see an offer deadline: %#v", p1View)
	}

	final := engine.Handle(Request{Type: "confirm", PlayerID: "p3", RoomID: roomID}, sessions["p3"])
	if final.Type != "confirmed" || final.Status != StatusActive {
		t.Fatalf("refill confirmation = %#v", final)
	}
	room := engine.rooms[roomID]
	if room.DeadlineUnixMS != 0 || room.EmptySinceUnixMS != 0 {
		t.Fatalf("completed refill kept stale proposal timing: deadline=%d empty=%d", room.DeadlineUnixMS, room.EmptySinceUnixMS)
	}
	states = engine.PublicRoomStates()
	if states["p1"].Type != "confirmed" || states["p3"].Type != "confirmed" {
		t.Fatalf("post-refill decisions = %#v %#v", states["p1"], states["p3"])
	}
	if states["p1"].DeadlineMS != 0 {
		t.Fatalf("broadcast still carries proposal deadline: %#v", states["p1"])
	}

	// Duplicate confirm is idempotent and never reopens a proposal.
	dup := engine.Handle(Request{Type: "confirm", PlayerID: "p1", RoomID: roomID}, sessions["p1"])
	if dup.Type != "confirmed" || dup.Status != StatusActive || dup.DeadlineMS != 0 {
		t.Fatalf("duplicate confirm regressed room: %#v", dup)
	}
}

func TestOpenSeatsAreVisibleWithoutGuessing(t *testing.T) {
	engine, _ := testEngine(t)
	s1, _ := engine.Hello("p1")
	s2, _ := engine.Hello("p2")
	s3, _ := engine.Hello("p3")
	for _, q := range []struct {
		id      string
		session string
		level   int
	}{{"p1", s1, 10}, {"p2", s2, 11}, {"p3", s3, 12}} {
		engine.Handle(Request{Type: "queue", PlayerID: q.id, Mode: "ranked", Level: q.level, RoomSize: 3}, q.session)
	}
	roomID := "room-1"
	for _, q := range []struct {
		id      string
		session string
	}{{"p1", s1}, {"p2", s2}, {"p3", s3}} {
		engine.Handle(Request{Type: "confirm", PlayerID: q.id, RoomID: roomID}, q.session)
	}
	if room := engine.rooms[roomID]; room == nil || room.Status != StatusActive || room.Size != 3 {
		t.Fatalf("3-player room not active: %#v", room)
	}
	s4, _ := engine.Hello("p4")
	// A confirmed member leaving voluntarily opens an immediately fillable seat.
	engine.Handle(Request{Type: "cancel", PlayerID: "p3"}, s3)
	states := engine.PublicRoomStates()
	seats := states["p1"].Seats
	if len(seats) != 3 {
		t.Fatalf("seats = %#v", seats)
	}
	open := 0
	for _, seat := range seats {
		if seat.State == SeatOpen {
			open++
		}
	}
	if open != 1 {
		t.Fatalf("expected one open seat: %#v", seats)
	}

	// The waiting player fills the open seat and gets an invitation view.
	engine.Handle(Request{Type: "queue", PlayerID: "p4", Mode: "ranked", Level: 12, RoomSize: 3}, s4)
	engine.Tick()
	states = engine.PublicRoomStates()
	if states["p4"].RoomID != roomID {
		t.Fatalf("p4 was not offered the open seat: %#v", states["p4"])
	}
	if seat := findSeat(t, states["p4"].Seats, "p4"); seat.State != SeatInvited {
		t.Fatalf("p4 fill = %#v", seat)
	}
	if seat := findSeat(t, states["p1"].Seats, "p4"); seat.State != SeatInvited {
		t.Fatalf("p1 cannot explain the pending seat: %#v", states["p1"].Seats)
	}
}

func TestPersistedRoomHasNoStaleDeadlineAfterRefill(t *testing.T) {
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

	restarted, err := NewEngine(engine.store, func() time.Time { return *now })
	if err != nil {
		t.Fatal(err)
	}
	room := restarted.rooms[roomID]
	if room == nil || room.Status != StatusActive || room.DeadlineUnixMS != 0 {
		t.Fatalf("restored room kept stale deadline: %#v", room)
	}
	states := restarted.PublicRoomStates()
	if states["p1"].Type != "confirmed" || states["p1"].DeadlineMS != 0 {
		t.Fatalf("restarted view = %#v", states["p1"])
	}
}

func TestReservationBoundarySurvivesRestartRegardlessOfTTL(t *testing.T) {
	engine, now := testEngine(t)
	sessions := map[string]string{}
	for _, player := range []string{"p1", "p2", "p3"} {
		session, _ := engine.Hello(player)
		sessions[player] = session
	}
	roomID := activeRoom(t, engine, sessions, "p1", "p2")
	engine.Handle(queueReq("p3", 12, 2), sessions["p3"])
	engine.Disconnect(sessions["p2"])
	originalExpiry := now.Add(2 * time.Second).UnixMilli()
	if seat := engine.rooms[roomID].Members["p2"]; seat.ReserveUntilUnixMS != originalExpiry {
		t.Fatalf("reserve boundary = %d want %d", seat.ReserveUntilUnixMS, originalExpiry)
	}

	// Restart with a much shorter TTL: the persisted absolute boundary still wins.
	restartedAt := now.Add(1500 * time.Millisecond)
	restarted, err := NewEngine(engine.store, func() time.Time { return restartedAt })
	if err != nil {
		t.Fatal(err)
	}
	restarted.SetTimeouts(10*time.Second, 15*time.Second, 100*time.Millisecond)
	room := restarted.rooms[roomID]
	if seat := room.Members["p2"]; seat == nil || !seat.Disconnected || seat.ReserveUntilUnixMS != originalExpiry {
		t.Fatalf("reservation boundary changed after restart: %#v", seat)
	}
	restarted.Tick()
	if restarted.rooms[roomID].Members["p2"] == nil || restarted.rooms[roomID].Members["p3"] != nil {
		t.Fatalf("seat released before persisted boundary: %#v", restarted.rooms[roomID].Members)
	}

	restartedAt = now.Add(2500 * time.Millisecond)
	restarted.now = func() time.Time { return restartedAt }
	restarted.Tick()
	room = restarted.rooms[roomID]
	if room.Members["p2"] != nil || room.Members["p3"] == nil || room.Members["p3"].Status != StatusInvited {
		t.Fatalf("persisted boundary did not release/refill: %#v", room.Members)
	}
}
