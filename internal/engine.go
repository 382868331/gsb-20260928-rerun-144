package internal

import (
	"fmt"
	"sort"
	"sync/atomic"
	"time"
)

const (
	StatusInvited   = "invited"
	StatusConfirmed = "confirmed"
	StatusProposed  = "proposed"
	StatusActive    = "active"
	StatusCanceled  = "canceled"

	SeatOnline   = "online"
	SeatReserved = "reserved"
	SeatInvited  = "invited"
	SeatOpen     = "open"
)

type Member struct {
	PlayerID                string
	Status                  string
	Disconnected            bool
	DisconnectedSinceUnixMS int64
	ReserveUntilUnixMS      int64
	JoinSeq                 int64
}

type Room struct {
	ID               string
	Mode             string
	Level            int
	Size             int
	Status           string
	Members          map[string]*Member
	CreatedSeq       int64
	DeadlineUnixMS   int64
	EmptySinceUnixMS int64
}

func (r *Room) confirmedIDs() []string {
	ids := make([]string, 0, len(r.Members))
	for id, member := range r.Members {
		if member.Status == StatusConfirmed && !member.Disconnected {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (r *Room) sortedMembers() []*Member {
	members := make([]*Member, 0, len(r.Members))
	for _, member := range r.Members {
		members = append(members, member)
	}
	sort.Slice(members, func(i, j int) bool {
		if members[i].JoinSeq != members[j].JoinSeq {
			return members[i].JoinSeq < members[j].JoinSeq
		}
		return members[i].PlayerID < members[j].PlayerID
	})
	return members
}

// Seats renders the room in a form any client can interpret without guessing
// whether a missing player is offline, protected or replaced. Occupied seats
// keep their join order; remaining seats are reported as explicitly open.
func (r *Room) Seats() []SeatState {
	seats := make([]SeatState, 0, r.Size)
	for _, member := range r.sortedMembers() {
		seat := SeatState{PlayerID: member.PlayerID}
		switch {
		case member.Disconnected:
			seat.State = SeatReserved
			seat.DeadlineMS = member.ReserveUntilUnixMS
		case member.Status == StatusConfirmed:
			seat.State = SeatOnline
		case member.Status == StatusInvited:
			seat.State = SeatInvited
			seat.DeadlineMS = r.DeadlineUnixMS
		default:
			seat.State = SeatOpen
		}
		seats = append(seats, seat)
	}
	for len(seats) < r.Size {
		seats = append(seats, SeatState{State: SeatOpen})
	}
	return seats
}

func (r *Room) hasInvited() bool {
	for _, member := range r.Members {
		if member.Status == StatusInvited {
			return true
		}
	}
	return false
}

func (r *Room) hasDisconnected() bool {
	for _, member := range r.Members {
		if member.Disconnected {
			return true
		}
	}
	return false
}

// hasConfirmedMember reports whether any seat is owned by a confirmed member,
// including a reserved (temporarily offline) one. Reserved owners still count:
// after a restart every confirmed seat is reserved, and open seats must stay
// eligible for backfill once their reservation expires.
func (r *Room) hasConfirmedMember() bool {
	for _, member := range r.Members {
		if member.Status == StatusConfirmed {
			return true
		}
	}
	return false
}

// roomView builds the externally visible state for a room participant. A room
// only resolves to the final confirmed decision once it is full and every
// member has confirmed; during reservation/backfill it stays a room_state so
// the seats explain exactly who owns each slot.
func (e *Engine) roomView(room *Room, member *Member) Response {
	resolved := room.Status == StatusActive && len(room.Members) == room.Size && !room.hasInvited() && !room.hasDisconnected()
	resp := Response{OK: true, RoomID: room.ID, Mode: room.Mode, RoomSize: room.Size,
		Members: room.confirmedIDs(), Seats: room.Seats()}
	if resolved {
		resp.Type = "confirmed"
		resp.Status = StatusActive
		return resp
	}
	resp.Type = "room_state"
	resp.Status = room.Status
	resp.Reason = member.Status
	if member.Disconnected {
		resp.Reason = SeatReserved
	}
	if member.Status == StatusInvited {
		resp.DeadlineMS = room.DeadlineUnixMS
	} else if member.Disconnected {
		resp.DeadlineMS = member.ReserveUntilUnixMS
	}
	return resp
}

func (r *Room) allConfirmed() bool {
	return len(r.Members) == r.Size && len(r.Members) == len(r.confirmedIDs())
}

type ClientEvent struct {
	Kind      string
	SessionID string
	PlayerID  string
	Request   Request
	Conn      interface{}
	Resp      chan Response
}

type Engine struct {
	store        *Store
	proposalTTL  time.Duration
	emptyTTL     time.Duration
	reserveTTL   time.Duration
	queue        []QueueRecord
	rooms        map[string]*Room
	sessions     map[string]string
	players      map[string]string
	results      map[string]string
	nextSeq      int64
	nextRoom     int64
	now          func() time.Time
	sessionNonce atomic.Int64
}

func NewEngine(store *Store, now func() time.Time) (*Engine, error) {
	if now == nil {
		now = time.Now
	}
	nowMS := now().UnixMilli()
	nextSeq, nextRoom, queue, rooms, results, err := store.Load(nowMS, int64(10*time.Second/time.Millisecond))
	if err != nil {
		return nil, err
	}
	e := &Engine{store: store, proposalTTL: 10 * time.Second, emptyTTL: 15 * time.Second, reserveTTL: 5 * time.Second,
		queue: queue, rooms: rooms, sessions: map[string]string{}, players: map[string]string{}, results: results,
		nextSeq: nextSeq, nextRoom: nextRoom, now: now}
	e.normalizeLoadedRooms(nowMS)
	e.fillVacancies()
	_ = e.save()
	return e, nil
}

// normalizeLoadedRooms makes restart honor the exact boundaries captured in
// the persisted file: expired reservations release the seat, expired refill
// invitations return the replacement to the queue, and a full confirmed room
// never carries an obsolete proposal deadline.
func (e *Engine) normalizeLoadedRooms(nowMS int64) {
	for id, room := range e.rooms {
		if room.Status != StatusActive && room.Status != StatusProposed {
			continue
		}
		for playerID, member := range room.Members {
			// A persisted confirmed seat only proves past ownership, not a live
			// connection in this process. Restore it as reserved until the
			// member actually reconnects, with an explicit reserve deadline.
			if member.Status == StatusConfirmed && !member.Disconnected {
				member.Disconnected = true
				member.DisconnectedSinceUnixMS = nowMS
				member.ReserveUntilUnixMS = nowMS + e.reserveTTL.Milliseconds()
			}
			if member.Disconnected {
				if member.ReserveUntilUnixMS <= 0 {
					member.ReserveUntilUnixMS = member.DisconnectedSinceUnixMS + e.reserveTTL.Milliseconds()
				}
				if nowMS >= member.ReserveUntilUnixMS {
					delete(room.Members, playerID)
					e.setResult(playerID, StatusCanceled)
				}
			}
		}
		if len(room.Members) == 0 {
			delete(e.rooms, id)
			continue
		}
		if room.Status == StatusActive {
			if room.DeadlineUnixMS > 0 && nowMS >= room.DeadlineUnixMS {
				for playerID, member := range room.Members {
					if member.Status == StatusInvited {
						e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
						e.nextSeq++
						delete(room.Members, playerID)
					}
				}
				room.DeadlineUnixMS = 0
			}
			if len(room.Members) == room.Size && !room.hasInvited() {
				room.DeadlineUnixMS = 0
				room.EmptySinceUnixMS = 0
			}
		}
	}
	e.match()
}

func (e *Engine) save() error {
	return e.store.Save(e.nextSeq, e.nextRoom, e.queue, e.rooms, e.results)
}

func (e *Engine) setResult(player, result string) { e.results[player] = result }

func (e *Engine) Shutdown() []string {
	canceled := make([]string, 0)
	for _, room := range e.rooms {
		if room.Status == StatusProposed {
			for playerID := range room.Members {
				canceled = append(canceled, playerID)
				e.setResult(playerID, StatusCanceled)
			}
			room.Status = StatusCanceled
			room.DeadlineUnixMS = 0
		}
	}
	for _, item := range e.queue {
		canceled = append(canceled, item.PlayerID)
		e.setResult(item.PlayerID, StatusCanceled)
	}
	e.queue = nil
	_ = e.save()
	return canceled
}

func (e *Engine) queued(player string) bool {
	for _, item := range e.queue {
		if item.PlayerID == player {
			return true
		}
	}
	return false
}

func (e *Engine) roomOf(player string) *Room {
	for _, room := range e.rooms {
		if _, ok := room.Members[player]; ok {
			return room
		}
	}
	return nil
}

func (e *Engine) Hello(player string) (string, Response) {
	old := e.players[player]
	if old != "" {
		delete(e.sessions, old)
	}
	n := e.sessionNonce.Add(1)
	sessionID := fmt.Sprintf("s-%d-%d", e.now().UnixNano(), n)
	e.sessions[sessionID] = player
	e.players[player] = sessionID
	resp := Response{Type: "hello", OK: true, SessionID: sessionID}
	if result := e.results[player]; result != "" {
		resp.Status = result
	}
	if room := e.roomOf(player); room != nil && room.Status != StatusCanceled {
		if member := room.Members[player]; member != nil && member.Disconnected {
			member.Disconnected = false
			member.DisconnectedSinceUnixMS = 0
			member.ReserveUntilUnixMS = 0
		}
		resp.Type = "resumed"
		resp.RoomID = room.ID
		resp.Mode = room.Mode
		resp.RoomSize = room.Size
		resp.Status = room.Status
		resp.Members = room.confirmedIDs()
		resp.Seats = room.Seats()
		if member := room.Members[player]; member != nil && member.Status == StatusInvited {
			resp.DeadlineMS = room.DeadlineUnixMS
		}
	}
	_ = e.save()
	return sessionID, resp
}

func (e *Engine) CurrentSession(player string) string { return e.players[player] }
func (e *Engine) Result(player string) string         { return e.results[player] }

func (e *Engine) SetTimeouts(proposal, empty, reserve time.Duration) {
	e.proposalTTL = proposal
	e.emptyTTL = empty
	e.reserveTTL = reserve
}

func (e *Engine) PublicRoomStates() map[string]Response {
	states := map[string]Response{}
	for _, room := range e.rooms {
		if room.Status == StatusCanceled {
			continue
		}
		for playerID, member := range room.Members {
			resp := e.roomView(room, member)
			if member.Status == StatusInvited && !member.Disconnected {
				resp.Type = "invited"
				resp.Reason = StatusInvited
				resp.DeadlineMS = room.DeadlineUnixMS
			}
			states[playerID] = resp
		}
	}
	for _, item := range e.queue {
		states[item.PlayerID] = Response{Type: "queued", OK: true, Waiting: e.waitingIDs()}
	}
	return states
}

func (e *Engine) Disconnect(sessionID string) {
	player := e.sessions[sessionID]
	if player == "" {
		return
	}
	delete(e.sessions, sessionID)
	if e.players[player] != sessionID {
		return
	}
	delete(e.players, player)
	e.removeQueue(player)
	if room := e.roomOf(player); room != nil && room.Status != StatusCanceled {
		member := room.Members[player]
		if member != nil && member.Status == StatusConfirmed {
			member.Disconnected = true
			member.DisconnectedSinceUnixMS = e.now().UnixMilli()
			member.ReserveUntilUnixMS = member.DisconnectedSinceUnixMS + e.reserveTTL.Milliseconds()
		} else {
			e.setResult(player, StatusCanceled)
			delete(room.Members, player)
			if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
				room.EmptySinceUnixMS = e.now().UnixMilli()
			}
			e.fillVacancies()
		}
	}
	e.deleteEmptyRooms()
	_ = e.save()
}

func (e *Engine) Handle(req Request, sessionID string) Response {
	switch req.Type {
	case "queue":
		return e.enqueue(req, sessionID)
	case "cancel":
		return e.cancel(req, sessionID)
	case "confirm":
		return e.confirm(req, sessionID)
	case "reject":
		return e.reject(req, sessionID)
	case "snapshot":
		return e.snapshot()
	case "result":
		if !e.validSession(sessionID, req.PlayerID) {
			return Response{Type: "error", Reason: "stale_session"}
		}
		return Response{Type: "result", OK: true, Status: e.results[req.PlayerID]}
	default:
		return Response{Type: "error", Reason: "unknown_command"}
	}
}

func (e *Engine) validSession(sessionID, player string) bool {
	return e.sessions[sessionID] == player && e.players[player] == sessionID
}

func (e *Engine) enqueue(req Request, sessionID string) Response {
	if !e.validSession(sessionID, req.PlayerID) {
		return Response{Type: "error", Reason: "stale_session"}
	}
	if req.RoomSize < 2 || req.RoomSize > 4 {
		return Response{Type: "error", Reason: "invalid_room_size"}
	}
	if req.Mode == "" {
		return Response{Type: "error", Reason: "mode_required"}
	}
	if e.roomOf(req.PlayerID) != nil {
		return Response{Type: "error", Reason: "already_in_room"}
	}
	if e.queued(req.PlayerID) {
		return Response{Type: "queued", OK: true, Waiting: e.waitingIDs()}
	}
	e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: req.PlayerID, Mode: req.Mode, Level: req.Level, RoomSize: req.RoomSize})
	e.nextSeq++
	e.match()
	if err := e.save(); err != nil {
		return Response{Type: "error", Reason: "persist_failed"}
	}
	if room := e.roomOf(req.PlayerID); room != nil {
		return e.inviteResponse(room)
	}
	return Response{Type: "queued", OK: true, Waiting: e.waitingIDs()}
}

func (e *Engine) cancel(req Request, sessionID string) Response {
	if !e.validSession(sessionID, req.PlayerID) {
		return Response{Type: "error", Reason: "stale_session"}
	}
	if room := e.roomOf(req.PlayerID); room != nil && room.Status != StatusCanceled {
		if member := room.Members[req.PlayerID]; member == nil || member.Status != StatusConfirmed {
			e.setResult(req.PlayerID, StatusCanceled)
		}
		delete(room.Members, req.PlayerID)
		if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
			room.EmptySinceUnixMS = e.now().UnixMilli()
		}
		e.fillVacancies()
		e.deleteEmptyRooms()
		_ = e.save()
		roomID := room.ID
		return Response{Type: "canceled", OK: true, RoomID: roomID}
	}
	if e.removeQueue(req.PlayerID) {
		_ = e.save()
		return Response{Type: "canceled", OK: true}
	}
	return Response{Type: "canceled", OK: true}
}

func (e *Engine) confirm(req Request, sessionID string) Response {
	if !e.validSession(sessionID, req.PlayerID) {
		return Response{Type: "error", Reason: "stale_session"}
	}
	room := e.roomOf(req.PlayerID)
	if room == nil || room.ID != req.RoomID || room.Status == StatusCanceled {
		return Response{Type: "error", Reason: "not_invited"}
	}
	member := room.Members[req.PlayerID]
	if member.Status != StatusConfirmed {
		member.Status = StatusConfirmed
	}
	member.Disconnected = false
	member.DisconnectedSinceUnixMS = 0
	member.ReserveUntilUnixMS = 0
	if room.Status == StatusProposed && room.allConfirmed() {
		room.Status = StatusActive
		room.DeadlineUnixMS = 0
		room.EmptySinceUnixMS = 0
		for playerID := range room.Members {
			e.setResult(playerID, StatusActive)
		}
	}
	// A refill round completes exactly like the initial proposal: once every
	// seat is confirmed again the proposal deadline is obsolete and must not
	// survive in broadcasts or the persisted state file.
	if room.Status == StatusActive && len(room.Members) == room.Size && !room.hasInvited() {
		room.DeadlineUnixMS = 0
		room.EmptySinceUnixMS = 0
		for playerID := range room.Members {
			e.setResult(playerID, StatusActive)
		}
	}
	_ = e.save()
	return confirmResponse(room, member)
}

func (e *Engine) reject(req Request, sessionID string) Response {
	if !e.validSession(sessionID, req.PlayerID) {
		return Response{Type: "error", Reason: "stale_session"}
	}
	room := e.roomOf(req.PlayerID)
	if room == nil || room.ID != req.RoomID || room.Status == StatusCanceled {
		return Response{Type: "error", Reason: "not_invited"}
	}
	if member := room.Members[req.PlayerID]; member != nil && member.Status == StatusConfirmed {
		return Response{Type: "error", Reason: "already_confirmed"}
	}
	e.setResult(req.PlayerID, "rejected")
	delete(room.Members, req.PlayerID)
	if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
		room.EmptySinceUnixMS = e.now().UnixMilli()
	}
	e.fillVacancies()
	e.deleteEmptyRooms()
	_ = e.save()
	return Response{Type: "rejected", OK: true, RoomID: room.ID}
}

func (e *Engine) removeQueue(player string) bool {
	out := e.queue[:0]
	removed := false
	for _, item := range e.queue {
		if item.PlayerID == player {
			removed = true
			continue
		}
		out = append(out, item)
	}
	e.queue = out
	return removed
}

func (e *Engine) match() {
	for {
		if len(e.queue) < 2 {
			return
		}
		sort.SliceStable(e.queue, func(i, j int) bool { return e.queue[i].Seq < e.queue[j].Seq })
		start := -1
		var selected []QueueRecord
		for anchorIndex, possibleAnchor := range e.queue {
			candidates := []QueueRecord{possibleAnchor}
			for _, candidate := range e.queue[anchorIndex+1:] {
				if len(candidates) == possibleAnchor.RoomSize {
					break
				}
				if candidate.Mode == possibleAnchor.Mode && candidate.RoomSize == possibleAnchor.RoomSize && abs(candidate.Level-possibleAnchor.Level) <= 2 {
					candidates = append(candidates, candidate)
				}
			}
			if len(candidates) == possibleAnchor.RoomSize {
				start = anchorIndex
				selected = candidates
				break
			}
		}
		if start < 0 {
			return
		}
		anchor := e.queue[start]
		id := fmt.Sprintf("room-%d", e.nextRoom)
		e.nextRoom++
		room := &Room{ID: id, Mode: anchor.Mode, Level: anchor.Level, Size: anchor.RoomSize,
			Status: StatusProposed, Members: map[string]*Member{}, CreatedSeq: anchor.Seq,
			DeadlineUnixMS: e.now().Add(e.proposalTTL).UnixMilli()}
		chosen := map[string]bool{}
		for _, item := range selected {
			chosen[item.PlayerID] = true
			room.Members[item.PlayerID] = &Member{PlayerID: item.PlayerID, Status: StatusInvited, JoinSeq: item.Seq}
		}
		e.rooms[id] = room
		remaining := e.queue[:0]
		for _, item := range e.queue {
			if !chosen[item.PlayerID] {
				remaining = append(remaining, item)
			}
		}
		e.queue = remaining
	}
}

func (e *Engine) fillVacancies() {
	sort.SliceStable(e.queue, func(i, j int) bool { return e.queue[i].Seq < e.queue[j].Seq })
	rooms := make([]*Room, 0, len(e.rooms))
	for _, room := range e.rooms {
		rooms = append(rooms, room)
	}
	sort.Slice(rooms, func(i, j int) bool {
		if rooms[i].CreatedSeq != rooms[j].CreatedSeq {
			return rooms[i].CreatedSeq < rooms[j].CreatedSeq
		}
		return rooms[i].ID < rooms[j].ID
	})
	for _, room := range rooms {
		if room.Status != StatusProposed && room.Status != StatusActive {
			continue
		}
		if room.Status == StatusActive && !room.hasConfirmedMember() {
			continue
		}
		for len(room.Members) < room.Size {
			hadInvited := room.hasInvited()
			index := -1
			for i, item := range e.queue {
				if item.Mode == room.Mode && item.RoomSize == room.Size && abs(item.Level-room.Level) <= 2 {
					index = i
					break
				}
			}
			if index < 0 {
				break
			}
			item := e.queue[index]
			e.queue = append(e.queue[:index], e.queue[index+1:]...)
			room.Members[item.PlayerID] = &Member{PlayerID: item.PlayerID, Status: StatusInvited, JoinSeq: item.Seq}
			if !hadInvited {
				room.DeadlineUnixMS = e.now().Add(e.proposalTTL).UnixMilli()
			}
			if room.Status == StatusActive && room.EmptySinceUnixMS == 0 {
				room.EmptySinceUnixMS = e.now().UnixMilli()
			}
		}
		if len(room.Members) == room.Size {
			room.EmptySinceUnixMS = 0
		}
		if len(room.Members) == room.Size && !room.hasInvited() {
			room.DeadlineUnixMS = 0
		}
	}
}
func (e *Engine) deleteEmptyRooms() {
	for id, room := range e.rooms {
		if len(room.Members) == 0 {
			delete(e.rooms, id)
		}
	}
}

func (e *Engine) Tick() []Response {
	nowMS := e.now().UnixMilli()
	changed := false
	for id, room := range e.rooms {
		if room.Status == StatusProposed || room.Status == StatusActive {
			expired := false
			for playerID, member := range room.Members {
				if member.Disconnected && member.ReserveUntilUnixMS > 0 && nowMS >= member.ReserveUntilUnixMS {
					delete(room.Members, playerID)
					e.setResult(playerID, StatusCanceled)
					expired = true
				}
			}
			if expired {
				changed = true
				if len(room.Members) == 0 {
					delete(e.rooms, id)
					continue
				}
				if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
					room.EmptySinceUnixMS = nowMS
				}
				e.fillVacancies()
				if len(room.Members) == room.Size && !room.hasInvited() {
					room.DeadlineUnixMS = 0
					room.EmptySinceUnixMS = 0
				}
			}
		}
		switch room.Status {
		case StatusProposed:
			if room.DeadlineUnixMS > 0 && nowMS >= room.DeadlineUnixMS {
				if room.allConfirmed() {
					room.Status = StatusActive
					room.DeadlineUnixMS = 0
					changed = true
					continue
				}
				for playerID, member := range room.Members {
					if member.Status == StatusConfirmed || member.Status == StatusInvited {
						e.setResult(playerID, StatusCanceled)
						if !member.Disconnected {
							e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
							e.nextSeq++
						}
					}
				}
				delete(e.rooms, id)
				changed = true
			}
		case StatusActive:
			if room.DeadlineUnixMS > 0 && nowMS >= room.DeadlineUnixMS {
				for playerID, member := range room.Members {
					if member.Status == StatusInvited {
						e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
						e.nextSeq++
						delete(room.Members, playerID)
					}
				}
				room.DeadlineUnixMS = 0
				changed = true
			}
			e.fillVacancies()
			if len(room.Members) == room.Size && !room.hasInvited() {
				room.DeadlineUnixMS = 0
				room.EmptySinceUnixMS = 0
			} else if room.hasInvited() && room.DeadlineUnixMS == 0 {
				room.DeadlineUnixMS = e.now().Add(e.proposalTTL).UnixMilli()
			}
			if len(room.Members) < room.Size {
				if room.EmptySinceUnixMS == 0 {
					room.EmptySinceUnixMS = nowMS
				}
				if nowMS-room.EmptySinceUnixMS >= e.emptyTTL.Milliseconds() {
					for playerID, member := range room.Members {
						if member.Disconnected {
							continue
						}
						if member.Status == StatusConfirmed || member.Status == StatusInvited {
							e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
							e.nextSeq++
						}
					}
					delete(e.rooms, id)
					changed = true
				}
			} else {
				room.EmptySinceUnixMS = 0
			}
		}
	}
	if changed {
		e.match()
		_ = e.save()
	}
	return nil
}

func confirmResponse(room *Room, member *Member) Response {
	resp := Response{OK: true, RoomID: room.ID, Status: room.Status, Members: room.confirmedIDs(), Seats: room.Seats()}
	resolving := room.Status == StatusActive && len(room.Members) == room.Size && !room.hasInvited() && !room.hasDisconnected()
	if resolving {
		resp.Type = "confirmed"
	} else {
		resp.Type = "room_state"
		resp.Reason = StatusConfirmed
		if member != nil && member.Disconnected {
			resp.Reason = SeatReserved
			resp.DeadlineMS = member.ReserveUntilUnixMS
		} else if room.hasInvited() {
			resp.DeadlineMS = room.DeadlineUnixMS
		}
	}
	return resp
}

func (e *Engine) inviteResponse(room *Room) Response {
	return Response{Type: "invited", OK: true, RoomID: room.ID, Mode: room.Mode, RoomSize: room.Size,
		DeadlineMS: room.DeadlineUnixMS, Members: room.confirmedIDs(), Seats: room.Seats()}
}

func (e *Engine) waitingIDs() []string {
	ids := make([]string, 0, len(e.queue))
	for _, item := range e.queue {
		ids = append(ids, item.PlayerID)
	}
	return ids
}

func (e *Engine) snapshot() Response {
	rooms := []string{}
	for id := range e.rooms {
		rooms = append(rooms, id)
	}
	sort.Strings(rooms)
	return Response{Type: "snapshot", OK: true, Waiting: e.waitingIDs(), RoomID: "", Members: rooms}
}

func abs(v int) int {
	if v < 0 {
		return -v
	}
	return v
}
