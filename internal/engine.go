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
)

type Member struct {
	PlayerID string
	Status   string
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
		if member.Status == StatusConfirmed {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	return ids
}

func (r *Room) allConfirmed() bool {
	return len(r.Members) == r.Size && len(r.confirmedIDs()) == r.Size
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
	queue        []QueueRecord
	rooms        map[string]*Room
	sessions     map[string]string
	players      map[string]string
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
	nextSeq, queue, rooms, err := store.Load(nowMS, int64(10*time.Second/time.Millisecond))
	if err != nil {
		return nil, err
	}
	e := &Engine{store: store, proposalTTL: 10 * time.Second, emptyTTL: 15 * time.Second,
		queue: queue, rooms: rooms, sessions: map[string]string{}, players: map[string]string{},
		nextSeq: nextSeq, nextRoom: 1, now: now}
	for id := range rooms {
		var n int64
		fmt.Sscanf(id, "room-%d", &n)
		if n >= e.nextRoom {
			e.nextRoom = n + 1
		}
	}
	e.fillVacancies()
	return e, nil
}

func (e *Engine) save() error { return e.store.Save(e.nextSeq, e.queue, e.rooms) }

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
	if room := e.roomOf(player); room != nil {
		resp.Type = "resumed"
		resp.RoomID = room.ID
		resp.Mode = room.Mode
		resp.RoomSize = room.Size
		resp.Members = room.confirmedIDs()
	}
	return sessionID, resp
}

func (e *Engine) CurrentSession(player string) string { return e.players[player] }

func (e *Engine) PublicRoomStates() map[string]Response {
	states := map[string]Response{}
	for _, room := range e.rooms {
		for playerID, member := range room.Members {
			states[playerID] = Response{Type: "room_state", OK: true, RoomID: room.ID, Mode: room.Mode,
				RoomSize: room.Size, Members: room.confirmedIDs(), Reason: member.Status, DeadlineMS: room.DeadlineUnixMS}
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
	if e.players[player] == sessionID {
		delete(e.players, player)
	}
	e.removeQueue(player)
	if room := e.roomOf(player); room != nil {
		delete(room.Members, player)
		if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
			room.EmptySinceUnixMS = e.now().UnixMilli()
		}
		e.fillVacancies()
	}
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
	if room := e.roomOf(req.PlayerID); room != nil {
		delete(room.Members, req.PlayerID)
		if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
			room.EmptySinceUnixMS = e.now().UnixMilli()
		}
		if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
			room.EmptySinceUnixMS = e.now().UnixMilli()
		}
		e.fillVacancies()
		_ = e.save()
		return Response{Type: "canceled", OK: true, RoomID: room.ID}
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
	if room == nil || room.ID != req.RoomID {
		return Response{Type: "error", Reason: "not_invited"}
	}
	member := room.Members[req.PlayerID]
	member.Status = StatusConfirmed
	if room.allConfirmed() {
		room.Status = StatusActive
		room.DeadlineUnixMS = 0
		room.EmptySinceUnixMS = 0
	}
	_ = e.save()
	return Response{Type: "confirmed", OK: true, RoomID: room.ID, Status: room.Status, Members: room.confirmedIDs()}
}

func (e *Engine) reject(req Request, sessionID string) Response {
	if !e.validSession(sessionID, req.PlayerID) {
		return Response{Type: "error", Reason: "stale_session"}
	}
	room := e.roomOf(req.PlayerID)
	if room == nil || room.ID != req.RoomID {
		return Response{Type: "error", Reason: "not_invited"}
	}
	delete(room.Members, req.PlayerID)
	if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
		room.EmptySinceUnixMS = e.now().UnixMilli()
	}
	e.fillVacancies()
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
			room.Members[item.PlayerID] = &Member{PlayerID: item.PlayerID, Status: StatusInvited}
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
	for _, room := range e.rooms {
		for len(room.Members) < room.Size {
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
			room.Members[item.PlayerID] = &Member{PlayerID: item.PlayerID, Status: StatusInvited}
			room.DeadlineUnixMS = e.now().Add(e.proposalTTL).UnixMilli()
			if room.Status == StatusActive && room.EmptySinceUnixMS == 0 {
				room.EmptySinceUnixMS = e.now().UnixMilli()
			}
		}
		if len(room.Members) == room.Size {
			room.EmptySinceUnixMS = 0
		}
	}
}

func (e *Engine) Tick() []Response {
	nowMS := e.now().UnixMilli()
	changed := false
	for id, room := range e.rooms {
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
						e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
						e.nextSeq++
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
				if room.allConfirmed() {
					room.DeadlineUnixMS = 0
					room.EmptySinceUnixMS = 0
				}
				changed = true
			}
			e.fillVacancies()
			if len(room.Members) < room.Size {
				if room.EmptySinceUnixMS == 0 {
					room.EmptySinceUnixMS = nowMS
				}
				if nowMS-room.EmptySinceUnixMS >= e.emptyTTL.Milliseconds() {
					for playerID, member := range room.Members {
						if member.Status == StatusConfirmed {
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

func (e *Engine) inviteResponse(room *Room) Response {
	return Response{Type: "invited", OK: true, RoomID: room.ID, Mode: room.Mode, RoomSize: room.Size,
		DeadlineMS: room.DeadlineUnixMS, Members: room.confirmedIDs()}
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
