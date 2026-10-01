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
	Conn      any
	Resp      chan Response
}

type SessionRecord struct {
	SessionID string `json:"session_id"`
	PlayerID  string `json:"player_id"`
	RoomID    string `json:"room_id,omitempty"`
	Result    string `json:"result"`
}

type Engine struct {
	store          *Store
	proposalTTL    time.Duration
	emptyTTL       time.Duration
	queue          []QueueRecord
	rooms          map[string]*Room
	sessions       map[string]string
	sessionRecords map[string]*SessionRecord
	players        map[string]string
	notifications  map[string]Response
	nextSeq        int64
	nextRoom       int64
	now            func() time.Time
	sessionNonce   atomic.Int64
}

func NewEngine(store *Store, now func() time.Time) (*Engine, error) {
	if now == nil {
		now = time.Now
	}
	nowMS := now().UnixMilli()
	nextSeq, queue, rooms, records, err := store.Load(nowMS, int64(10*time.Second/time.Millisecond))
	if err != nil {
		return nil, err
	}
	e := &Engine{store: store, proposalTTL: 10 * time.Second, emptyTTL: 15 * time.Second,
		queue: queue, rooms: rooms, sessions: map[string]string{}, sessionRecords: records, players: map[string]string{},
		notifications: map[string]Response{}, nextSeq: nextSeq, nextRoom: 1, now: now}
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

func (e *Engine) save() error {
	return e.store.Save(e.nextSeq, e.queue, e.rooms, e.sessionRecords)
}

func (e *Engine) SetTimeouts(proposalTTL, emptyTTL time.Duration) {
	if proposalTTL > 0 {
		e.proposalTTL = proposalTTL
	}
	if emptyTTL > 0 {
		e.emptyTTL = emptyTTL
	}
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
	for _, room := range e.roomsSorted() {
		if _, ok := room.Members[player]; ok {
			return room
		}
	}
	return nil
}

func (e *Engine) roomsSorted() []*Room {
	rooms := make([]*Room, 0, len(e.rooms))
	for _, room := range e.rooms {
		rooms = append(rooms, room)
	}
	sort.Slice(rooms, func(i, j int) bool { return rooms[i].CreatedSeq < rooms[j].CreatedSeq })
	return rooms
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
	if e.sessionRecords == nil {
		e.sessionRecords = map[string]*SessionRecord{}
	}
	if e.sessionRecords[sessionID] == nil {
		e.sessionRecords[sessionID] = &SessionRecord{SessionID: sessionID, PlayerID: player}
	}
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

func (e *Engine) SessionRecord(sessionID string) SessionRecord {
	if record := e.sessionRecords[sessionID]; record != nil {
		return *record
	}
	return SessionRecord{}
}

func (e *Engine) markResult(room *Room, result string, players ...string) {
	for _, player := range players {
		if sessionID := e.players[player]; sessionID != "" {
			record := e.sessionRecords[sessionID]
			if record != nil {
				record.Result = result
				if room != nil {
					record.RoomID = room.ID
				}
			}
		}
	}
}

func (e *Engine) markResultIfEmpty(room *Room, result string, players ...string) {
	for _, player := range players {
		if sessionID := e.players[player]; sessionID != "" {
			record := e.sessionRecords[sessionID]
			if record != nil && record.Result == "" {
				record.Result = result
				record.RoomID = room.ID
			}
		}
	}
}

func (e *Engine) dissolveProposal(room *Room, leavingPlayer string, leavingResult string) Response {
	remaining := make([]string, 0, len(room.Members)-1)
	for playerID := range room.Members {
		if playerID != leavingPlayer {
			remaining = append(remaining, playerID)
		}
	}
	e.markResult(room, leavingResult, leavingPlayer)
	delete(e.rooms, room.ID)
	e.markResultIfEmpty(room, StatusCanceled, remaining...)
	for _, playerID := range remaining {
		e.notify(playerID, Response{Type: StatusCanceled, OK: true, RoomID: room.ID, Mode: room.Mode, RoomSize: room.Size})
		e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
		e.nextSeq++
	}
	e.match()
	_ = e.save()
	return Response{Type: leavingResult, OK: true, RoomID: room.ID}
}

func (e *Engine) notify(player string, resp Response) {
	if e.notifications == nil {
		e.notifications = map[string]Response{}
	}
	e.notifications[player] = resp
}

func (e *Engine) notifyRoom(room *Room) {
	if room == nil {
		return
	}
	for playerID, member := range room.Members {
		respType := "room_state"
		if member.Status == StatusInvited {
			respType = "invited"
		}
		e.notify(playerID, Response{Type: respType, OK: true, RoomID: room.ID, Mode: room.Mode,
			RoomSize: room.Size, Members: room.confirmedIDs(), Reason: member.Status, DeadlineMS: room.DeadlineUnixMS})
	}
}

func (e *Engine) Notifications() map[string]Response {
	notifications := e.notifications
	if notifications == nil {
		notifications = map[string]Response{}
	}
	e.notifications = map[string]Response{}
	return notifications
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
		e.markResultIfEmpty(room, StatusCanceled, player)
		delete(room.Members, player)
		if len(room.Members) == 0 {
			delete(e.rooms, room.ID)
		}
		if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
			room.EmptySinceUnixMS = e.now().UnixMilli()
		}
		e.fillVacancies()
		e.notifyRoom(room)
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
		if room.Status == StatusProposed {
			return e.dissolveProposal(room, req.PlayerID, StatusCanceled)
		}
		delete(room.Members, req.PlayerID)
		if len(room.Members) == 0 {
			delete(e.rooms, room.ID)
		}
		if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
			room.EmptySinceUnixMS = e.now().UnixMilli()
		}
		e.fillVacancies()
		_ = e.save()
		e.markResult(room, StatusCanceled, req.PlayerID)
		e.notifyRoom(room)
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
		ids := make([]string, 0, len(room.Members))
		for id := range room.Members {
			ids = append(ids, id)
		}
		e.markResult(room, StatusActive, ids...)
	}
	_ = e.save()
	e.notifyRoom(room)
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
	if room.Status == StatusProposed {
		return e.dissolveProposal(room, req.PlayerID, "rejected")
	}
	delete(room.Members, req.PlayerID)
	if room.Status == StatusActive && len(room.Members) < room.Size && room.EmptySinceUnixMS == 0 {
		room.EmptySinceUnixMS = e.now().UnixMilli()
	}
	e.fillVacancies()
	_ = e.save()
	e.markResult(room, "rejected", req.PlayerID)
	e.notifyRoom(room)
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
		e.notifyRoom(room)
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
	for _, room := range e.roomsSorted() {
		added := false
		deadline := room.DeadlineUnixMS
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
			added = true
			deadline = e.now().Add(e.proposalTTL).UnixMilli()
			if room.Status == StatusActive && room.EmptySinceUnixMS == 0 {
				room.EmptySinceUnixMS = e.now().UnixMilli()
			}
		}
		room.DeadlineUnixMS = deadline
		if len(room.Members) == room.Size {
			room.EmptySinceUnixMS = 0
		}
		if added {
			e.notifyRoom(room)
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
					if len(room.Members) == room.Size {
						room.Status = StatusActive
						room.DeadlineUnixMS = 0
					}
					changed = true
					continue
				}
				expired := make([]string, 0, len(room.Members))
				for playerID, member := range room.Members {
					if member.Status == StatusConfirmed || member.Status == StatusInvited {
						expired = append(expired, playerID)
					}
				}
				delete(e.rooms, id)
				for _, playerID := range expired {
					e.markResultIfEmpty(room, StatusCanceled, playerID)
					e.notify(playerID, Response{Type: StatusCanceled, OK: true, RoomID: room.ID, Mode: room.Mode, RoomSize: room.Size})
					e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
					e.nextSeq++
				}
				changed = true
			}
		case StatusActive:
			if room.DeadlineUnixMS > 0 && nowMS >= room.DeadlineUnixMS {
				expired := []string{}
				for playerID, member := range room.Members {
					if member.Status == StatusInvited {
						expired = append(expired, playerID)
						delete(room.Members, playerID)
					}
				}
				room.DeadlineUnixMS = 0
				if room.allConfirmed() {
					room.EmptySinceUnixMS = 0
				} else {
					room.EmptySinceUnixMS = nowMS
				}
				changed = true
				for _, playerID := range expired {
					e.markResultIfEmpty(room, StatusCanceled, playerID)
					e.notify(playerID, Response{Type: StatusCanceled, OK: true, RoomID: room.ID, Mode: room.Mode, RoomSize: room.Size})
				}
				for _, playerID := range expired {
					e.queue = append(e.queue, QueueRecord{Seq: e.nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
					e.nextSeq++
				}
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

func itemLevel(room *Room) int {
	for _, member := range room.Members {
		if member.Status == StatusConfirmed {
			return room.Level
		}
	}
	return room.Level
}
