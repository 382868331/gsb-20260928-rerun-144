package main

import (
	"fmt"
	"sort"
	"sync"
	"time"
)

const (
	StatusIdle    = "idle"
	StatusQueued  = "queued"
	StatusMatched = "matched" // in a pending room, confirmation outstanding
	StatusInRoom  = "in_room" // confirmed room member

	RoomPending   = "pending"
	RoomConfirmed = "confirmed"
	RoomClosed    = "closed"
)

type Player struct {
	ID      string
	Session string // session of the connection that owns current state
	Status  string
	Mode    string
	RoomID  string
	conn    *clientConn // nil while offline
}

type Slot struct {
	PlayerID  string
	Session   string
	Token     string
	Confirmed bool
}

type Room struct {
	ID       string
	Mode     string
	State    string
	Slots    []*Slot
	Deadline time.Time
}

type event struct {
	kind string // hello|enqueue|cancel|confirm|leave|status|disconnect|tick
	conn *clientConn
	msg  InMessage
}

type Server struct {
	events         chan event
	players        map[string]*Player
	queue          []string // player IDs, FIFO
	rooms          map[string]*Room
	store          *Store
	roomSize       int
	confirmTimeout time.Duration
	seq            int
	done           chan struct{}
	stopped        chan struct{}
	stopOnce       sync.Once
}

func NewServer(store *Store, roomSize int, confirmTimeout time.Duration) *Server {
	s := &Server{
		events:         make(chan event, 256),
		players:        map[string]*Player{},
		rooms:          map[string]*Room{},
		store:          store,
		roomSize:       roomSize,
		confirmTimeout: confirmTimeout,
		done:           make(chan struct{}),
		stopped:        make(chan struct{}),
	}
	s.restore()
	return s
}

func (s *Server) nextID(prefix string) string {
	s.seq++
	return fmt.Sprintf("%s-%d", prefix, s.seq)
}

// restore loads persisted state. Pending rooms cannot outlive their
// connections, so they are closed and their members become idle: nobody
// stays stuck and nobody holds a duplicate seat after a restart.
func (s *Server) restore() {
	snap, err := s.store.Load()
	if err != nil || snap == nil {
		return
	}
	s.seq = snap.Seq
	for _, rp := range snap.Players {
		s.players[rp.ID] = &Player{ID: rp.ID, Status: StatusIdle}
	}
	for _, rr := range snap.Rooms {
		room := &Room{ID: rr.ID, Mode: rr.Mode, State: rr.State}
		for _, rs := range rr.Slots {
			room.Slots = append(room.Slots, &Slot{PlayerID: rs.PlayerID, Session: rs.Session, Token: rs.Token, Confirmed: rs.Confirmed})
		}
		if room.State == RoomConfirmed {
			s.rooms[room.ID] = room
			for _, sl := range room.Slots {
				if p := s.players[sl.PlayerID]; p != nil {
					p.Status = StatusInRoom
					p.RoomID = room.ID
				}
			}
		}
		// pending rooms are dropped: members stay idle, no seat is held
	}
}

func (s *Server) snapshot() {
	snap := Snapshot{Seq: s.seq}
	ids := make([]string, 0, len(s.players))
	for id := range s.players {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		p := s.players[id]
		snap.Players = append(snap.Players, PlayerSnap{ID: p.ID, Status: p.Status, RoomID: p.RoomID})
	}
	for _, r := range s.rooms {
		if r.State == RoomClosed {
			continue
		}
		rr := RoomSnap{ID: r.ID, Mode: r.Mode, State: r.State}
		for _, sl := range r.Slots {
			rr.Slots = append(rr.Slots, SlotSnap{PlayerID: sl.PlayerID, Session: sl.Session, Token: sl.Token, Confirmed: sl.Confirmed})
		}
		snap.Rooms = append(snap.Rooms, rr)
	}
	s.store.Save(snap)
}

// checkInvariants verifies no duplicate seats and no stuck players.
func (s *Server) checkInvariants() error {
	seat := map[string]string{}
	for _, r := range s.rooms {
		if r.State == RoomClosed {
			continue
		}
		for _, sl := range r.Slots {
			if prev, dup := seat[sl.PlayerID]; dup {
				return fmt.Errorf("player %s occupies two rooms %s and %s", sl.PlayerID, prev, r.ID)
			}
			seat[sl.PlayerID] = r.ID
			p := s.players[sl.PlayerID]
			if p == nil {
				return fmt.Errorf("room %s references unknown player %s", r.ID, sl.PlayerID)
			}
			if r.State == RoomConfirmed && (p.Status != StatusInRoom || p.RoomID != r.ID) {
				return fmt.Errorf("player %s status %s disagrees with confirmed room %s", p.ID, p.Status, r.ID)
			}
		}
	}
	for _, p := range s.players {
		if p.Status == StatusInRoom {
			r := s.rooms[p.RoomID]
			if r == nil || r.State != RoomConfirmed {
				return fmt.Errorf("player %s stuck in missing room %s", p.ID, p.RoomID)
			}
		}
		if p.Status == StatusMatched && p.conn == nil {
			return fmt.Errorf("player %s stuck matched without connection", p.ID)
		}
	}
	return nil
}

func (s *Server) Run() {
	defer close(s.stopped)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
	for {
		select {
		case <-s.done:
			return
		case ev := <-s.events:
			s.handle(ev)
		case <-tick.C:
			s.expireRooms()
		}
	}
}

func (s *Server) Stop() {
	s.stopOnce.Do(func() { close(s.done) })
	<-s.stopped
}

func (s *Server) handle(ev event) {
	switch ev.kind {
	case "disconnect":
		s.onDisconnect(ev.conn)
	case "tick":
		s.expireRooms()
	default:
		s.onMessage(ev.conn, ev.msg)
	}
	if err := s.checkInvariants(); err != nil {
		panic("invariant violated: " + err.Error())
	}
	s.snapshot()
}

func (s *Server) onMessage(c *clientConn, m InMessage) {
	switch m.Type {
	case "hello":
		if m.PlayerID == "" {
			c.send(OutMessage{Type: "error", Reason: "player_id required"})
			return
		}
		p := s.players[m.PlayerID]
		if p == nil {
			p = &Player{ID: m.PlayerID, Status: StatusIdle}
			s.players[m.PlayerID] = p
		}
		// A new connection supersedes any previous session. The old
		// connection can no longer act for this player.
		p.Session = c.session
		p.conn = c
		c.playerID = m.PlayerID
		c.send(OutMessage{Type: "welcome", PlayerID: p.ID, Session: c.session, RoomID: p.RoomID, Reason: p.Status})
	case "enqueue":
		p := s.owned(c)
		if p == nil {
			return
		}
		switch p.Status {
		case StatusInRoom:
			c.send(OutMessage{Type: "error", Reason: "already_in_room:" + p.RoomID})
			return
		case StatusQueued, StatusMatched:
			c.send(OutMessage{Type: "error", Reason: "already_" + p.Status})
			return
		}
		p.Status = StatusQueued
		p.Mode = m.Mode
		s.queue = append(s.queue, p.ID)
		c.send(OutMessage{Type: "queued", PlayerID: p.ID})
		s.matchmake(m.Mode)
	case "cancel":
		p := s.owned(c)
		if p == nil {
			return
		}
		// Deterministic: events are serialized, so cancel either finds the
		// player still queued (cancel wins) or already matched (match wins).
		if p.Status == StatusQueued {
			s.removeFromQueue(p.ID)
			p.Status = StatusIdle
			c.send(OutMessage{Type: "cancelled", PlayerID: p.ID, OK: true})
		} else {
			c.send(OutMessage{Type: "cancel_failed", Reason: "already_" + p.Status})
		}
	case "confirm":
		p := s.owned(c)
		if p == nil {
			return
		}
		r := s.rooms[m.RoomID]
		if r == nil || r.State != RoomPending {
			c.send(OutMessage{Type: "confirm_failed", Reason: "no_pending_room"})
			return
		}
		var slot *Slot
		for _, sl := range r.Slots {
			if sl.PlayerID == p.ID {
				slot = sl
			}
		}
		// The token binds the offer to the exact session that received it;
		// an old connection cannot confirm for a new session.
		if slot == nil || slot.Session != c.session || slot.Token != m.Token {
			c.send(OutMessage{Type: "confirm_failed", Reason: "stale_token"})
			return
		}
		slot.Confirmed = true
		c.send(OutMessage{Type: "confirm_ok", RoomID: r.ID})
		s.maybeConfirmRoom(r)
	case "leave_room":
		p := s.owned(c)
		if p == nil {
			return
		}
		if p.Status != StatusInRoom {
			c.send(OutMessage{Type: "error", Reason: "not_in_room"})
			return
		}
		r := s.rooms[p.RoomID]
		for i, sl := range r.Slots {
			if sl.PlayerID == p.ID {
				r.Slots = append(r.Slots[:i], r.Slots[i+1:]...)
			}
		}
		p.Status = StatusIdle
		p.RoomID = ""
		if len(r.Slots) == 0 {
			r.State = RoomClosed
			delete(s.rooms, r.ID)
		}
		c.send(OutMessage{Type: "left_room", PlayerID: p.ID, OK: true})
	case "status":
		p := s.owned(c)
		if p == nil {
			return
		}
		c.send(OutMessage{Type: "status", PlayerID: p.ID, Reason: p.Status, RoomID: p.RoomID})
	default:
		c.send(OutMessage{Type: "error", Reason: "unknown_type"})
	}
}

// owned returns the player bound to this connection's session, rejecting
// actions from superseded (stale) connections.
func (s *Server) owned(c *clientConn) *Player {
	if c.playerID == "" {
		c.send(OutMessage{Type: "error", Reason: "say_hello_first"})
		return nil
	}
	p := s.players[c.playerID]
	if p == nil || p.Session != c.session {
		c.send(OutMessage{Type: "error", Reason: "stale_session"})
		return nil
	}
	return p
}

func (s *Server) removeFromQueue(id string) {
	for i, qid := range s.queue {
		if qid == id {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

// matchmake forms a room from the oldest queued players of one mode.
func (s *Server) matchmake(mode string) {
	for {
		var group []*Player
		for _, id := range s.queue {
			p := s.players[id]
			if p != nil && p.Status == StatusQueued && p.Mode == mode && p.conn != nil {
				group = append(group, p)
				if len(group) == s.roomSize {
					break
				}
			}
		}
		if len(group) < s.roomSize {
			return
		}
		room := &Room{ID: s.nextID("room"), Mode: mode, State: RoomPending, Deadline: time.Now().Add(s.confirmTimeout)}
		for _, p := range group {
			s.removeFromQueue(p.ID)
			p.Status = StatusMatched
			p.RoomID = room.ID
			token := room.ID + ":" + p.Session
			room.Slots = append(room.Slots, &Slot{PlayerID: p.ID, Session: p.Session, Token: token})
		}
		s.rooms[room.ID] = room
		for _, sl := range room.Slots {
			if p := s.players[sl.PlayerID]; p != nil && p.conn != nil {
				p.conn.send(OutMessage{Type: "match_offer", RoomID: room.ID, Token: sl.Token, Players: roomMembers(room)})
			}
		}
	}
}

func roomMembers(r *Room) []string {
	out := []string{}
	for _, sl := range r.Slots {
		out = append(out, sl.PlayerID)
	}
	return out
}

func (s *Server) maybeConfirmRoom(r *Room) {
	if r.State != RoomPending {
		return
	}
	for _, sl := range r.Slots {
		if !sl.Confirmed {
			return
		}
	}
	r.State = RoomConfirmed
	for _, sl := range r.Slots {
		p := s.players[sl.PlayerID]
		p.Status = StatusInRoom
		if p.conn != nil {
			p.conn.send(OutMessage{Type: "room_confirmed", RoomID: r.ID, Players: roomMembers(r)})
		}
	}
}

// abortRoom releases every seat of a pending room. Members who are still
// connected go back to the front of the queue and are notified.
func (s *Server) abortRoom(r *Room, reason string) {
	if r.State != RoomPending {
		return
	}
	r.State = RoomClosed
	delete(s.rooms, r.ID)
	requeued := false
	for _, sl := range r.Slots {
		p := s.players[sl.PlayerID]
		if p == nil || p.RoomID != r.ID {
			continue
		}
		p.RoomID = ""
		if p.conn != nil && p.Session == sl.Session {
			p.Status = StatusQueued
			s.queue = append([]string{p.ID}, s.queue...)
			p.conn.send(OutMessage{Type: "match_aborted", RoomID: r.ID, Reason: reason})
			p.conn.send(OutMessage{Type: "queued", PlayerID: p.ID})
			requeued = true
		} else {
			p.Status = StatusIdle
		}
	}
	if requeued {
		s.matchmake(r.Mode)
	}
}

func (s *Server) expireRooms() {
	now := time.Now()
	for _, r := range s.rooms {
		if r.State == RoomPending && now.After(r.Deadline) {
			s.abortRoom(r, "confirm_timeout")
		}
	}
}

func (s *Server) onDisconnect(c *clientConn) {
	p := s.players[c.playerID]
	if p == nil || p.Session != c.session {
		return // stale connection already superseded
	}
	p.conn = nil
	switch p.Status {
	case StatusQueued:
		s.removeFromQueue(p.ID)
		p.Status = StatusIdle
	case StatusMatched:
		if r := s.rooms[p.RoomID]; r != nil {
			s.abortRoom(r, "player_disconnected:"+p.ID)
		}
		p.Status = StatusIdle
		p.RoomID = ""
	case StatusInRoom:
		// confirmed seat is kept; the player may reconnect and resume
	}
}
