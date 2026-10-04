package internal

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
)

const StateVersion = 1

type QueueRecord struct {
	Seq      int64  `json:"seq"`
	PlayerID string `json:"player_id"`
	Mode     string `json:"mode"`
	Level    int    `json:"level"`
	RoomSize int    `json:"room_size"`
}

type MemberRecord struct {
	Status                  string `json:"status"`
	Disconnected            bool   `json:"disconnected,omitempty"`
	DisconnectedSinceUnixMS int64  `json:"disconnected_since_unix_ms,omitempty"`
	ReserveUntilUnixMS      int64  `json:"reserve_until_unix_ms,omitempty"`
	JoinSeq                 int64  `json:"join_seq,omitempty"`
}

type RoomRecord struct {
	ID               string                  `json:"id"`
	Mode             string                  `json:"mode"`
	Level            int                     `json:"level"`
	Size             int                     `json:"size"`
	Status           string                  `json:"status"`
	Members          map[string]MemberRecord `json:"members"`
	CreatedSeq       int64                   `json:"created_seq"`
	DeadlineUnixMS   int64                   `json:"deadline_unix_ms"`
	EmptySinceUnixMS int64                   `json:"empty_since_unix_ms,omitempty"`
}

type persistedState struct {
	Version  int               `json:"version"`
	NextSeq  int64             `json:"next_seq"`
	NextRoom int64             `json:"next_room,omitempty"`
	Queue    []QueueRecord     `json:"queue"`
	Rooms    []RoomRecord      `json:"rooms"`
	Results  map[string]string `json:"results,omitempty"`
}

type Store struct {
	path string
}

func NewStore(path string) *Store { return &Store{path: path} }

func (s *Store) Save(nextSeq, nextRoom int64, queue []QueueRecord, rooms map[string]*Room, results map[string]string) error {
	records := make([]RoomRecord, 0, len(rooms))
	for _, room := range rooms {
		members := make(map[string]MemberRecord, len(room.Members))
		for id, member := range room.Members {
			members[id] = MemberRecord{Status: member.Status, Disconnected: member.Disconnected,
				DisconnectedSinceUnixMS: member.DisconnectedSinceUnixMS, ReserveUntilUnixMS: member.ReserveUntilUnixMS,
				JoinSeq: member.JoinSeq}
		}
		records = append(records, RoomRecord{
			ID: room.ID, Mode: room.Mode, Level: room.Level, Size: room.Size,
			Status: room.Status, Members: members, CreatedSeq: room.CreatedSeq,
			DeadlineUnixMS: room.DeadlineUnixMS, EmptySinceUnixMS: room.EmptySinceUnixMS,
		})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	data, err := json.MarshalIndent(persistedState{Version: StateVersion, NextSeq: nextSeq, NextRoom: nextRoom, Queue: append([]QueueRecord(nil), queue...), Rooms: records, Results: results}, "", "  ")
	if err != nil {
		return err
	}
	dir := filepath.Dir(s.path)
	tmp, err := os.CreateTemp(dir, ".state-*.tmp")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer os.Remove(tmpName)
	if _, err := tmp.Write(data); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	return os.Rename(tmpName, s.path)
}

func (s *Store) Load(nowMS int64, proposalTTLMS int64) (int64, int64, []QueueRecord, map[string]*Room, map[string]string, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return 1, 1, nil, map[string]*Room{}, map[string]string{}, nil
	}
	if err != nil {
		return 0, 0, nil, nil, nil, err
	}
	var saved persistedState
	if err := json.Unmarshal(data, &saved); err != nil {
		return 0, 0, nil, nil, nil, err
	}
	if saved.Version != StateVersion {
		return 0, 0, nil, nil, nil, fmt.Errorf("unsupported state version %d", saved.Version)
	}
	rooms := make(map[string]*Room, len(saved.Rooms))
	queued := map[string]bool{}
	for _, q := range saved.Queue {
		queued[q.PlayerID] = true
	}
	occupied := map[string]string{}
	nextSeq := saved.NextSeq
	for _, rec := range saved.Rooms {
		room := &Room{ID: rec.ID, Mode: rec.Mode, Level: rec.Level, Size: rec.Size,
			Status: rec.Status, Members: map[string]*Member{}, CreatedSeq: rec.CreatedSeq,
			DeadlineUnixMS: rec.DeadlineUnixMS, EmptySinceUnixMS: rec.EmptySinceUnixMS}
		for playerID, memberRec := range rec.Members {
			if previous := occupied[playerID]; previous != "" {
				return 0, 0, nil, nil, nil, fmt.Errorf("duplicate room occupancy for %s in %s and %s", playerID, previous, rec.ID)
			}
			occupied[playerID] = rec.ID
			if queued[playerID] {
				return 0, 0, nil, nil, nil, fmt.Errorf("duplicate occupancy for %s", playerID)
			}
			member := &Member{PlayerID: playerID, Status: memberRec.Status, Disconnected: memberRec.Disconnected,
				DisconnectedSinceUnixMS: memberRec.DisconnectedSinceUnixMS, ReserveUntilUnixMS: memberRec.ReserveUntilUnixMS,
				JoinSeq: memberRec.JoinSeq}
			room.Members[playerID] = member
		}
		if room.Status == StatusProposed {
			for playerID, member := range room.Members {
				if member.Status == StatusConfirmed {
					member.Status = StatusInvited
				}
				saved.Queue = append(saved.Queue, QueueRecord{Seq: nextSeq, PlayerID: playerID, Mode: room.Mode, Level: room.Level, RoomSize: room.Size})
				nextSeq++
			}
			continue
		}
		if room.Status != StatusCanceled {
			rooms[room.ID] = room
		}
	}
	sort.SliceStable(saved.Queue, func(i, j int) bool { return saved.Queue[i].Seq < saved.Queue[j].Seq })
	maxRoom := int64(0)
	for _, rec := range saved.Rooms {
		var n int64
		fmt.Sscanf(rec.ID, "room-%d", &n)
		if n > maxRoom {
			maxRoom = n
		}
	}
	nextRoom := saved.NextRoom
	if nextRoom <= maxRoom {
		nextRoom = maxRoom + 1
	}
	results := saved.Results
	if results == nil {
		results = map[string]string{}
	}
	return nextSeq, nextRoom, saved.Queue, rooms, results, nil
}
