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
	Status string `json:"status"`
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
	Version int           `json:"version"`
	NextSeq int64         `json:"next_seq"`
	Queue   []QueueRecord `json:"queue"`
	Rooms   []RoomRecord  `json:"rooms"`
}

type Store struct {
	path string
}

func NewStore(path string) *Store { return &Store{path: path} }

func (s *Store) Save(nextSeq int64, queue []QueueRecord, rooms map[string]*Room) error {
	records := make([]RoomRecord, 0, len(rooms))
	for _, room := range rooms {
		members := make(map[string]MemberRecord, len(room.Members))
		for id, member := range room.Members {
			members[id] = MemberRecord{Status: member.Status}
		}
		records = append(records, RoomRecord{
			ID: room.ID, Mode: room.Mode, Level: room.Level, Size: room.Size,
			Status: room.Status, Members: members, CreatedSeq: room.CreatedSeq,
			DeadlineUnixMS: room.DeadlineUnixMS, EmptySinceUnixMS: room.EmptySinceUnixMS,
		})
	}
	sort.Slice(records, func(i, j int) bool { return records[i].ID < records[j].ID })
	data, err := json.MarshalIndent(persistedState{Version: StateVersion, NextSeq: nextSeq, Queue: append([]QueueRecord(nil), queue...), Rooms: records}, "", "  ")
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

func (s *Store) Load(nowMS int64, proposalTTLMS int64) (int64, []QueueRecord, map[string]*Room, error) {
	data, err := os.ReadFile(s.path)
	if os.IsNotExist(err) {
		return 1, nil, map[string]*Room{}, nil
	}
	if err != nil {
		return 0, nil, nil, err
	}
	var saved persistedState
	if err := json.Unmarshal(data, &saved); err != nil {
		return 0, nil, nil, err
	}
	if saved.Version != StateVersion {
		return 0, nil, nil, fmt.Errorf("unsupported state version %d", saved.Version)
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
				return 0, nil, nil, fmt.Errorf("duplicate room occupancy for %s in %s and %s", playerID, previous, rec.ID)
			}
			occupied[playerID] = rec.ID
			if queued[playerID] {
				return 0, nil, nil, fmt.Errorf("duplicate occupancy for %s", playerID)
			}
			room.Members[playerID] = &Member{PlayerID: playerID, Status: memberRec.Status}
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
		rooms[room.ID] = room
	}
	sort.SliceStable(saved.Queue, func(i, j int) bool { return saved.Queue[i].Seq < saved.Queue[j].Seq })
	return nextSeq, saved.Queue, rooms, nil
}
