package main

import (
	"encoding/json"
	"os"
	"path/filepath"
)

type PlayerSnap struct {
	ID     string `json:"id"`
	Status string `json:"status"`
	RoomID string `json:"room_id,omitempty"`
}

type SlotSnap struct {
	PlayerID  string `json:"player_id"`
	Session   string `json:"session"`
	Token     string `json:"token"`
	Confirmed bool   `json:"confirmed"`
}

type RoomSnap struct {
	ID    string     `json:"id"`
	Mode  string     `json:"mode"`
	State string     `json:"state"`
	Slots []SlotSnap `json:"slots"`
}

type Snapshot struct {
	Seq     int          `json:"seq"`
	Players []PlayerSnap `json:"players"`
	Rooms   []RoomSnap   `json:"rooms"`
}

type Store struct{ path string }

func NewStore(dir string) *Store { return &Store{path: filepath.Join(dir, "state.json")} }

func (st *Store) Save(s Snapshot) {
	b, err := json.MarshalIndent(s, "", "  ")
	if err != nil {
		return
	}
	tmp := st.path + ".tmp"
	if err := os.WriteFile(tmp, b, 0o644); err != nil {
		return
	}
	_ = os.Rename(tmp, st.path) // atomic replace
}

func (st *Store) Load() (*Snapshot, error) {
	b, err := os.ReadFile(st.path)
	if err != nil {
		return nil, err
	}
	var s Snapshot
	if err := json.Unmarshal(b, &s); err != nil {
		return nil, err
	}
	return &s, nil
}
