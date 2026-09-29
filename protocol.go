package main

// Wire protocol: newline-delimited JSON over TCP.

type InMessage struct {
	Type     string `json:"type"` // hello|enqueue|cancel|confirm|leave_room|status
	PlayerID string `json:"player_id,omitempty"`
	Mode     string `json:"mode,omitempty"`
	RoomID   string `json:"room_id,omitempty"`
	Token    string `json:"token,omitempty"`
}

type OutMessage struct {
	Type     string   `json:"type"`
	PlayerID string   `json:"player_id,omitempty"`
	Session  string   `json:"session,omitempty"`
	RoomID   string   `json:"room_id,omitempty"`
	Token    string   `json:"token,omitempty"`
	Players  []string `json:"players,omitempty"`
	Reason   string   `json:"reason,omitempty"`
	OK       bool     `json:"ok,omitempty"`
}
