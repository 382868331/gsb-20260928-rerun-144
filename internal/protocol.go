package internal

type Request struct {
	Type      string `json:"type"`
	SessionID string `json:"session_id,omitempty"`
	PlayerID  string `json:"player_id,omitempty"`
	Mode      string `json:"mode,omitempty"`
	Level     int    `json:"level,omitempty"`
	RoomSize  int    `json:"room_size,omitempty"`
	RoomID    string `json:"room_id,omitempty"`
}

type Response struct {
	Type       string   `json:"type"`
	OK         bool     `json:"ok"`
	SessionID  string   `json:"session_id,omitempty"`
	RoomID     string   `json:"room_id,omitempty"`
	Mode       string   `json:"mode,omitempty"`
	RoomSize   int      `json:"room_size,omitempty"`
	Members    []string `json:"members,omitempty"`
	Waiting    []string `json:"waiting,omitempty"`
	Reason     string   `json:"reason,omitempty"`
	Message    string   `json:"message,omitempty"`
	Status     string   `json:"status,omitempty"`
	DeadlineMS int64    `json:"deadline_ms,omitempty"`
}
