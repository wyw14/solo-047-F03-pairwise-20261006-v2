package store

import (
	"fmt"
	"time"
)

type RampPlanStep struct {
	Number  int     `json:"number"`
	Voltage float64 `json:"voltage"`
}

type RampStepRecord struct {
	StepNumber        int       `json:"step_number"`
	Voltage           float64   `json:"voltage"`
	CursorBefore      uint64    `json:"cursor_before"`
	CursorAfter       uint64    `json:"cursor_after"`
	CommandAcceptedAt time.Time `json:"command_accepted_at"`
	CompletedAt       time.Time `json:"completed_at"`
}

type RampCheckpoint struct {
	RepeaterID    string           `json:"repeater_id"`
	StableStep    int              `json:"stable_step"`
	UpdatedAt     time.Time        `json:"updated_at"`
	Generation    uint64           `json:"generation,omitempty"`
	State         string           `json:"state,omitempty"`
	Plan          []RampPlanStep   `json:"plan,omitempty"`
	Records       []RampStepRecord `json:"records,omitempty"`
	ReviewedBy    string           `json:"reviewed_by,omitempty"`
	InvalidReason string           `json:"invalid_reason,omitempty"`
}

type HeartbeatSnapshot struct {
	StationID string    `json:"station_id"`
	SeenAt    time.Time `json:"seen_at"`
	Sequence  uint64    `json:"sequence"`
}

type Checkpoints struct {
	snapshots *SnapshotStore
}

func NewCheckpoints(snapshots *SnapshotStore) *Checkpoints {
	return &Checkpoints{snapshots: snapshots}
}

func (c *Checkpoints) SaveRamp(value RampCheckpoint) error {
	return c.snapshots.Save(c.Key("ramp", value.RepeaterID), value)
}

func (c *Checkpoints) LoadRamp(repeaterID string) (RampCheckpoint, bool, error) {
	var value RampCheckpoint
	found, err := c.snapshots.Load(c.Key("ramp", repeaterID), &value)
	return value, found, err
}

func (c *Checkpoints) SaveHeartbeat(value HeartbeatSnapshot) error {
	return c.snapshots.Save(c.Key("heartbeat", value.StationID), value)
}

func (c *Checkpoints) LoadHeartbeat(stationID string) (HeartbeatSnapshot, bool, error) {
	var value HeartbeatSnapshot
	found, err := c.snapshots.Load(c.Key("heartbeat", stationID), &value)
	return value, found, err
}

func (c *Checkpoints) Key(kind, id string) string {
	return fmt.Sprintf("%s-%s", kind, id)
}
