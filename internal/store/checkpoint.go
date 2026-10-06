package store

import (
	"fmt"
	"time"
)

// RampStepRecord captures one ramp step: the voltage command that was issued
// and the telemetry cursor (last current-sample sequence) that proved it
// stable, so a resumed or recovered ramp never re-issues a completed step.
type RampStepRecord struct {
	Step        int       `json:"step"`
	Voltage     float64   `json:"voltage"`
	CommandAt   time.Time `json:"command_at,omitempty"`
	Cursor      uint64    `json:"cursor"`
	Completed   bool      `json:"completed"`
	CompletedAt time.Time `json:"completed_at,omitempty"`
}

type RampCheckpoint struct {
	RepeaterID string           `json:"repeater_id"`
	StableStep int              `json:"stable_step"`
	UpdatedAt  time.Time        `json:"updated_at"`
	Generation uint64           `json:"generation"`
	Status     string           `json:"status"`
	ReviewedBy string           `json:"reviewed_by,omitempty"`
	Planned    bool             `json:"planned"`
	Steps      []RampStepRecord `json:"steps,omitempty"`
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
