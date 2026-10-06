package repeater

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

type RampStep struct {
	Number  int     `json:"number"`
	Voltage float64 `json:"voltage"`
}

type RampStatus string

const (
	RampActive      RampStatus = "active"
	RampPaused      RampStatus = "paused"
	RampCompleted   RampStatus = "completed"
	RampInvalidated RampStatus = "invalidated"
)

// RampSession is the resumable state of one repeater ramp. Every step records
// the voltage command that was issued and the telemetry cursor that proved it
// stable; the session is bound to the repeater trip generation, so a
// generation or fault change invalidates it.
type RampSession struct {
	RepeaterID string                 `json:"repeater_id"`
	Generation uint64                 `json:"generation"`
	Status     RampStatus             `json:"status"`
	ReviewedBy string                 `json:"reviewed_by,omitempty"`
	StableStep int                    `json:"stable_step"`
	Planned    bool                   `json:"planned"`
	Steps      []store.RampStepRecord `json:"steps"`
	UpdatedAt  time.Time              `json:"updated_at"`
}

// NextStep returns the first step that has not completed yet.
func (s RampSession) NextStep() (store.RampStepRecord, bool) {
	for _, record := range s.Steps {
		if !record.Completed {
			return record, true
		}
	}
	return store.RampStepRecord{}, false
}

func (s *RampSession) recordOf(step int) *store.RampStepRecord {
	for index := range s.Steps {
		if s.Steps[index].Step == step {
			return &s.Steps[index]
		}
	}
	return nil
}

type RampController struct {
	actuator    *feed.Actuator
	repository  *telemetry.Repository
	window      telemetry.StabilityWindow
	checkpoints *store.Checkpoints
	now         func() time.Time

	mu       sync.Mutex
	sessions map[string]*RampSession
}

func NewRampController(actuator *feed.Actuator, repository *telemetry.Repository, window telemetry.StabilityWindow, checkpoints *store.Checkpoints, now func() time.Time) *RampController {
	return &RampController{
		actuator: actuator, repository: repository, window: window, checkpoints: checkpoints, now: now,
		sessions: make(map[string]*RampSession),
	}
}

// Start opens a planned ramp for the repeater. Starting again with the same
// plan and generation returns the existing session untouched; a different
// plan, generation, or fault invalidates the previous ramp first.
func (c *RampController) Start(_ context.Context, repeater *Repeater, plan []RampStep) (RampSession, error) {
	if len(plan) == 0 {
		return RampSession{}, fmt.Errorf("ramp plan requires at least one step")
	}
	seen := make(map[int]bool, len(plan))
	for _, step := range plan {
		if step.Number < 1 || step.Voltage <= 0 || seen[step.Number] {
			return RampSession{}, fmt.Errorf("invalid ramp plan step %d", step.Number)
		}
		seen[step.Number] = true
	}
	snapshot := repeater.Snapshot()
	if snapshot.Health == HealthTripped {
		return RampSession{}, fmt.Errorf("repeater %s is latched-trip; ramp cannot start", snapshot.ID)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if existing, ok := c.sessions[snapshot.ID]; ok && existing.Status != RampInvalidated {
		if existing.Generation == snapshot.TripGeneration && samePlan(existing.Steps, plan) {
			return *existing, nil
		}
		c.invalidateLocked(existing)
	}
	session := &RampSession{
		RepeaterID: snapshot.ID,
		Generation: snapshot.TripGeneration,
		Status:     RampActive,
		Planned:    true,
		UpdatedAt:  c.now().UTC(),
	}
	for _, step := range plan {
		session.Steps = append(session.Steps, store.RampStepRecord{Step: step.Number, Voltage: step.Voltage})
	}
	if err := c.persistLocked(session); err != nil {
		return RampSession{}, err
	}
	c.sessions[snapshot.ID] = session
	return *session, nil
}

func samePlan(records []store.RampStepRecord, plan []RampStep) bool {
	if len(records) != len(plan) {
		return false
	}
	for index, step := range plan {
		if records[index].Step != step.Number || records[index].Voltage != step.Voltage {
			return false
		}
	}
	return true
}

// ApplyStep issues the voltage command for one step and waits for a stable
// current window. A step that already completed in the session is not issued
// again: the duplicate request returns the current snapshot unchanged.
func (c *RampController) ApplyStep(ctx context.Context, repeater *Repeater, step RampStep, samplesAfter uint64) (Snapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionForLocked(repeater)
	if err != nil {
		return repeater.Snapshot(), err
	}
	if session.Status == RampPaused {
		return repeater.Snapshot(), fmt.Errorf("ramp for repeater %s is paused pending manual review", session.RepeaterID)
	}
	record := session.recordOf(step.Number)
	if record != nil && record.Completed {
		if record.Voltage != step.Voltage {
			return repeater.Snapshot(), fmt.Errorf("ramp step %d already completed with voltage %v", step.Number, record.Voltage)
		}
		return repeater.Snapshot(), nil
	}
	if session.Planned && record == nil {
		return repeater.Snapshot(), fmt.Errorf("ramp step %d is not in the plan for repeater %s", step.Number, session.RepeaterID)
	}
	if record != nil {
		if next, ok := session.NextStep(); ok && next.Step != step.Number {
			return repeater.Snapshot(), fmt.Errorf("ramp step %d must complete before step %d", next.Step, step.Number)
		}
		if record.Voltage != step.Voltage {
			return repeater.Snapshot(), fmt.Errorf("ramp step %d planned with voltage %v, got %v", step.Number, record.Voltage, step.Voltage)
		}
	}
	if session.Status == RampCompleted {
		session.Status = RampActive
	}
	command := feed.VoltageCommand{
		RepeaterID: repeater.Snapshot().ID, Step: step.Number, Voltage: step.Voltage, AcceptedAt: c.now().UTC(),
	}
	if err := c.actuator.ApplyVoltage(ctx, command); err != nil {
		return repeater.Snapshot(), err
	}
	samples := telemetry.CurrentSamples(c.repository, command.RepeaterID, samplesAfter)
	result, err := c.window.Evaluate(samples)
	if err != nil {
		return repeater.Snapshot(), err
	}
	if !result.Stable {
		return repeater.Snapshot(), fmt.Errorf("current for ramp step %d is not stable", step.Number)
	}
	now := c.now().UTC()
	if record == nil {
		session.Steps = append(session.Steps, store.RampStepRecord{Step: step.Number, Voltage: step.Voltage})
		record = &session.Steps[len(session.Steps)-1]
	}
	record.CommandAt = command.AcceptedAt
	record.Cursor = result.LastSeq
	record.Completed = true
	record.CompletedAt = now
	if step.Number > session.StableStep {
		session.StableStep = step.Number
	}
	if _, ok := session.NextStep(); !ok && len(session.Steps) > 0 {
		session.Status = RampCompleted
	}
	session.UpdatedAt = now
	if err := c.persistLocked(session); err != nil {
		return repeater.Snapshot(), err
	}
	return repeater.update(func(current *Snapshot) {
		current.StableStep = session.StableStep
		current.UpdatedAt = now
	}), nil
}

// Pause suspends the ramp so no further steps are issued until a manual
// review approves the resume.
func (c *RampController) Pause(repeater *Repeater) (RampSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionForLocked(repeater)
	if err != nil {
		return RampSession{}, err
	}
	if session.Status == RampCompleted {
		return RampSession{}, fmt.Errorf("ramp for repeater %s already completed", session.RepeaterID)
	}
	if session.Status != RampPaused {
		session.Status = RampPaused
		session.UpdatedAt = c.now().UTC()
		if err := c.persistLocked(session); err != nil {
			return RampSession{}, err
		}
	}
	return *session, nil
}

// Review records the manual review approval for a paused ramp.
func (c *RampController) Review(repeater *Repeater, reviewer string) (RampSession, error) {
	if reviewer == "" {
		return RampSession{}, fmt.Errorf("reviewer is required")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionForLocked(repeater)
	if err != nil {
		return RampSession{}, err
	}
	if session.Status != RampPaused {
		return RampSession{}, fmt.Errorf("ramp for repeater %s is not paused", session.RepeaterID)
	}
	session.ReviewedBy = reviewer
	session.UpdatedAt = c.now().UTC()
	if err := c.persistLocked(session); err != nil {
		return RampSession{}, err
	}
	return *session, nil
}

// Resume reactivates a paused, reviewed ramp; the next ApplyStep continues
// from the first incomplete step recorded in the session.
func (c *RampController) Resume(repeater *Repeater) (RampSession, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, err := c.sessionForLocked(repeater)
	if err != nil {
		return RampSession{}, err
	}
	if session.Status != RampPaused {
		return *session, nil
	}
	if session.ReviewedBy == "" {
		return RampSession{}, fmt.Errorf("ramp for repeater %s requires manual review before resume", session.RepeaterID)
	}
	session.Status = RampActive
	session.ReviewedBy = ""
	session.UpdatedAt = c.now().UTC()
	if err := c.persistLocked(session); err != nil {
		return RampSession{}, err
	}
	return *session, nil
}

func (c *RampController) SessionOf(repeaterID string) (RampSession, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	session, ok := c.sessions[repeaterID]
	if !ok {
		return RampSession{}, false
	}
	return *session, true
}

// InvalidateOnFault invalidates any live ramp for the repeater; called when a
// fault trips the repeater or its trip generation changes.
func (c *RampController) InvalidateOnFault(repeaterID string, _ uint64) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if session, ok := c.sessions[repeaterID]; ok {
		c.invalidateLocked(session)
	}
}

func (c *RampController) Recover(repeater *Repeater) (Snapshot, error) {
	checkpoint, found, err := c.checkpoints.LoadRamp(repeater.Snapshot().ID)
	if err != nil || !found {
		return repeater.Snapshot(), err
	}
	session := &RampSession{
		RepeaterID: checkpoint.RepeaterID,
		Generation: checkpoint.Generation,
		Status:     RampStatus(checkpoint.Status),
		ReviewedBy: checkpoint.ReviewedBy,
		StableStep: checkpoint.StableStep,
		Planned:    checkpoint.Planned,
		Steps:      checkpoint.Steps,
		UpdatedAt:  checkpoint.UpdatedAt.UTC(),
	}
	if session.Status == "" {
		session.Status = RampActive
	}
	c.mu.Lock()
	c.sessions[session.RepeaterID] = session
	// A ramp recovered against a changed generation or an active fault is
	// stale: invalidate it instead of resuming.
	_ = c.validateLocked(session, repeater.Snapshot())
	c.mu.Unlock()
	return repeater.update(func(current *Snapshot) {
		current.StableStep = checkpoint.StableStep
		current.UpdatedAt = checkpoint.UpdatedAt.UTC()
	}), nil
}

func (c *RampController) sessionForLocked(repeater *Repeater) (*RampSession, error) {
	snapshot := repeater.Snapshot()
	session, ok := c.sessions[snapshot.ID]
	if !ok {
		session = &RampSession{
			RepeaterID: snapshot.ID, Generation: snapshot.TripGeneration,
			Status: RampActive, UpdatedAt: c.now().UTC(),
		}
		c.sessions[snapshot.ID] = session
	}
	if err := c.validateLocked(session, snapshot); err != nil {
		return nil, err
	}
	return session, nil
}

func (c *RampController) validateLocked(session *RampSession, snapshot Snapshot) error {
	if session.Status == RampInvalidated {
		return fmt.Errorf("ramp for repeater %s is invalidated", session.RepeaterID)
	}
	if snapshot.TripGeneration != session.Generation || snapshot.Health == HealthTripped {
		c.invalidateLocked(session)
		return fmt.Errorf("ramp for repeater %s invalidated by generation or fault change", session.RepeaterID)
	}
	return nil
}

func (c *RampController) invalidateLocked(session *RampSession) {
	if session.Status == RampInvalidated {
		return
	}
	session.Status = RampInvalidated
	session.UpdatedAt = c.now().UTC()
	_ = c.persistLocked(session)
}

func (c *RampController) persistLocked(session *RampSession) error {
	return c.checkpoints.SaveRamp(store.RampCheckpoint{
		RepeaterID: session.RepeaterID,
		StableStep: session.StableStep,
		UpdatedAt:  session.UpdatedAt,
		Generation: session.Generation,
		Status:     string(session.Status),
		ReviewedBy: session.ReviewedBy,
		Planned:    session.Planned,
		Steps:      session.Steps,
	})
}
