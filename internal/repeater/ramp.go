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

type RampState string

const (
	RampActive      RampState = "active"
	RampPaused      RampState = "paused"
	RampCompleted   RampState = "completed"
	RampInvalidated RampState = "invalidated"
)

type RampStepRecord struct {
	Step         RampStep            `json:"step"`
	Command      feed.VoltageCommand `json:"command"`
	CursorBefore uint64              `json:"cursor_before"`
	CursorAfter  uint64              `json:"cursor_after"`
	CompletedAt  time.Time           `json:"completed_at"`
}

type RampSnapshot struct {
	RepeaterID    string           `json:"repeater_id"`
	Generation    uint64           `json:"generation"`
	State         RampState        `json:"state"`
	Plan          []RampStep       `json:"plan,omitempty"`
	Records       []RampStepRecord `json:"records,omitempty"`
	StableStep    int              `json:"stable_step"`
	NextStep      int              `json:"next_step"`
	ReviewedBy    string           `json:"reviewed_by,omitempty"`
	InvalidReason string           `json:"invalid_reason,omitempty"`
	CreatedAt     time.Time        `json:"created_at"`
	UpdatedAt     time.Time        `json:"updated_at"`
}

type RampOutcome struct {
	Repeater      Snapshot       `json:"repeater"`
	Run           RampSnapshot   `json:"run"`
	Record        RampStepRecord `json:"record"`
	CommandIssued bool           `json:"command_issued"`
}

type rampRun struct {
	snapshot RampSnapshot
}

type RampController struct {
	actuator    *feed.Actuator
	repository  *telemetry.Repository
	window      telemetry.StabilityWindow
	checkpoints *store.Checkpoints
	now         func() time.Time
	mu          sync.Mutex
	runs        map[string]*rampRun
}

func NewRampController(actuator *feed.Actuator, repository *telemetry.Repository, window telemetry.StabilityWindow, checkpoints *store.Checkpoints, now func() time.Time) *RampController {
	return &RampController{
		actuator: actuator, repository: repository, window: window, checkpoints: checkpoints, now: now,
		runs: make(map[string]*rampRun),
	}
}

// Start opens a new ramp run bound to the repeater's current trip generation.
// A run that is still active or paused must finish or be invalidated first.
func (c *RampController) Start(repeater *Repeater, plan []RampStep) (RampSnapshot, error) {
	for index, step := range plan {
		if step.Number != index+1 || step.Voltage <= 0 {
			return RampSnapshot{}, fmt.Errorf("ramp plan must number steps sequentially from 1 with positive voltage")
		}
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	current := repeater.Snapshot()
	if existing, ok := c.runs[current.ID]; ok {
		state := existing.snapshot.State
		if state == RampActive || state == RampPaused {
			return RampSnapshot{}, fmt.Errorf("ramp for repeater %s is already %s", current.ID, state)
		}
	}
	run := &rampRun{snapshot: RampSnapshot{
		RepeaterID: current.ID,
		Generation: current.TripGeneration,
		State:      RampActive,
		Plan:       append([]RampStep(nil), plan...),
		CreatedAt:  c.now().UTC(),
		UpdatedAt:  c.now().UTC(),
	}}
	c.recompute(&run.snapshot)
	c.runs[current.ID] = run
	if err := c.persistLocked(&run.snapshot); err != nil {
		return RampSnapshot{}, err
	}
	return copyRamp(run.snapshot), nil
}

// ApplyStep issues the voltage command for the next incomplete step and records
// the telemetry cursor bracketing it. Requests for an already-completed step are
// replayed from the run record without re-issuing a command.
func (c *RampController) ApplyStep(ctx context.Context, repeater *Repeater, step RampStep, samplesAfter uint64) (RampOutcome, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	current := repeater.Snapshot()
	run, ok := c.runs[current.ID]
	if !ok {
		run = &rampRun{snapshot: RampSnapshot{
			RepeaterID: current.ID,
			Generation: current.TripGeneration,
			State:      RampActive,
			CreatedAt:  c.now().UTC(),
			UpdatedAt:  c.now().UTC(),
		}}
		c.recompute(&run.snapshot)
		c.runs[current.ID] = run
	}
	if err := c.checkValidLocked(&run.snapshot, current); err != nil {
		_ = c.persistLocked(&run.snapshot)
		return RampOutcome{}, err
	}
	if run.snapshot.State == RampInvalidated {
		return RampOutcome{}, fmt.Errorf("ramp for repeater %s was invalidated: %s", current.ID, run.snapshot.InvalidReason)
	}
	if record, found := findRecord(run.snapshot.Records, step.Number); found {
		return RampOutcome{Repeater: current, Run: copyRamp(run.snapshot), Record: record}, nil
	}
	if step.Number >= 1 && step.Number <= run.snapshot.StableStep {
		// Completed before a restart without a stored record; never re-issue.
		return RampOutcome{Repeater: current, Run: copyRamp(run.snapshot)}, nil
	}
	switch run.snapshot.State {
	case RampPaused:
		return RampOutcome{}, fmt.Errorf("ramp for repeater %s is paused: manual review required before resume", current.ID)
	case RampCompleted:
		return RampOutcome{}, fmt.Errorf("ramp for repeater %s is already completed", current.ID)
	}
	if step.Number != run.snapshot.NextStep {
		return RampOutcome{}, fmt.Errorf("next incomplete ramp step for repeater %s is %d, not %d", current.ID, run.snapshot.NextStep, step.Number)
	}
	if planned, found := findPlanned(run.snapshot.Plan, step.Number); found && planned.Voltage != step.Voltage {
		return RampOutcome{}, fmt.Errorf("ramp step %d voltage %.2f does not match planned %.2f", step.Number, step.Voltage, planned.Voltage)
	}
	cursorBefore := samplesAfter
	if cursorBefore == 0 && len(run.snapshot.Records) > 0 {
		cursorBefore = run.snapshot.Records[len(run.snapshot.Records)-1].CursorAfter
	}
	command := feed.VoltageCommand{
		RepeaterID: current.ID, Step: step.Number, Voltage: step.Voltage, AcceptedAt: c.now().UTC(),
	}
	if err := c.actuator.ApplyVoltage(ctx, command); err != nil {
		return RampOutcome{}, err
	}
	samples := telemetry.CurrentSamples(c.repository, command.RepeaterID, cursorBefore)
	result, err := c.window.Evaluate(samples)
	if err != nil {
		return RampOutcome{}, err
	}
	if !result.Stable {
		return RampOutcome{}, fmt.Errorf("current for ramp step %d is not stable", step.Number)
	}
	record := RampStepRecord{
		Step: step, Command: command, CursorBefore: cursorBefore, CursorAfter: result.LastSeq,
		CompletedAt: c.now().UTC(),
	}
	run.snapshot.Records = append(run.snapshot.Records, record)
	run.snapshot.UpdatedAt = c.now().UTC()
	c.recompute(&run.snapshot)
	if err := c.persistLocked(&run.snapshot); err != nil {
		return RampOutcome{}, err
	}
	after := repeater.update(func(snapshot *Snapshot) {
		snapshot.StableStep = run.snapshot.StableStep
		snapshot.UpdatedAt = c.now().UTC()
	})
	return RampOutcome{Repeater: after, Run: copyRamp(run.snapshot), Record: record, CommandIssued: true}, nil
}

// Pause halts an active run so no further voltage commands are issued until a
// reviewed resume.
func (c *RampController) Pause(repeater *Repeater) (RampSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	run, err := c.runLocked(repeater.Snapshot().ID)
	if err != nil {
		return RampSnapshot{}, err
	}
	if err := c.checkValidLocked(&run.snapshot, repeater.Snapshot()); err != nil {
		_ = c.persistLocked(&run.snapshot)
		return RampSnapshot{}, err
	}
	if run.snapshot.State != RampActive {
		return RampSnapshot{}, fmt.Errorf("cannot pause ramp for repeater %s in state %s", run.snapshot.RepeaterID, run.snapshot.State)
	}
	run.snapshot.State = RampPaused
	run.snapshot.UpdatedAt = c.now().UTC()
	if err := c.persistLocked(&run.snapshot); err != nil {
		return RampSnapshot{}, err
	}
	return copyRamp(run.snapshot), nil
}

// Resume reactivates a paused run after manual review; the next ApplyStep
// continues from the first incomplete step.
func (c *RampController) Resume(repeater *Repeater, reviewedBy string) (RampSnapshot, error) {
	if reviewedBy == "" {
		return RampSnapshot{}, fmt.Errorf("manual review attribution is required to resume a ramp")
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	run, err := c.runLocked(repeater.Snapshot().ID)
	if err != nil {
		return RampSnapshot{}, err
	}
	if err := c.checkValidLocked(&run.snapshot, repeater.Snapshot()); err != nil {
		_ = c.persistLocked(&run.snapshot)
		return RampSnapshot{}, err
	}
	if run.snapshot.State != RampPaused {
		return RampSnapshot{}, fmt.Errorf("cannot resume ramp for repeater %s in state %s", run.snapshot.RepeaterID, run.snapshot.State)
	}
	run.snapshot.State = RampActive
	run.snapshot.ReviewedBy = reviewedBy
	run.snapshot.UpdatedAt = c.now().UTC()
	if err := c.persistLocked(&run.snapshot); err != nil {
		return RampSnapshot{}, err
	}
	return copyRamp(run.snapshot), nil
}

// Invalidate marks a run dead so it can never issue another voltage command.
func (c *RampController) Invalidate(repeater *Repeater, reason string) (RampSnapshot, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	run, err := c.runLocked(repeater.Snapshot().ID)
	if err != nil {
		return RampSnapshot{}, err
	}
	if run.snapshot.State == RampActive || run.snapshot.State == RampPaused {
		run.snapshot.State = RampInvalidated
		run.snapshot.InvalidReason = reason
		run.snapshot.UpdatedAt = c.now().UTC()
		if err := c.persistLocked(&run.snapshot); err != nil {
			return RampSnapshot{}, err
		}
	}
	return copyRamp(run.snapshot), nil
}

// Status returns the current run for a repeater, lazily invalidating it when
// the repeater's generation or fault state has moved on.
func (c *RampController) Status(repeater *Repeater) (RampSnapshot, bool, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	run, ok := c.runs[repeater.Snapshot().ID]
	if !ok {
		return RampSnapshot{}, false, nil
	}
	if err := c.checkValidLocked(&run.snapshot, repeater.Snapshot()); err != nil {
		_ = c.persistLocked(&run.snapshot)
	}
	return copyRamp(run.snapshot), true, nil
}

// Recover reloads the persisted run and restores repeater progress after a
// restart; completed steps are never re-issued afterwards.
func (c *RampController) Recover(repeater *Repeater) (Snapshot, error) {
	checkpoint, found, err := c.checkpoints.LoadRamp(repeater.Snapshot().ID)
	if err != nil || !found {
		return repeater.Snapshot(), err
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	run := &rampRun{snapshot: RampSnapshot{
		RepeaterID:    checkpoint.RepeaterID,
		Generation:    checkpoint.Generation,
		State:         RampState(checkpoint.State),
		StableStep:    checkpoint.StableStep,
		ReviewedBy:    checkpoint.ReviewedBy,
		InvalidReason: checkpoint.InvalidReason,
		UpdatedAt:     checkpoint.UpdatedAt.UTC(),
	}}
	if run.snapshot.State == "" {
		run.snapshot.State = RampActive
	}
	for _, step := range checkpoint.Plan {
		run.snapshot.Plan = append(run.snapshot.Plan, RampStep{Number: step.Number, Voltage: step.Voltage})
	}
	current := repeater.Snapshot()
	for _, stored := range checkpoint.Records {
		run.snapshot.Records = append(run.snapshot.Records, RampStepRecord{
			Step: RampStep{Number: stored.StepNumber, Voltage: stored.Voltage},
			Command: feed.VoltageCommand{
				RepeaterID: checkpoint.RepeaterID, Step: stored.StepNumber,
				Voltage: stored.Voltage, AcceptedAt: stored.CommandAcceptedAt.UTC(),
			},
			CursorBefore: stored.CursorBefore, CursorAfter: stored.CursorAfter,
			CompletedAt: stored.CompletedAt.UTC(),
		})
	}
	run.snapshot.CreatedAt = run.snapshot.UpdatedAt
	c.recompute(&run.snapshot)
	c.runs[current.ID] = run
	return repeater.update(func(snapshot *Snapshot) {
		snapshot.StableStep = run.snapshot.StableStep
		snapshot.UpdatedAt = checkpoint.UpdatedAt.UTC()
	}), nil
}

func (c *RampController) runLocked(repeaterID string) (*rampRun, error) {
	run, ok := c.runs[repeaterID]
	if !ok {
		return nil, fmt.Errorf("no ramp found for repeater %s", repeaterID)
	}
	return run, nil
}

// checkValidLocked invalidates the run when the repeater's trip generation has
// changed or a fault has latched since the run started.
func (c *RampController) checkValidLocked(run *RampSnapshot, current Snapshot) error {
	if run.State == RampCompleted || run.State == RampInvalidated {
		return nil
	}
	reason := ""
	switch {
	case current.TripGeneration != run.Generation:
		reason = fmt.Sprintf("repeater generation changed from %d to %d", run.Generation, current.TripGeneration)
	case current.Health == HealthTripped:
		reason = "repeater fault latched"
	}
	if reason == "" {
		return nil
	}
	run.State = RampInvalidated
	run.InvalidReason = reason
	run.UpdatedAt = c.now().UTC()
	return fmt.Errorf("ramp for repeater %s invalidated: %s", run.RepeaterID, reason)
}

// recompute derives StableStep, NextStep and terminal completion from the
// recorded steps.
func (c *RampController) recompute(run *RampSnapshot) {
	for _, record := range run.Records {
		if record.Step.Number > run.StableStep {
			run.StableStep = record.Step.Number
		}
	}
	run.NextStep = 0
	if len(run.Plan) > 0 {
		for _, step := range run.Plan {
			if _, found := findRecord(run.Records, step.Number); !found {
				run.NextStep = step.Number
				break
			}
		}
		if run.NextStep == 0 && run.State == RampActive {
			run.State = RampCompleted
			run.UpdatedAt = c.now().UTC()
		}
	} else if run.State == RampActive || run.State == RampPaused {
		run.NextStep = run.StableStep + 1
	}
}

func (c *RampController) persistLocked(run *RampSnapshot) error {
	checkpoint := store.RampCheckpoint{
		RepeaterID:    run.RepeaterID,
		StableStep:    run.StableStep,
		UpdatedAt:     run.UpdatedAt,
		Generation:    run.Generation,
		State:         string(run.State),
		ReviewedBy:    run.ReviewedBy,
		InvalidReason: run.InvalidReason,
	}
	for _, step := range run.Plan {
		checkpoint.Plan = append(checkpoint.Plan, store.RampPlanStep{Number: step.Number, Voltage: step.Voltage})
	}
	for _, record := range run.Records {
		checkpoint.Records = append(checkpoint.Records, store.RampStepRecord{
			StepNumber: record.Step.Number, Voltage: record.Command.Voltage,
			CursorBefore: record.CursorBefore, CursorAfter: record.CursorAfter,
			CommandAcceptedAt: record.Command.AcceptedAt, CompletedAt: record.CompletedAt,
		})
	}
	return c.checkpoints.SaveRamp(checkpoint)
}

func findRecord(records []RampStepRecord, number int) (RampStepRecord, bool) {
	for _, record := range records {
		if record.Step.Number == number {
			return record, true
		}
	}
	return RampStepRecord{}, false
}

func findPlanned(plan []RampStep, number int) (RampStep, bool) {
	for _, step := range plan {
		if step.Number == number {
			return step, true
		}
	}
	return RampStep{}, false
}

func copyRamp(run RampSnapshot) RampSnapshot {
	run.Plan = append([]RampStep(nil), run.Plan...)
	run.Records = append([]RampStepRecord(nil), run.Records...)
	return run
}
