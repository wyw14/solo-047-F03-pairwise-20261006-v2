package repeater

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

var rampClock = time.Date(2026, 10, 5, 8, 0, 0, 0, time.UTC)

func newRampController(actuator *feed.Actuator, repository *telemetry.Repository, checkpoints *store.Checkpoints) *RampController {
	return NewRampController(actuator, repository,
		telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5, MinimumPeriod: time.Second},
		checkpoints, func() time.Time { return rampClock })
}

func rampRig(t *testing.T) (*feed.Actuator, *telemetry.Repository, *store.Checkpoints, *RampController, *Repeater) {
	t.Helper()
	repository := telemetry.NewRepository()
	checkpoints := store.NewCheckpoints(store.NewSnapshotStore(t.TempDir()))
	actuator := feed.NewActuator()
	controller := newRampController(actuator, repository, checkpoints)
	target, err := New("R07", rampClock)
	if err != nil {
		t.Fatal(err)
	}
	return actuator, repository, checkpoints, controller, target
}

func addCurrentSamples(t *testing.T, repository *telemetry.Repository, repeaterID string, sequences ...uint64) {
	t.Helper()
	for _, sequence := range sequences {
		sample := telemetry.Sample{
			ID:         fmt.Sprintf("sample-%s-%d", repeaterID, sequence),
			AssetID:    repeaterID,
			Kind:       telemetry.KindCurrent,
			Value:      10 + float64(sequence)*0.1,
			Sequence:   sequence,
			ObservedAt: rampClock.Add(time.Duration(sequence) * 2 * time.Second),
		}
		if err := repository.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRampPauseReviewResumeFlow(t *testing.T) {
	actuator, repository, _, controller, target := rampRig(t)
	ctx := context.Background()
	plan := []RampStep{{Number: 1, Voltage: 120}, {Number: 2, Voltage: 240}}

	session, err := controller.Start(ctx, target, plan)
	if err != nil {
		t.Fatal(err)
	}
	if session.Status != RampActive || len(session.Steps) != 2 {
		t.Fatalf("unexpected session: %+v", session)
	}

	// Duplicate start with the same plan returns the existing session.
	again, err := controller.Start(ctx, target, plan)
	if err != nil {
		t.Fatal(err)
	}
	if again.UpdatedAt != session.UpdatedAt {
		t.Fatalf("duplicate start reset the session: %+v", again)
	}

	addCurrentSamples(t, repository, "R07", 1, 2)
	if _, err := controller.ApplyStep(ctx, target, plan[0], 0); err != nil {
		t.Fatal(err)
	}
	if commands := len(actuator.Commands()); commands != 1 {
		t.Fatalf("expected 1 voltage command, got %d", commands)
	}

	// Duplicate request for the completed step must not re-issue the command.
	if _, err := controller.ApplyStep(ctx, target, plan[0], 0); err != nil {
		t.Fatal(err)
	}
	if commands := len(actuator.Commands()); commands != 1 {
		t.Fatalf("duplicate step re-issued a command, got %d commands", commands)
	}
	if _, err := controller.ApplyStep(ctx, target, RampStep{Number: 1, Voltage: 999}, 0); err == nil {
		t.Fatal("expected conflict for completed step with different voltage")
	}

	// A step outside the plan is rejected.
	if _, err := controller.ApplyStep(ctx, target, RampStep{Number: 3, Voltage: 1}, 0); err == nil {
		t.Fatal("expected step outside the plan to be rejected")
	}

	paused, err := controller.Pause(target)
	if err != nil {
		t.Fatal(err)
	}
	if paused.Status != RampPaused {
		t.Fatalf("expected paused session, got %+v", paused)
	}
	if _, err := controller.ApplyStep(ctx, target, plan[1], 2); err == nil ||
		!strings.Contains(err.Error(), "paused") {
		t.Fatalf("expected paused ramp to reject steps, got %v", err)
	}
	if _, err := controller.Resume(target); err == nil ||
		!strings.Contains(err.Error(), "review") {
		t.Fatalf("expected resume to require review, got %v", err)
	}
	if _, err := controller.Review(target, "operator-1"); err != nil {
		t.Fatal(err)
	}
	resumed, err := controller.Resume(target)
	if err != nil {
		t.Fatal(err)
	}
	if resumed.Status != RampActive {
		t.Fatalf("expected active session after resume, got %+v", resumed)
	}
	next, ok := resumed.NextStep()
	if !ok || next.Step != 2 {
		t.Fatalf("expected resume at step 2, got %+v", next)
	}

	addCurrentSamples(t, repository, "R07", 3, 4)
	if _, err := controller.ApplyStep(ctx, target, plan[1], 2); err != nil {
		t.Fatal(err)
	}
	session, _ = controller.SessionOf("R07")
	if session.Status != RampCompleted || session.StableStep != 2 {
		t.Fatalf("expected completed session at step 2, got %+v", session)
	}
	if session.Steps[0].Cursor != 2 || session.Steps[1].Cursor != 4 {
		t.Fatalf("expected telemetry cursors 2 and 4, got %+v", session.Steps)
	}
	if session.Steps[0].CommandAt.IsZero() || session.Steps[1].Voltage != 240 {
		t.Fatalf("expected voltage commands recorded per step, got %+v", session.Steps)
	}
	if commands := len(actuator.Commands()); commands != 2 {
		t.Fatalf("expected 2 voltage commands, got %d", commands)
	}
}

func TestRampInvalidatedByGenerationAndFault(t *testing.T) {
	_, repository, _, controller, target := rampRig(t)
	ctx := context.Background()
	plan := []RampStep{{Number: 1, Voltage: 120}, {Number: 2, Voltage: 240}}
	if _, err := controller.Start(ctx, target, plan); err != nil {
		t.Fatal(err)
	}
	addCurrentSamples(t, repository, "R07", 1, 2)
	if _, err := controller.ApplyStep(ctx, target, plan[0], 0); err != nil {
		t.Fatal(err)
	}

	// Trip generation moves: the old ramp must be invalidated.
	target.update(func(current *Snapshot) { current.TripGeneration = 7 })
	if _, err := controller.ApplyStep(ctx, target, plan[1], 2); err == nil ||
		!strings.Contains(err.Error(), "invalidated") {
		t.Fatalf("expected invalidation after generation change, got %v", err)
	}
	session, _ := controller.SessionOf("R07")
	if session.Status != RampInvalidated {
		t.Fatalf("expected invalidated session, got %+v", session)
	}

	// A latched-trip fault invalidates eagerly through the health projector.
	_, _, _, controller2, target2 := rampRig(t)
	projector := NewHealthProjector(incident.NewRepeaterLifecycle(), 85)
	projector.Register(target2)
	projector.SetInvalidator(controller2)
	if _, err := controller2.Start(ctx, target2, plan); err != nil {
		t.Fatal(err)
	}
	if err := projector.ApplySample(ctx, telemetry.Sample{
		ID: "hot-1", AssetID: "R07", Kind: telemetry.KindTemperature, Value: 90,
		Sequence: 9, Generation: 3, ObservedAt: rampClock,
	}); err != nil {
		t.Fatal(err)
	}
	session, _ = controller2.SessionOf("R07")
	if session.Status != RampInvalidated {
		t.Fatalf("expected fault to invalidate the ramp, got %+v", session)
	}
	if _, err := controller2.Start(ctx, target2, plan); err == nil {
		t.Fatal("expected start to reject a latched-trip repeater")
	}
}

func TestRampRecoverRestoresSession(t *testing.T) {
	_, repository, checkpoints, controller, target := rampRig(t)
	ctx := context.Background()
	plan := []RampStep{{Number: 1, Voltage: 120}, {Number: 2, Voltage: 240}}
	if _, err := controller.Start(ctx, target, plan); err != nil {
		t.Fatal(err)
	}
	addCurrentSamples(t, repository, "R07", 1, 2)
	if _, err := controller.ApplyStep(ctx, target, plan[0], 0); err != nil {
		t.Fatal(err)
	}

	// A fresh controller over the same checkpoint store recovers the session.
	recoveredActuator := feed.NewActuator()
	recoveredController := newRampController(recoveredActuator, repository, checkpoints)
	if _, err := recoveredController.Recover(target); err != nil {
		t.Fatal(err)
	}
	session, ok := recoveredController.SessionOf("R07")
	if !ok || session.StableStep != 1 || !session.Steps[0].Completed || session.Steps[0].Cursor != 2 {
		t.Fatalf("unexpected recovered session: %+v", session)
	}
	// The recovered ramp must not re-issue the completed step.
	if _, err := recoveredController.ApplyStep(ctx, target, plan[0], 0); err != nil {
		t.Fatal(err)
	}
	if commands := len(recoveredActuator.Commands()); commands != 0 {
		t.Fatalf("recovered ramp re-issued a completed step: %d commands", commands)
	}

	// Recovery against a changed generation invalidates the stale ramp.
	target.update(func(current *Snapshot) { current.TripGeneration = 11 })
	if _, err := recoveredController.Recover(target); err != nil {
		t.Fatal(err)
	}
	session, _ = recoveredController.SessionOf("R07")
	if session.Status != RampInvalidated {
		t.Fatalf("expected stale recovered ramp to be invalidated, got %+v", session)
	}
}
