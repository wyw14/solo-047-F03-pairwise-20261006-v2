package repeater

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/telemetry"
)

var rampClock = time.Date(2026, 10, 5, 0, 0, 0, 0, time.UTC)

func rampFixture(t *testing.T) (*RampController, *feed.Actuator, *telemetry.Repository, *Repeater) {
	t.Helper()
	repository := telemetry.NewRepository()
	actuator := feed.NewActuator()
	checkpoints := store.NewCheckpoints(store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots")))
	window := telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5, MinimumPeriod: time.Second}
	controller := NewRampController(actuator, repository, window, checkpoints, func() time.Time { return rampClock })
	asset, err := New("R07", rampClock)
	if err != nil {
		t.Fatal(err)
	}
	return controller, actuator, repository, asset
}

func addCurrentSamples(t *testing.T, repository *telemetry.Repository, from, to uint64) {
	t.Helper()
	for sequence := from; sequence <= to; sequence++ {
		sample := telemetry.Sample{
			ID: fmt.Sprintf("current-%d", sequence), AssetID: "R07", Kind: telemetry.KindCurrent,
			Value: 10 + float64(sequence)*0.1, Unit: "A", Sequence: sequence,
			ObservedAt: rampClock.Add(time.Duration(sequence) * time.Second),
		}
		if err := repository.Add(sample); err != nil {
			t.Fatal(err)
		}
	}
}

func TestRampRecordsCursorsAndCommandsPerStep(t *testing.T) {
	controller, actuator, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	plan := []RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}
	run, err := controller.Start(asset, plan)
	if err != nil {
		t.Fatal(err)
	}
	if run.State != RampActive || run.NextStep != 1 {
		t.Fatalf("unexpected run after start: %+v", run)
	}

	outcome, err := controller.ApplyStep(context.Background(), asset, plan[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.CommandIssued || outcome.Record.CursorBefore != 0 || outcome.Record.CursorAfter != 3 {
		t.Fatalf("unexpected first step outcome: %+v", outcome)
	}
	if outcome.Record.Command.Voltage != 110 || outcome.Record.Command.Step != 1 {
		t.Fatalf("voltage command not recorded: %+v", outcome.Record.Command)
	}
	if outcome.Repeater.StableStep != 1 {
		t.Fatalf("repeater stable step=%d", outcome.Repeater.StableStep)
	}

	addCurrentSamples(t, repository, 4, 6)
	outcome, err = controller.ApplyStep(context.Background(), asset, plan[1], 0)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.CursorBefore != 3 || outcome.Record.CursorAfter != 6 {
		t.Fatalf("telemetry cursor did not chain across steps: %+v", outcome.Record)
	}
	if outcome.Run.State != RampCompleted || outcome.Run.NextStep != 0 {
		t.Fatalf("run should be completed: %+v", outcome.Run)
	}
	if commands := actuator.Commands(); len(commands) != 2 {
		t.Fatalf("expected 2 voltage commands, got %d", len(commands))
	}
}

func TestRampRejectsOutOfOrderStep(t *testing.T) {
	controller, actuator, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	if _, err := controller.Start(asset, []RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyStep(context.Background(), asset, RampStep{Number: 2, Voltage: 220}, 0); err == nil {
		t.Fatal("expected out-of-order step to be rejected")
	}
	if commands := actuator.Commands(); len(commands) != 0 {
		t.Fatalf("no command should be issued, got %d", len(commands))
	}
}

func TestRampDoesNotReissueCompletedStep(t *testing.T) {
	controller, actuator, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	if _, err := controller.Start(asset, []RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}); err != nil {
		t.Fatal(err)
	}
	first, err := controller.ApplyStep(context.Background(), asset, RampStep{Number: 1, Voltage: 110}, 0)
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := controller.ApplyStep(context.Background(), asset, RampStep{Number: 1, Voltage: 110}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.CommandIssued {
		t.Fatal("completed step must not issue another voltage command")
	}
	if replayed.Record != first.Record {
		t.Fatalf("replayed record mismatch: %+v vs %+v", replayed.Record, first.Record)
	}
	if commands := actuator.Commands(); len(commands) != 1 {
		t.Fatalf("expected 1 voltage command, got %d", len(commands))
	}
}

func TestRampPauseRequiresReviewedResume(t *testing.T) {
	controller, actuator, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	plan := []RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}
	if _, err := controller.Start(asset, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyStep(context.Background(), asset, plan[0], 0); err != nil {
		t.Fatal(err)
	}
	paused, err := controller.Pause(asset)
	if err != nil {
		t.Fatal(err)
	}
	if paused.State != RampPaused {
		t.Fatalf("expected paused run, got %s", paused.State)
	}
	if _, err := controller.ApplyStep(context.Background(), asset, plan[1], 0); err == nil {
		t.Fatal("paused ramp must not apply steps")
	}
	if _, err := controller.Resume(asset, ""); err == nil {
		t.Fatal("resume without review attribution must fail")
	}
	resumed, err := controller.Resume(asset, "operator-li")
	if err != nil {
		t.Fatal(err)
	}
	if resumed.State != RampActive || resumed.NextStep != 2 || resumed.ReviewedBy != "operator-li" {
		t.Fatalf("unexpected resumed run: %+v", resumed)
	}
	addCurrentSamples(t, repository, 4, 6)
	outcome, err := controller.ApplyStep(context.Background(), asset, plan[1], 0)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.Record.CursorBefore != 3 {
		t.Fatalf("resumed step should chain from recorded cursor 3, got %d", outcome.Record.CursorBefore)
	}
	if commands := actuator.Commands(); len(commands) != 2 {
		t.Fatalf("expected 2 voltage commands, got %d", len(commands))
	}
}

func TestRampInvalidatesOnGenerationChange(t *testing.T) {
	controller, actuator, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 6)
	plan := []RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}
	if _, err := controller.Start(asset, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyStep(context.Background(), asset, plan[0], 0); err != nil {
		t.Fatal(err)
	}
	asset.update(func(current *Snapshot) { current.TripGeneration = 2 })

	if _, err := controller.ApplyStep(context.Background(), asset, plan[1], 0); err == nil {
		t.Fatal("ramp from an older generation must be invalidated")
	}
	if _, err := controller.ApplyStep(context.Background(), asset, plan[0], 0); err == nil {
		t.Fatal("invalidated ramp must not replay completed steps")
	}
	if commands := actuator.Commands(); len(commands) != 1 {
		t.Fatalf("invalidation must not issue commands, got %d", len(commands))
	}
	run, found, err := controller.Status(asset)
	if err != nil || !found {
		t.Fatalf("status: found=%v err=%v", found, err)
	}
	if run.State != RampInvalidated || run.InvalidReason == "" {
		t.Fatalf("expected invalidated run with reason: %+v", run)
	}

	restarted, err := controller.Start(asset, plan)
	if err != nil {
		t.Fatal(err)
	}
	if restarted.Generation != 2 || restarted.State != RampActive {
		t.Fatalf("new run should bind to generation 2: %+v", restarted)
	}
}

func TestRampInvalidatesOnLatchedFault(t *testing.T) {
	controller, _, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	if _, err := controller.Start(asset, []RampStep{{Number: 1, Voltage: 110}}); err != nil {
		t.Fatal(err)
	}
	asset.update(func(current *Snapshot) { current.Health = HealthTripped })
	if _, err := controller.ApplyStep(context.Background(), asset, RampStep{Number: 1, Voltage: 110}, 0); err == nil {
		t.Fatal("ramp must invalidate when a fault is latched")
	}
	run, found, err := controller.Status(asset)
	if err != nil || !found {
		t.Fatalf("status: found=%v err=%v", found, err)
	}
	if run.State != RampInvalidated {
		t.Fatalf("expected invalidated run, got %s", run.State)
	}
}

func TestRampRecoverRestoresRunWithoutReissuing(t *testing.T) {
	controller, _, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	plan := []RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}
	if _, err := controller.Start(asset, plan); err != nil {
		t.Fatal(err)
	}
	if _, err := controller.ApplyStep(context.Background(), asset, plan[0], 0); err != nil {
		t.Fatal(err)
	}

	freshActuator := feed.NewActuator()
	// Recover through a new controller backed by the same checkpoint store.
	recovered := NewRampController(freshActuator, repository, telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5, MinimumPeriod: time.Second}, controller.checkpoints, func() time.Time { return rampClock })
	snapshot, err := recovered.Recover(asset)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StableStep != 1 {
		t.Fatalf("recovered stable step=%d", snapshot.StableStep)
	}
	replayed, err := recovered.ApplyStep(context.Background(), asset, plan[0], 0)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.CommandIssued {
		t.Fatal("recovered run must not re-issue a completed step")
	}
	addCurrentSamples(t, repository, 4, 6)
	outcome, err := recovered.ApplyStep(context.Background(), asset, plan[1], 0)
	if err != nil {
		t.Fatal(err)
	}
	if !outcome.CommandIssued || outcome.Record.CursorBefore != 3 {
		t.Fatalf("recovery should resume at the next incomplete step: %+v", outcome)
	}
	if commands := freshActuator.Commands(); len(commands) != 1 {
		t.Fatalf("only the resumed step may be issued, got %d", len(commands))
	}
}

func TestRampRecoverLegacyCheckpointFloor(t *testing.T) {
	controller, actuator, repository, asset := rampFixture(t)
	addCurrentSamples(t, repository, 1, 3)
	if err := controller.checkpoints.SaveRamp(store.RampCheckpoint{RepeaterID: "R07", StableStep: 2, UpdatedAt: rampClock}); err != nil {
		t.Fatal(err)
	}
	snapshot, err := controller.Recover(asset)
	if err != nil {
		t.Fatal(err)
	}
	if snapshot.StableStep != 2 {
		t.Fatalf("recovered stable step=%d", snapshot.StableStep)
	}
	outcome, err := controller.ApplyStep(context.Background(), asset, RampStep{Number: 2, Voltage: 220}, 0)
	if err != nil {
		t.Fatal(err)
	}
	if outcome.CommandIssued {
		t.Fatal("step at or below the recovered floor must not be re-issued")
	}
	if commands := actuator.Commands(); len(commands) != 0 {
		t.Fatalf("no command expected, got %d", len(commands))
	}
}
