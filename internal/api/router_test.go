package api

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"testing"
	"time"

	"github.com/wyw14/cry-103/internal/feed"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/landing"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/platform"
	"github.com/wyw14/cry-103/internal/repair"
	"github.com/wyw14/cry-103/internal/repeater"
	"github.com/wyw14/cry-103/internal/route"
	"github.com/wyw14/cry-103/internal/span"
	"github.com/wyw14/cry-103/internal/store"
	"github.com/wyw14/cry-103/internal/switchover"
	"github.com/wyw14/cry-103/internal/telemetry"
	"github.com/wyw14/cry-103/internal/topology"
)

func testServer(t *testing.T) *Server {
	t.Helper()
	clock := fixedClock{now: time.Date(2026, 8, 24, 3, 0, 0, 0, time.UTC)}
	ids := platform.UUIDGenerator{}
	registry := topology.NewRegistry()
	if _, err := registry.Register("S3", topology.PathPrimary, clock.Now()); err != nil {
		t.Fatal(err)
	}
	target, err := span.New("S3", "S3", 126, clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	path, err := route.New("R-S3", "S3", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	drainer := route.NewDrainer(0)
	activator := route.NewActivator(registry)
	receipts := landing.NewReceiptRegistry()
	permitService := repair.NewPermitService(feed.NewIsolationService(), receipts, ids, clock.Now)
	circuit, err := feed.NewCircuit("B", "east", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	permitService.RegisterCircuit(circuit)
	repeaterAsset, err := repeater.New("R07", clock.Now())
	if err != nil {
		t.Fatal(err)
	}
	lifecycle := incident.NewRepeaterLifecycle()
	projector := repeater.NewHealthProjector(lifecycle, 85)
	projector.Register(repeaterAsset)
	repository := telemetry.NewRepository()
	actuator := feed.NewActuator()
	checkpoints := store.NewCheckpoints(store.NewSnapshotStore(filepath.Join(t.TempDir(), "snapshots")))
	acceptance := repair.NewAcceptanceService(switchover.NewRestorer(drainer, activator, registry, ids, clock.Now), clock.Now)
	acceptance.Register("S3", target, path)
	services := &Services{
		Clock: clock, IDs: ids, Journal: store.NewJournal(filepath.Join(t.TempDir(), "events.jsonl")),
		Spans: map[string]*span.Span{"S3": target}, Routes: map[string]*route.Route{"R-S3": path},
		Repeaters: map[string]*repeater.Repeater{"R07": repeaterAsset}, Topology: registry,
		OTDR:        otdr.NewSessionService(registry, landing.NewScanner(registry, 0), ids, clock.Now),
		Switchovers: switchover.NewSessionService(drainer, activator, ids, clock.Now, time.Second),
		Permits:     permitService, Receipts: receipts, Acceptance: acceptance,
		Telemetry: telemetry.NewIngestor(repository, clock.Now, projector), TelemetryRepository: repository,
		Admission: incident.NewAdmissionService(telemetry.NewMuteManager(clock.Now)),
		Ramps: repeater.NewRampController(actuator, repository,
			telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5}, checkpoints, clock.Now),
		Actuator: actuator,
	}
	return NewServer(services)
}

type fixedClock struct {
	now time.Time
}

func (c fixedClock) Now() time.Time {
	return c.now
}

func TestHealthAndScanHTTPFlow(t *testing.T) {
	server := testServer(t)
	health := httptest.NewRecorder()
	server.Handler().ServeHTTP(health, httptest.NewRequest(http.MethodGet, "/healthz", nil))
	if health.Code != http.StatusOK {
		t.Fatalf("health status=%d body=%s", health.Code, health.Body.String())
	}

	body, _ := json.Marshal(ScanRequest{StationID: "east", LengthKM: 126, Chunks: 3})
	scan := httptest.NewRecorder()
	server.Handler().ServeHTTP(scan, httptest.NewRequest(http.MethodPost, "/api/spans/S3/otdr-scans", bytes.NewReader(body)))
	if scan.Code != http.StatusCreated {
		t.Fatalf("scan status=%d body=%s", scan.Code, scan.Body.String())
	}
	var trace otdr.Trace
	if err := json.Unmarshal(scan.Body.Bytes(), &trace); err != nil {
		t.Fatal(err)
	}
	if trace.SpanID != "S3" || len(trace.Chunks) != 3 {
		t.Fatalf("unexpected trace: %+v", trace)
	}

	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/system/status", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("status code=%d body=%s", status.Code, status.Body.String())
	}
}

func TestRampPauseResumeHTTPFlow(t *testing.T) {
	server := testServer(t)
	post := func(path string, payload any) *httptest.ResponseRecorder {
		t.Helper()
		body, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		return recorder
	}
	type rampPayload struct {
		Ramp          repeater.RampSnapshot `json:"ramp"`
		CommandIssued bool                  `json:"command_issued"`
		Commands      []feed.VoltageCommand `json:"commands"`
	}
	decodeRamp := func(recorder *httptest.ResponseRecorder) rampPayload {
		t.Helper()
		var payload rampPayload
		if err := json.Unmarshal(recorder.Body.Bytes(), &payload); err != nil {
			t.Fatalf("decode ramp response: %v body=%s", err, recorder.Body.String())
		}
		return payload
	}
	plan := []repeater.RampStep{{Number: 1, Voltage: 110}, {Number: 2, Voltage: 220}}

	started := post("/api/repeaters/R07/ramp", RampRequest{Action: "start", Steps: plan})
	if started.Code != http.StatusCreated {
		t.Fatalf("start status=%d body=%s", started.Code, started.Body.String())
	}
	if run := decodeRamp(started).Ramp; run.State != repeater.RampActive || run.NextStep != 1 {
		t.Fatalf("unexpected started run: %+v", run)
	}

	paused := post("/api/repeaters/R07/ramp", RampRequest{Action: "pause"})
	if paused.Code != http.StatusOK || decodeRamp(paused).Ramp.State != repeater.RampPaused {
		t.Fatalf("pause status=%d body=%s", paused.Code, paused.Body.String())
	}
	if blocked := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 110}); blocked.Code != http.StatusConflict {
		t.Fatalf("paused ramp must reject steps, status=%d", blocked.Code)
	}
	if unreviewed := post("/api/repeaters/R07/ramp", RampRequest{Action: "resume"}); unreviewed.Code != http.StatusConflict {
		t.Fatalf("resume without review must fail, status=%d", unreviewed.Code)
	}
	resumed := post("/api/repeaters/R07/ramp", RampRequest{Action: "resume", ReviewedBy: "operator-li"})
	if resumed.Code != http.StatusOK || decodeRamp(resumed).Ramp.State != repeater.RampActive {
		t.Fatalf("resume status=%d body=%s", resumed.Code, resumed.Body.String())
	}

	for sequence := uint64(1); sequence <= 2; sequence++ {
		if sample := post("/api/repeaters/R07/telemetry", TelemetryRequest{Kind: "current", Value: 10, Sequence: sequence}); sample.Code != http.StatusAccepted {
			t.Fatalf("telemetry status=%d body=%s", sample.Code, sample.Body.String())
		}
	}
	first := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 110})
	if first.Code != http.StatusOK || !decodeRamp(first).CommandIssued {
		t.Fatalf("first step status=%d body=%s", first.Code, first.Body.String())
	}
	duplicate := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 110})
	if duplicate.Code != http.StatusOK {
		t.Fatalf("duplicate step status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	if payload := decodeRamp(duplicate); payload.CommandIssued || len(payload.Commands) != 1 {
		t.Fatalf("completed step must not be re-issued: %+v", payload)
	}
	if mismatch := post("/api/repeaters/R07/ramp", RampRequest{Step: 2, Voltage: 300}); mismatch.Code != http.StatusConflict {
		t.Fatalf("voltage mismatch with plan must fail, status=%d", mismatch.Code)
	}

	for sequence := uint64(3); sequence <= 4; sequence++ {
		post("/api/repeaters/R07/telemetry", TelemetryRequest{Kind: "current", Value: 10, Sequence: sequence})
	}
	second := post("/api/repeaters/R07/ramp", RampRequest{Step: 2, Voltage: 220})
	if second.Code != http.StatusOK || decodeRamp(second).Ramp.State != repeater.RampCompleted {
		t.Fatalf("second step status=%d body=%s", second.Code, second.Body.String())
	}

	if restarted := post("/api/repeaters/R07/ramp", RampRequest{Action: "start", Steps: plan}); restarted.Code != http.StatusCreated {
		t.Fatalf("restart after completion status=%d body=%s", restarted.Code, restarted.Body.String())
	}
	for sequence := uint64(5); sequence <= 6; sequence++ {
		post("/api/repeaters/R07/telemetry", TelemetryRequest{Kind: "current", Value: 10, Sequence: sequence})
	}
	if step := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 110}); step.Code != http.StatusOK {
		t.Fatalf("new run first step status=%d body=%s", step.Code, step.Body.String())
	}
	if trip := post("/api/repeaters/R07/telemetry", TelemetryRequest{Kind: "temperature", Value: 90, Sequence: 7, Generation: 1}); trip.Code != http.StatusAccepted {
		t.Fatalf("trip sample status=%d body=%s", trip.Code, trip.Body.String())
	}
	if invalidated := post("/api/repeaters/R07/ramp", RampRequest{Step: 2, Voltage: 220}); invalidated.Code != http.StatusConflict {
		t.Fatalf("fault must invalidate the old ramp, status=%d", invalidated.Code)
	}
	if replay := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 110}); replay.Code != http.StatusConflict {
		t.Fatalf("invalidated ramp must not re-issue completed steps, status=%d", replay.Code)
	}
	status := httptest.NewRecorder()
	server.Handler().ServeHTTP(status, httptest.NewRequest(http.MethodGet, "/api/repeaters/R07/ramp", nil))
	if status.Code != http.StatusOK {
		t.Fatalf("get ramp status=%d body=%s", status.Code, status.Body.String())
	}
	if payload := decodeRamp(status); payload.Ramp.State != repeater.RampInvalidated || payload.Ramp.InvalidReason == "" {
		t.Fatalf("expected invalidated ramp with reason: %+v", payload.Ramp)
	}
}
