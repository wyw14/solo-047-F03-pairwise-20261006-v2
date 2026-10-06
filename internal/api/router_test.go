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
	checkpoints := store.NewCheckpoints(store.NewSnapshotStore(t.TempDir()))
	ramps := repeater.NewRampController(actuator, repository,
		telemetry.StabilityWindow{MinimumSamples: 2, MaximumSpread: 1.5, MinimumPeriod: 0}, checkpoints, clock.Now)
	projector.SetInvalidator(ramps)
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
		Ramps:     ramps, Actuator: actuator, RepeaterHealth: projector,
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

func TestRampPauseReviewResumeHTTPFlow(t *testing.T) {
	server := testServer(t)
	post := func(path string, payload any) *httptest.ResponseRecorder {
		body, _ := json.Marshal(payload)
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodPost, path, bytes.NewReader(body)))
		return recorder
	}
	get := func(path string) *httptest.ResponseRecorder {
		recorder := httptest.NewRecorder()
		server.Handler().ServeHTTP(recorder, httptest.NewRequest(http.MethodGet, path, nil))
		return recorder
	}

	started := post("/api/repeaters/R07/ramp/start", RampStartRequest{
		Steps: []repeater.RampStep{{Number: 1, Voltage: 120}, {Number: 2, Voltage: 240}},
	})
	if started.Code != http.StatusCreated {
		t.Fatalf("start status=%d body=%s", started.Code, started.Body.String())
	}

	for _, sequence := range []uint64{1, 2} {
		sample := post("/api/repeaters/R07/telemetry", TelemetryRequest{Kind: "current", Value: 10, Unit: "A", Sequence: sequence})
		if sample.Code != http.StatusAccepted {
			t.Fatalf("telemetry status=%d body=%s", sample.Code, sample.Body.String())
		}
	}
	step := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 120})
	if step.Code != http.StatusOK {
		t.Fatalf("step 1 status=%d body=%s", step.Code, step.Body.String())
	}
	// Duplicate request must not re-issue the completed step.
	duplicate := post("/api/repeaters/R07/ramp", RampRequest{Step: 1, Voltage: 120})
	if duplicate.Code != http.StatusOK {
		t.Fatalf("duplicate step 1 status=%d body=%s", duplicate.Code, duplicate.Body.String())
	}
	var duplicateBody struct {
		Commands []feed.VoltageCommand `json:"commands"`
	}
	if err := json.Unmarshal(duplicate.Body.Bytes(), &duplicateBody); err != nil {
		t.Fatal(err)
	}
	if len(duplicateBody.Commands) != 1 {
		t.Fatalf("expected 1 voltage command after duplicate, got %d", len(duplicateBody.Commands))
	}

	if paused := post("/api/repeaters/R07/ramp/pause", nil); paused.Code != http.StatusOK {
		t.Fatalf("pause status=%d body=%s", paused.Code, paused.Body.String())
	}
	if blocked := post("/api/repeaters/R07/ramp", RampRequest{Step: 2, Voltage: 240, SamplesAfter: 2}); blocked.Code != http.StatusConflict {
		t.Fatalf("expected paused ramp conflict, got %d body=%s", blocked.Code, blocked.Body.String())
	}
	if unreviewed := post("/api/repeaters/R07/ramp/resume", nil); unreviewed.Code != http.StatusConflict {
		t.Fatalf("expected resume to require review, got %d body=%s", unreviewed.Code, unreviewed.Body.String())
	}
	if reviewed := post("/api/repeaters/R07/ramp/review", RampReviewRequest{Reviewer: "operator-1"}); reviewed.Code != http.StatusOK {
		t.Fatalf("review status=%d body=%s", reviewed.Code, reviewed.Body.String())
	}
	resumed := post("/api/repeaters/R07/ramp/resume", nil)
	if resumed.Code != http.StatusOK {
		t.Fatalf("resume status=%d body=%s", resumed.Code, resumed.Body.String())
	}
	var resumedBody struct {
		NextStep struct {
			Step int `json:"step"`
		} `json:"next_step"`
	}
	if err := json.Unmarshal(resumed.Body.Bytes(), &resumedBody); err != nil {
		t.Fatal(err)
	}
	if resumedBody.NextStep.Step != 2 {
		t.Fatalf("expected resume at step 2, got %d", resumedBody.NextStep.Step)
	}

	for _, sequence := range []uint64{3, 4} {
		post("/api/repeaters/R07/telemetry", TelemetryRequest{Kind: "current", Value: 10, Unit: "A", Sequence: sequence})
	}
	if finished := post("/api/repeaters/R07/ramp", RampRequest{Step: 2, Voltage: 240, SamplesAfter: 2}); finished.Code != http.StatusOK {
		t.Fatalf("step 2 status=%d body=%s", finished.Code, finished.Body.String())
	}
	session := get("/api/repeaters/R07/ramp")
	if session.Code != http.StatusOK {
		t.Fatalf("session status=%d body=%s", session.Code, session.Body.String())
	}
	var sessionBody struct {
		Session repeater.RampSession `json:"session"`
	}
	if err := json.Unmarshal(session.Body.Bytes(), &sessionBody); err != nil {
		t.Fatal(err)
	}
	if sessionBody.Session.Status != repeater.RampCompleted || sessionBody.Session.Steps[1].Cursor != 4 {
		t.Fatalf("unexpected session after resume: %+v", sessionBody.Session)
	}
}
