package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
)

type Server struct {
	services *Services
	router   chi.Router
}

func NewServer(services *Services) *Server {
	server := &Server{services: services}
	router := chi.NewRouter()
	router.Use(recoverer)
	router.Get("/healthz", server.health)
	router.Route("/api", func(r chi.Router) {
		r.Get("/system/status", server.systemStatus)
		r.Get("/events", server.listEvents)
		r.Get("/topology/{id}", server.getTopology)
		r.Get("/otdr-scans", server.listScans)
		r.Get("/otdr-scans/{id}", server.getScan)
		r.Post("/spans/{id}/otdr-scans", server.startScan)
		r.Get("/spans/{id}", server.getSpan)
		r.Post("/routes/{id}/switchovers", server.startSwitchover)
		r.Get("/routes/{id}", server.getRoute)
		r.Post("/landings/{id}/feed-isolations", server.startIsolation)
		r.Post("/landings/{id}/feed-receipts", server.recordReceipt)
		r.Get("/landings/{id}/feed-receipts", server.listReceipts)
		r.Get("/landings/{id}/health", server.getLandingHealth)
		r.Post("/permits/{id}/evaluate", server.evaluatePermit)
		r.Post("/permits/{id}/ground", server.groundPermit)
		r.Get("/permits/{id}", server.getPermit)
		r.Post("/repairs/{id}/acceptance", server.addAcceptanceProof)
		r.Get("/repairs/{id}/acceptance", server.getAcceptance)
		r.Post("/repeaters/{id}/telemetry", server.ingestTelemetry)
		r.Get("/repeaters/{id}/telemetry", server.repeaterTelemetry)
		r.Post("/repeaters/{id}/ramp", server.rampRepeater)
		r.Post("/repeaters/{id}/ramp/start", server.startRamp)
		r.Get("/repeaters/{id}/ramp", server.getRamp)
		r.Post("/repeaters/{id}/ramp/pause", server.pauseRamp)
		r.Post("/repeaters/{id}/ramp/review", server.reviewRamp)
		r.Post("/repeaters/{id}/ramp/resume", server.resumeRamp)
		r.Post("/repeaters/{id}/reset", server.resetRepeater)
		r.Get("/repeaters/{id}", server.getRepeater)
		r.Post("/spans/{id}/alarms", server.admitAlarm)
		r.Get("/spans/{id}/alarms", server.listAlarms)
		r.Post("/spans/{id}/wet-plant-tests", server.startWetPlantTest)
		r.Post("/wet-plant-tests/{id}", server.finishWetPlantTest)
		r.Post("/spans/{id}/incidents/{incidentID}", server.updateIncident)
	})
	server.router = router
	return server
}

func (s *Server) Handler() http.Handler {
	return s.router
}

func (s *Server) health(writer http.ResponseWriter, _ *http.Request) {
	writeJSON(writer, http.StatusOK, HealthResponse{
		Status: "ok", Service: "cableguard", Timestamp: s.services.Clock.Now().UTC(),
	})
}

func recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(writer http.ResponseWriter, request *http.Request) {
		defer func() {
			if recovered := recover(); recovered != nil {
				writeJSON(writer, http.StatusInternalServerError, ErrorResponse{Error: "internal server error"})
			}
		}()
		started := time.Now()
		next.ServeHTTP(writer, request)
		_ = started
	})
}
