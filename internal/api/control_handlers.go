package api

import (
	"net/http"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/wyw14/cry-103/internal/incident"
	"github.com/wyw14/cry-103/internal/otdr"
	"github.com/wyw14/cry-103/internal/repeater"
)

func (s *Server) getLandingHealth(writer http.ResponseWriter, request *http.Request) {
	if chi.URLParam(request, "id") != s.services.Station.Snapshot().ID {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "landing station not found"})
		return
	}
	writeJSON(writer, http.StatusOK, s.services.Health.Evaluate(s.services.Station, s.services.Clock.Now()))
}

func (s *Server) rampRepeater(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input RampRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	result, err := s.services.Ramps.ApplyStep(request.Context(), target, repeater.RampStep{Number: input.Step, Voltage: input.Voltage}, input.SamplesAfter)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	session, _ := s.services.Ramps.SessionOf(result.ID)
	writeJSON(writer, http.StatusOK, map[string]any{
		"repeater": result, "session": rampView(session), "commands": s.services.Actuator.Commands(),
	})
}

func (s *Server) startRamp(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	var input RampStartRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	session, err := s.services.Ramps.Start(request.Context(), target, input.Steps)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusCreated, rampView(session))
}

func (s *Server) getRamp(writer http.ResponseWriter, request *http.Request) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	session, ok := s.services.Ramps.SessionOf(target.Snapshot().ID)
	if !ok {
		writeJSON(writer, http.StatusNotFound, ErrorResponse{Error: "ramp session not found"})
		return
	}
	writeJSON(writer, http.StatusOK, rampView(session))
}

func (s *Server) pauseRamp(writer http.ResponseWriter, request *http.Request) {
	s.updateRamp(writer, request, func(target *repeater.Repeater) (repeater.RampSession, error) {
		return s.services.Ramps.Pause(target)
	})
}

func (s *Server) reviewRamp(writer http.ResponseWriter, request *http.Request) {
	var input RampReviewRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	s.updateRamp(writer, request, func(target *repeater.Repeater) (repeater.RampSession, error) {
		return s.services.Ramps.Review(target, input.Reviewer)
	})
}

func (s *Server) resumeRamp(writer http.ResponseWriter, request *http.Request) {
	s.updateRamp(writer, request, func(target *repeater.Repeater) (repeater.RampSession, error) {
		return s.services.Ramps.Resume(target)
	})
}

func (s *Server) updateRamp(writer http.ResponseWriter, request *http.Request, action func(*repeater.Repeater) (repeater.RampSession, error)) {
	target, err := s.services.Repeater(chi.URLParam(request, "id"))
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	session, err := action(target)
	if err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	writeJSON(writer, http.StatusOK, rampView(session))
}

func rampView(session repeater.RampSession) map[string]any {
	view := map[string]any{"session": session}
	if next, ok := session.NextStep(); ok {
		view["next_step"] = next
	}
	return view
}

func (s *Server) resetRepeater(writer http.ResponseWriter, request *http.Request) {
	var input ResetRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	if err := s.services.RepeaterHealth.Reset(chi.URLParam(request, "id"), input.Generation); err != nil {
		writeError(writer, http.StatusConflict, err)
		return
	}
	target, _ := s.services.Repeater(chi.URLParam(request, "id"))
	writeJSON(writer, http.StatusOK, target.Snapshot())
}

func (s *Server) admitAlarm(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	var input AlarmRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	alarm := incident.Alarm{ID: s.services.IDs.New("alarm"), SpanID: spanID, Kind: input.Kind, ObservedAt: s.services.Clock.Now()}
	if !s.services.Admission.Accept(alarm) {
		writeJSON(writer, http.StatusAccepted, map[string]any{"admitted": false, "alarm": alarm})
		return
	}
	writeJSON(writer, http.StatusCreated, map[string]any{"admitted": true, "alarm": alarm})
}

func (s *Server) listAlarms(writer http.ResponseWriter, request *http.Request) {
	writeJSON(writer, http.StatusOK, map[string]any{"items": s.services.Admission.Accepted(chi.URLParam(request, "id"))})
}

func (s *Server) startWetPlantTest(writer http.ResponseWriter, request *http.Request) {
	snapshot := s.services.StartWetPlantTest(request.Context(), chi.URLParam(request, "id"))
	writeJSON(writer, http.StatusCreated, snapshot)
}

func (s *Server) finishWetPlantTest(writer http.ResponseWriter, request *http.Request) {
	var input TestRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	snapshot, err := s.services.FinishWetPlantTest(chi.URLParam(request, "id"), input.Action)
	if err != nil {
		writeError(writer, http.StatusNotFound, err)
		return
	}
	writeJSON(writer, http.StatusOK, snapshot)
}

func (s *Server) updateIncident(writer http.ResponseWriter, request *http.Request) {
	spanID := chi.URLParam(request, "id")
	incidentID := chi.URLParam(request, "incidentID")
	var input IncidentRequest
	if err := decodeJSON(request, &input); err != nil {
		writeError(writer, http.StatusBadRequest, err)
		return
	}
	current := s.services.Incident(incidentID, spanID)
	switch input.Action {
	case "local-complete":
		s.services.Classifier.LocalComplete(current, input.RequiresPeer, s.services.Clock.Now())
	case "peer-evidence":
		pair := s.services.Correlator.Add(otdr.Evidence{
			IncidentID: incidentID, StationID: input.StationID, TraceID: input.TraceID,
			DistanceKM: input.DistanceKM, ObservedAt: s.services.Clock.Now(),
		})
		_ = s.services.Correlator.Pair(incidentID)
		if pair.Complete || input.StationID != "" {
			s.services.Classifier.PeerEvidence(current, s.services.Clock.Now())
		}
	case "expire":
		s.services.Classifier.Expire(current, s.services.Clock.Now().Add(11*time.Second))
	default:
		writeJSON(writer, http.StatusBadRequest, ErrorResponse{Error: "unsupported incident action"})
		return
	}
	writeJSON(writer, http.StatusOK, current.Snapshot())
}
