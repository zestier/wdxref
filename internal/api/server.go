package api

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/httpencoding"
	"github.com/ekeid/ekeid/internal/model"
	"github.com/ekeid/ekeid/internal/store"
)

const apiReadTimeout = 5 * time.Second

// Server holds dependencies for the API handlers.
type Server struct {
	reader    *store.Reader
	version   string
	encodings []string
}

// NewServer creates a new API server. The encodings parameter controls which
// compression encodings the server will offer (nil = defaults).
func NewServer(reader *store.Reader, version string, encodings []string) *Server {
	return &Server{reader: reader, version: version, encodings: encodings}
}

// Handler returns the top-level HTTP handler with all routes and middleware.
func (s *Server) Handler() http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /v1/health", s.handleHealth)
	mux.HandleFunc("GET /v1/stats", s.handleStats)
	mux.HandleFunc("GET /v1/lookup/{key}/{value...}", s.handleLookup)
	mux.HandleFunc("GET /v1/lookup/{qid}", s.handleWikidataLookup)

	return s.applyMiddleware(mux)
}

func (s *Server) applyMiddleware(next http.Handler) http.Handler {
	compressed := httpencoding.Middleware(next, s.encodings)

	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// CORS headers
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")

		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		compressed.ServeHTTP(w, r)
	})
}

func (s *Server) handleLookup(w http.ResponseWriter, r *http.Request) {
	key := r.PathValue("key")
	value := r.PathValue("value")

	if value == "" {
		writeError(w, http.StatusBadRequest, "invalid_request", "Value is required")
		return
	}

	// key must be P followed by digits (e.g. P345)
	if len(key) < 2 || (key[0] != 'P' && key[0] != 'p') {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid property key %q. Must be P followed by digits (e.g. P345)", key))
		return
	}
	property, err := strconv.Atoi(key[1:])
	if err != nil || property <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid property key %q. Must be P followed by digits (e.g. P345)", key))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), apiReadTimeout)
	defer cancel()

	result, err := s.reader.LookupByPropertyContext(ctx, property, value)
	if err != nil {
		s.writeReaderError(w, err)
		return
	}

	resp := make([]lookupResponse, len(result))
	for i, r := range result {
		resp[i] = lookupResponse{
			WikidataID: fmt.Sprintf("Q%d", r.WikidataID),
			Mappings:   r.Mappings,
		}
	}

	w.Header().Set("Cache-Control", "public, max-age=3600")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleWikidataLookup(w http.ResponseWriter, r *http.Request) {
	qid := r.PathValue("qid")

	if len(qid) < 2 || (qid[0] != 'Q' && qid[0] != 'q') {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid Wikidata ID %q. Must be Q followed by digits (e.g. Q172241)", qid))
		return
	}
	wikidataID, err := strconv.ParseInt(qid[1:], 10, 64)
	if err != nil || wikidataID <= 0 {
		writeError(w, http.StatusBadRequest, "invalid_request",
			fmt.Sprintf("Invalid Wikidata ID %q. Must be Q followed by digits (e.g. Q172241)", qid))
		return
	}

	ctx, cancel := context.WithTimeout(r.Context(), apiReadTimeout)
	defer cancel()

	result, err := s.reader.LookupByWikidataIDContext(ctx, wikidataID)
	if err != nil {
		s.writeReaderError(w, err)
		return
	}

	if result == nil {
		writeError(w, http.StatusNotFound, "not_found",
			fmt.Sprintf("No entity found for %s", strings.ToUpper(qid[:1])+qid[1:]))
		return
	}

	wikidataIDStr := fmt.Sprintf("Q%d", result.WikidataID)

	w.Header().Set("Cache-Control", "public, max-age=3600")
	writeJSON(w, http.StatusOK, lookupResponse{
		WikidataID: wikidataIDStr,
		Mappings:   result.Mappings,
	})
}

func (s *Server) handleHealth(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), apiReadTimeout)
	defer cancel()

	info, err := s.reader.GetHealthContext(ctx)
	if err != nil {
		s.writeReaderError(w, err)
		return
	}

	resp := healthResponse{commonResponse: s.newCommonResponse(info)}

	w.Header().Set("Cache-Control", "no-cache")
	writeJSON(w, http.StatusOK, resp)
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	ctx, cancel := context.WithTimeout(r.Context(), apiReadTimeout)
	defer cancel()

	stats, err := s.reader.GetStatsContext(ctx)
	if err != nil {
		s.writeReaderError(w, err)
		return
	}

	resp := statsResponse{
		commonResponse:  s.newCommonResponse(&stats.HealthInfo),
		LastEventID:     stats.LastEventID,
		EntityCount:     stats.EntityCount,
		PendingCount:    stats.PendingCount,
		ProcessingCount: stats.ProcessingCount,
		FailedCount:     stats.FailedCount,
		StreamLength:    stats.StreamLength,
	}
	if !stats.OldestEvent.IsZero() {
		resp.OldestEvent = formatAPITime(stats.OldestEvent)
	}
	if !stats.NewestEvent.IsZero() {
		resp.NewestEvent = formatAPITime(stats.NewestEvent)
	}

	w.Header().Set("Cache-Control", "no-cache")
	writeJSON(w, http.StatusOK, resp)
}

// Response types

type lookupResponse struct {
	WikidataID string   `json:"wikidata_id"`
	Mappings   []string `json:"mappings"`
}

type commonResponse struct {
	Status        string `json:"status"`
	Version       string `json:"version"`
	State         string `json:"state,omitempty"`
	DumpTime      string `json:"dump_time,omitempty"`
	LastEventSync string `json:"last_event_sync,omitempty"`
	DatabaseSize  int64  `json:"database_size"`
	SchemaMatch   bool   `json:"schema_match"`
}

type healthResponse struct {
	commonResponse
}

type statsResponse struct {
	commonResponse
	LastEventID     string `json:"last_event_id,omitempty"`
	OldestEvent     string `json:"oldest_event,omitempty"`
	NewestEvent     string `json:"newest_event,omitempty"`
	EntityCount     int64  `json:"entity_count"`
	PendingCount    int64  `json:"pending_count"`
	ProcessingCount int64  `json:"processing_count"`
	FailedCount     int64  `json:"failed_count"`
	StreamLength    int64  `json:"stream_length"`
}

type errorResponse struct {
	Error   string `json:"error"`
	Message string `json:"message"`
}

// Helpers

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeError(w http.ResponseWriter, status int, code, message string) {
	writeJSON(w, status, errorResponse{Error: code, Message: message})
}

func (s *Server) newCommonResponse(info *model.HealthInfo) commonResponse {
	resp := commonResponse{
		Status:       "ok",
		Version:      s.version,
		State:        info.State,
		DatabaseSize: info.DatabaseSize,
		SchemaMatch:  info.SchemaMatch,
	}
	if !info.DumpTime.IsZero() {
		resp.DumpTime = formatAPITime(info.DumpTime)
	}
	if !info.LastEventSync.IsZero() {
		resp.LastEventSync = formatAPITime(info.LastEventSync)
	}
	return resp
}

func formatAPITime(t time.Time) string {
	return t.Format("2006-01-02T15:04:05Z")
}

func (s *Server) writeReaderError(w http.ResponseWriter, err error) {
	if errors.Is(err, store.ErrSchemaMismatch) {
		writeError(w, http.StatusServiceUnavailable, "unavailable", "Schema migration in progress")
		return
	}
	if errors.Is(err, context.DeadlineExceeded) || errors.Is(err, context.Canceled) {
		writeError(w, http.StatusGatewayTimeout, "timeout", "Backend read timed out")
		return
	}
	writeError(w, http.StatusInternalServerError, "internal_error", "An internal error occurred")
}
