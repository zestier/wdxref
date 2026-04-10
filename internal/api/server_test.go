package api

import (
	"context"
	"net/http"
	"net/http/httptest"
	"slices"
	"testing"

	"github.com/alicebob/miniredis/v2"
	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

func setupTestServer(t *testing.T) (*Server, *store.Writer) {
	t.Helper()
	s := miniredis.RunT(t)
	c, err := store.NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	w := store.NewWriter(c)
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	r := store.NewReader(c)
	srv := NewServer(r, "0.1.0-test")
	return srv, w
}

// testUpsertEntity is a test helper that wraps Pipe for convenience.
func testUpsertEntity(t *testing.T, w *store.Writer, wikidataID string, mappings []string) {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.UpsertEntity(store.EntityRecord{WikidataID: wikidataID, Mappings: mappings})
	if err := p.Exec(); err != nil {
		t.Fatalf("UpsertEntity(%s): %v", wikidataID, err)
	}
}

// testSetSyncState is a test helper that wraps Pipe for convenience.
func testSetSyncState(t *testing.T, w *store.Writer, key, value string) {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.SetSyncState(key, value)
	if err := p.Exec(); err != nil {
		t.Fatalf("SetSyncState(%s): %v", key, err)
	}
}

func TestLookupSuccess(t *testing.T) {
	srv, w := setupTestServer(t)
	testUpsertEntity(t, w, "Q172241", []string{"P345:tt0111161", "P4947:278", "P4835:2095", "P8013:the-shawshank-redemption"})

	req := httptest.NewRequest("GET", "/v1/lookup/P345/tt0111161", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp lookupResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.WikidataID != "Q172241" {
		t.Errorf("WikidataID = %q, want %q", resp.WikidataID, "Q172241")
	}
	if !slices.Contains(resp.Mappings, "P345:tt0111161") {
		t.Errorf("expected P345:tt0111161 in mappings, got %v", resp.Mappings)
	}
	if !slices.Contains(resp.Mappings, "P4947:278") {
		t.Errorf("expected P4947:278 in mappings, got %v", resp.Mappings)
	}

	cc := rec.Header().Get("Cache-Control")
	if cc != "public, max-age=3600" {
		t.Errorf("Cache-Control = %q, want %q", cc, "public, max-age=3600")
	}

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want %q", ct, "application/json")
	}
}

func TestLookupNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/lookup/P345/tt9999999", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}

	var resp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "not_found" {
		t.Errorf("Error = %q, want %q", resp.Error, "not_found")
	}
}

func TestLookupInvalidProperty(t *testing.T) {
	srv, _ := setupTestServer(t)

	tests := []struct {
		name string
		url  string
	}{
		{"no P prefix", "/v1/lookup/345/tt0111161"},
		{"non-numeric", "/v1/lookup/Pabc/tt0111161"},
		{"zero", "/v1/lookup/P0/tt0111161"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.url, nil)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}

			var resp errorResponse
			if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
				t.Fatalf("decode: %v", err)
			}
			if resp.Error != "invalid_request" {
				t.Errorf("Error = %q, want %q", resp.Error, "invalid_request")
			}
		})
	}
}

func TestWikidataLookupSuccess(t *testing.T) {
	srv, w := setupTestServer(t)
	testUpsertEntity(t, w, "Q172241", []string{"P345:tt0111161", "P4947:278"})

	req := httptest.NewRequest("GET", "/v1/lookup/Q172241", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}

	var resp lookupResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}
	if resp.WikidataID != "Q172241" {
		t.Errorf("WikidataID = %q, want %q", resp.WikidataID, "Q172241")
	}
	if !slices.Contains(resp.Mappings, "P345:tt0111161") {
		t.Errorf("expected P345:tt0111161 in mappings, got %v", resp.Mappings)
	}
}

func TestWikidataLookupNotFound(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/lookup/Q999999", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusNotFound)
	}
}

func TestWikidataLookupInvalid(t *testing.T) {
	srv, _ := setupTestServer(t)

	tests := []struct {
		name string
		url  string
	}{
		{"no Q prefix", "/v1/lookup/172241"},
		{"non-numeric", "/v1/lookup/Qabc"},
		{"zero", "/v1/lookup/Q0"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := httptest.NewRequest("GET", tc.url, nil)
			rec := httptest.NewRecorder()
			srv.Handler().ServeHTTP(rec, req)

			if rec.Code != http.StatusBadRequest {
				t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusBadRequest, rec.Body.String())
			}
		})
	}
}

func TestLookupPropertyDisambiguation(t *testing.T) {
	srv, w := setupTestServer(t)

	// Same TMDB ID "278" for movie (P4947) and TV (P4983)
	testUpsertEntity(t, w, "Q172241", []string{"P4947:278", "P345:tt0111161"})
	testUpsertEntity(t, w, "Q999999", []string{"P4983:278", "P345:tt9999999"})

	req := httptest.NewRequest("GET", "/v1/lookup/P4947/278", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("P4947 status = %d, want %d", rec.Code, http.StatusOK)
	}
	var movieResp lookupResponse
	json.NewDecoder(rec.Body).Decode(&movieResp)
	if movieResp.WikidataID != "Q172241" {
		t.Errorf("P4947 WikidataID = %q, want Q172241", movieResp.WikidataID)
	}

	req = httptest.NewRequest("GET", "/v1/lookup/P4983/278", nil)
	rec = httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("P4983 status = %d, want %d", rec.Code, http.StatusOK)
	}
	var tvResp lookupResponse
	json.NewDecoder(rec.Body).Decode(&tvResp)
	if tvResp.WikidataID != "Q999999" {
		t.Errorf("P4983 WikidataID = %q, want Q999999", tvResp.WikidataID)
	}
}

func TestHealthEndpoint(t *testing.T) {
	srv, w := setupTestServer(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt0000001", "P4947:1"})
	testSetSyncState(t, w, "dump_time", "2026-03-01T00:00:00Z")
	testSetSyncState(t, w, "last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)
	testSetSyncState(t, w, "state", "streaming")

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want %q", resp.Status, "ok")
	}
	if resp.Version != "0.1.0-test" {
		t.Errorf("Version = %q, want %q", resp.Version, "0.1.0-test")
	}
	if resp.DumpTime != "2026-03-01T00:00:00Z" {
		t.Errorf("DumpTime = %q, want %q", resp.DumpTime, "2026-03-01T00:00:00Z")
	}
	if resp.LastEventSync != "2026-03-10T14:22:00Z" {
		t.Errorf("LastEventSync = %q, want %q", resp.LastEventSync, "2026-03-10T14:22:00Z")
	}
	if resp.State != "streaming" {
		t.Errorf("State = %q, want %q", resp.State, "streaming")
	}

	cc := rec.Header().Get("Cache-Control")
	if cc != "no-cache" {
		t.Errorf("Cache-Control = %q, want %q", cc, "no-cache")
	}
}

func TestStatsEndpoint(t *testing.T) {
	srv, w := setupTestServer(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt0000001", "P4947:1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt0000002", "P4947:2"})
	testSetSyncState(t, w, "dump_time", "2026-03-01T00:00:00Z")
	testSetSyncState(t, w, "last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)
	testSetSyncState(t, w, "state", "streaming")

	req := httptest.NewRequest("GET", "/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want %q", resp.Status, "ok")
	}
	if resp.Version != "0.1.0-test" {
		t.Errorf("Version = %q, want %q", resp.Version, "0.1.0-test")
	}
	if resp.DumpTime != "2026-03-01T00:00:00Z" {
		t.Errorf("DumpTime = %q, want %q", resp.DumpTime, "2026-03-01T00:00:00Z")
	}
	if resp.LastEventID != `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]` {
		t.Errorf("LastEventID = %q, want raw event ID", resp.LastEventID)
	}
	if resp.LastEventSync != "2026-03-10T14:22:00Z" {
		t.Errorf("LastEventSync = %q, want %q", resp.LastEventSync, "2026-03-10T14:22:00Z")
	}
	if resp.State != "streaming" {
		t.Errorf("State = %q, want %q", resp.State, "streaming")
	}
	if !resp.SchemaMatch {
		t.Error("SchemaMatch should be true")
	}
	if resp.StreamLength != 2 {
		t.Errorf("StreamLength = %d, want 2", resp.StreamLength)
	}
	if resp.PendingCount != 0 {
		t.Errorf("PendingCount = %d, want 0", resp.PendingCount)
	}
	if resp.ProcessingCount != 0 {
		t.Errorf("ProcessingCount = %d, want 0", resp.ProcessingCount)
	}
	if resp.FailedCount != 0 {
		t.Errorf("FailedCount = %d, want 0", resp.FailedCount)
	}
	if resp.OldestEvent == "" {
		t.Error("OldestEvent should not be empty")
	}
	if resp.NewestEvent == "" {
		t.Error("NewestEvent should not be empty")
	}
}

func TestStatsSharesHealthFields(t *testing.T) {
	srv, w := setupTestServer(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt0000001", "P4947:1"})
	testSetSyncState(t, w, "dump_time", "2026-03-01T00:00:00Z")
	testSetSyncState(t, w, "last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)
	testSetSyncState(t, w, "state", "streaming")

	healthReq := httptest.NewRequest("GET", "/v1/health", nil)
	healthRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(healthRec, healthReq)
	if healthRec.Code != http.StatusOK {
		t.Fatalf("health status = %d, want %d", healthRec.Code, http.StatusOK)
	}

	statsReq := httptest.NewRequest("GET", "/v1/stats", nil)
	statsRec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(statsRec, statsReq)
	if statsRec.Code != http.StatusOK {
		t.Fatalf("stats status = %d, want %d", statsRec.Code, http.StatusOK)
	}

	var health healthResponse
	if err := json.NewDecoder(healthRec.Body).Decode(&health); err != nil {
		t.Fatalf("decode health: %v", err)
	}
	var stats statsResponse
	if err := json.NewDecoder(statsRec.Body).Decode(&stats); err != nil {
		t.Fatalf("decode stats: %v", err)
	}

	if stats.Status != health.Status {
		t.Errorf("Status = %q, want %q", stats.Status, health.Status)
	}
	if stats.Version != health.Version {
		t.Errorf("Version = %q, want %q", stats.Version, health.Version)
	}
	if stats.State != health.State {
		t.Errorf("State = %q, want %q", stats.State, health.State)
	}
	if stats.DumpTime != health.DumpTime {
		t.Errorf("DumpTime = %q, want %q", stats.DumpTime, health.DumpTime)
	}
	if stats.LastEventSync != health.LastEventSync {
		t.Errorf("LastEventSync = %q, want %q", stats.LastEventSync, health.LastEventSync)
	}
	if stats.DatabaseSize != health.DatabaseSize {
		t.Errorf("DatabaseSize = %d, want %d", stats.DatabaseSize, health.DatabaseSize)
	}
	if stats.SchemaMatch != health.SchemaMatch {
		t.Errorf("SchemaMatch = %t, want %t", stats.SchemaMatch, health.SchemaMatch)
	}
}

func TestCORSHeaders(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Errorf("CORS origin = %q, want *", rec.Header().Get("Access-Control-Allow-Origin"))
	}
	if rec.Header().Get("Access-Control-Allow-Methods") != "GET, OPTIONS" {
		t.Errorf("CORS methods = %q", rec.Header().Get("Access-Control-Allow-Methods"))
	}
}

func TestCORSPreflight(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("OPTIONS", "/v1/lookup/P345/tt0111161", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusNoContent {
		t.Errorf("OPTIONS status = %d, want %d", rec.Code, http.StatusNoContent)
	}
	if rec.Header().Get("Access-Control-Allow-Origin") != "*" {
		t.Error("missing CORS headers on preflight")
	}
}

func TestHealthEmptyDB(t *testing.T) {
	srv, _ := setupTestServer(t)

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp healthResponse
	json.NewDecoder(rec.Body).Decode(&resp)
	if resp.Status != "ok" {
		t.Errorf("Status = %q, want %q", resp.Status, "ok")
	}
}

// TestHealthSchemaMatchTrue verifies that the health endpoint reports
// schema_match=true when the schema is up to date.
func TestHealthSchemaMatchTrue(t *testing.T) {
	srv, _ := setupTestServer(t) // setupTestServer calls MigrateSchema

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.SchemaMatch {
		t.Error("expected SchemaMatch=true for healthy server")
	}
}

func TestLookupCaseInsensitiveProperty(t *testing.T) {
	srv, w := setupTestServer(t)
	testUpsertEntity(t, w, "Q172241", []string{"P345:tt0111161"})

	// Lowercase "p" should also work
	req := httptest.NewRequest("GET", "/v1/lookup/p345/tt0111161", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusOK, rec.Body.String())
	}
}

// --- Schema mismatch tests ---

// setupMismatchedServer creates a server whose Reader detects a schema mismatch.
func setupMismatchedServer(t *testing.T) *Server {
	t.Helper()
	s := miniredis.RunT(t)
	c, err := store.NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	w := store.NewWriter(c)
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})

	// Corrupt the schema version so the Reader sees a mismatch
	testSetSyncState(t, w, "schema_version", "wrong-version")

	r := store.NewReader(c)
	return NewServer(r, "0.1.0-test")
}

// TestLookupSchemaMismatch503 verifies that /v1/lookup/P…/… returns 503
// when the schema version doesn't match.
func TestLookupSchemaMismatch503(t *testing.T) {
	srv := setupMismatchedServer(t)

	req := httptest.NewRequest("GET", "/v1/lookup/P345/tt1", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d (503). body: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}

	var resp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "unavailable" {
		t.Errorf("Error = %q, want %q", resp.Error, "unavailable")
	}
}

// TestWikidataLookupSchemaMismatch503 verifies that /v1/lookup/Q… returns 503
// when the schema version doesn't match.
func TestWikidataLookupSchemaMismatch503(t *testing.T) {
	srv := setupMismatchedServer(t)

	req := httptest.NewRequest("GET", "/v1/lookup/Q1", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want %d (503). body: %s", rec.Code, http.StatusServiceUnavailable, rec.Body.String())
	}
}

// TestHealthSchemaMismatch verifies health endpoint reports schema_match=false.
func TestHealthSchemaMismatch(t *testing.T) {
	srv := setupMismatchedServer(t)

	req := httptest.NewRequest("GET", "/v1/health", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp healthResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.SchemaMatch {
		t.Error("expected SchemaMatch=false with corrupted schema version")
	}
}

// TestStatsEntityCount verifies /v1/stats includes entity_count.
func TestStatsEntityCount(t *testing.T) {
	srv, w := setupTestServer(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2", "P4947:100"})

	req := httptest.NewRequest("GET", "/v1/stats", nil)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want %d", rec.Code, http.StatusOK)
	}

	var resp statsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.EntityCount != 2 {
		t.Errorf("EntityCount = %d, want 2", resp.EntityCount)
	}
}

func TestLookupCanceledRequestReturnsGatewayTimeout(t *testing.T) {
	srv, w := setupTestServer(t)
	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req := httptest.NewRequest("GET", "/v1/lookup/P345/tt1", nil).WithContext(ctx)
	rec := httptest.NewRecorder()
	srv.Handler().ServeHTTP(rec, req)

	if rec.Code != http.StatusGatewayTimeout {
		t.Fatalf("status = %d, want %d. body: %s", rec.Code, http.StatusGatewayTimeout, rec.Body.String())
	}

	var resp errorResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Error != "timeout" {
		t.Errorf("Error = %q, want %q", resp.Error, "timeout")
	}
}
