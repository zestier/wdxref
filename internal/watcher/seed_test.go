package watcher

import (
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os/exec"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

func newTestStoreWriter(t *testing.T) *store.Writer {
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
	return w
}

func withDumpWriteRetryDelay(t *testing.T, fn func(int) time.Duration) {
	t.Helper()
	old := dumpWriteRetryDelay
	dumpWriteRetryDelay = fn
	t.Cleanup(func() {
		dumpWriteRetryDelay = old
	})
}

func withDumpResumeRetryDelay(t *testing.T, fn func(int) time.Duration) {
	t.Helper()
	old := dumpResumeRetryDelay
	dumpResumeRetryDelay = fn
	t.Cleanup(func() {
		dumpResumeRetryDelay = old
	})
}

func withStaticDumpLocator(seeder *Seeder, url, date string) {
	seeder.dumpLocator = func(context.Context) (string, string, bool) {
		return url, date, false
	}
}

// buildDumpEntityJSON constructs a Wikidata dump-format entity JSON line.
// This is the bare entity format (no {"entities":{...}} wrapper).
// Each property is given datatype "external-id" so extractExternalIDs picks it up.
func buildDumpEntityJSON(qid, label string, properties map[string]string) []byte {
	claims := make(map[string]interface{})
	for propID, value := range properties {
		claims[propID] = []map[string]interface{}{
			{
				"mainsnak": map[string]interface{}{
					"snaktype": "value",
					"property": propID,
					"datatype": "external-id",
					"datavalue": map[string]interface{}{
						"value": value,
						"type":  "string",
					},
				},
			},
		}
	}

	entity := map[string]interface{}{
		"type": "item",
		"id":   qid,
		"labels": map[string]interface{}{
			"en": map[string]string{
				"language": "en",
				"value":    label,
			},
		},
		"claims":   claims,
		"modified": "2026-03-09T00:00:00Z",
	}

	data, _ := json.Marshal(entity)
	return data
}

func TestParseEntityJSON_DumpMovie(t *testing.T) {
	data := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected non-nil entity")
	}
	if entity.ID != "Q172241" {
		t.Errorf("ID = %q, want Q172241", entity.ID)
	}
	if !slices.Contains(entity.Mappings, "P345:tt0111161") {
		t.Errorf("expected P345:tt0111161 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P4947:278") {
		t.Errorf("expected P4947:278 in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_DumpTVSeries(t *testing.T) {
	data := buildDumpEntityJSON("Q1396", "Breaking Bad", map[string]string{
		"P345":  "tt0903747",
		"P4983": "1396",
		"P4835": "81189",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity.ID != "Q1396" {
		t.Errorf("ID = %q, want Q1396", entity.ID)
	}
	if !slices.Contains(entity.Mappings, "P345:tt0903747") {
		t.Errorf("expected P345:tt0903747 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P4983:1396") {
		t.Errorf("expected P4983:1396 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P4835:81189") {
		t.Errorf("expected P4835:81189 in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_DumpNoExternalIDs(t *testing.T) {
	// Q-entity with no external-id claims should return entity with empty mappings.
	entity := map[string]interface{}{
		"type": "item",
		"id":   "Q42",
		"labels": map[string]interface{}{
			"en": map[string]string{"language": "en", "value": "Douglas Adams"},
		},
		"claims": map[string]interface{}{
			"P31": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"snaktype": "value",
						"property": "P31",
						"datatype": "wikibase-item",
						"datavalue": map[string]interface{}{
							"value": map[string]string{"id": "Q5"},
							"type":  "wikibase-entityid",
						},
					},
				},
			},
		},
	}
	data, _ := json.Marshal(entity)

	result, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if result == nil {
		t.Fatal("expected non-nil entity with empty mappings, got nil")
	}
	if result.ID != "Q42" {
		t.Errorf("ID = %q, want Q42", result.ID)
	}
	if len(result.Mappings) != 0 {
		t.Errorf("expected empty mappings, got %v", result.Mappings)
	}
	if result.Mappings == nil {
		t.Error("Mappings should be non-nil empty slice, got nil")
	}
}

func TestParseEntityJSON_DumpInvalidJSON(t *testing.T) {
	_, err := ParseEntityJSON([]byte(`{invalid json`))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

// TestConfigFingerprintDeterministic verifies configFingerprint is stable.
func TestConfigFingerprintDeterministic(t *testing.T) {
	h1 := configFingerprint()
	h2 := configFingerprint()
	if h1 != h2 {
		t.Errorf("configFingerprint not deterministic: %q != %q", h1, h2)
	}
	if len(h1) == 0 {
		t.Error("configFingerprint returned empty string")
	}
}

// TestConfigFingerprintDiffersFromSchemaVersion verifies that config_hash and
// schema_version are independent (different strings).
func TestConfigFingerprintDiffersFromSchemaVersion(t *testing.T) {
	cfgHash := configFingerprint()
	schemaVersion := store.SchemaVersion()
	if cfgHash == schemaVersion {
		t.Errorf("configFingerprint = SchemaVersion = %q; they should be independent", cfgHash)
	}
}

func compressBZ2(t *testing.T, data []byte) []byte {
	// bzip2 package in stdlib is decompress-only, so we use the gzip format
	// for tests that need compression. For bz2-specific tests, we use
	// a real bzip2 stream via exec if available, otherwise skip.
	t.Helper()
	// Try using external bzip2 command
	cmd := exec.Command("bzip2")
	cmd.Stdin = bytes.NewReader(data)
	var out bytes.Buffer
	cmd.Stdout = &out
	if err := cmd.Run(); err != nil {
		t.Skipf("bzip2 command not available: %v", err)
	}
	return out.Bytes()
}

func compressGZ(t *testing.T, data []byte) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := gzip.NewWriter(&buf)
	if _, err := w.Write(data); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	return buf.Bytes()
}

func TestProcessDumpStream(t *testing.T) {
	writer := newTestStoreWriter(t)

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})
	tv := buildDumpEntityJSON("Q1396", "Breaking Bad", map[string]string{
		"P345":  "tt0903747",
		"P4983": "1396",
		"P4835": "81189",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString(",\n")
	dumpData.Write(tv)
	dumpData.WriteString("\n]\n")

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	imported, lines, err := seeder.processDumpStream(context.Background(), &dumpData, 0, nil)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 2 {
		t.Errorf("imported = %d, want 2", imported)
	}
	if lines < 4 {
		t.Errorf("lines = %d, want >= 4", lines)
	}

	reader := store.NewReaderFromWriter(writer)

	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected movie to be found")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}

	result, err = reader.LookupByProperty(345, "tt0903747")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected TV series to be found")
	}
	if result.WikidataID != 1396 {
		t.Errorf("WikidataID = %d, want 1396", result.WikidataID)
	}
}

func TestProcessDumpStreamBZ2(t *testing.T) {
	writer := newTestStoreWriter(t)

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	compressed := compressBZ2(t, dumpData.Bytes())
	decompressed := bzip2.NewReader(bytes.NewReader(compressed))

	seeder := NewSeeder(writer, nil, DumpFormatBZ2)
	imported, _, err := seeder.processDumpStream(context.Background(), decompressed, 0, nil)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 1 {
		t.Errorf("imported = %d, want 1", imported)
	}
}

func TestProcessDumpStreamGZ(t *testing.T) {
	writer := newTestStoreWriter(t)

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	compressed := compressGZ(t, dumpData.Bytes())
	reader, err := gzip.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	imported, _, err := seeder.processDumpStream(context.Background(), reader, 0, nil)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 1 {
		t.Errorf("imported = %d, want 1", imported)
	}
}

func TestProcessDumpStream_ReturnsBackgroundWriterError(t *testing.T) {
	withDumpWriteRetryDelay(t, func(int) time.Duration { return time.Millisecond })

	s := miniredis.RunT(t)
	c, err := store.NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	writer := store.NewWriter(c)
	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// Force the write path to fail after setup but before the seed flush runs.
	s.Close()

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345": "tt0111161",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	_, _, err = seeder.processDumpStream(context.Background(), &dumpData, 0, nil)
	if err == nil {
		t.Fatal("expected background writer error, got nil")
	}
	if !strings.Contains(err.Error(), "background writer:") {
		t.Fatalf("expected background writer error, got: %v", err)
	}
}

func TestProcessDumpStream_RetriesSeedBatchWrite(t *testing.T) {
	var retryCalls atomic.Int32
	withDumpWriteRetryDelay(t, func(int) time.Duration {
		retryCalls.Add(1)
		return time.Millisecond
	})

	s := miniredis.RunT(t)
	c, err := store.NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	writer := store.NewWriter(c)
	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345": "tt0111161",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	// Fail the first flush attempt, then clear the error before the retry.
	s.SetError("transient write failure")
	go func() {
		for retryCalls.Load() == 0 {
			time.Sleep(time.Millisecond)
		}
		s.SetError("")
	}()

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	imported, _, err := seeder.processDumpStream(context.Background(), &dumpData, 0, nil)
	if err != nil {
		t.Fatalf("processDumpStream: %v", err)
	}
	if imported != 1 {
		t.Fatalf("imported = %d, want 1", imported)
	}
	if retryCalls.Load() == 0 {
		t.Fatal("expected at least one retry before seed batch succeeded")
	}

	reader := store.NewReaderFromWriter(writer)
	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected entity to be present after retry succeeded")
	}

	stats, err := reader.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 1 {
		t.Fatalf("EntityCount = %d, want 1", stats.EntityCount)
	}
}

func TestProcessDumpStream_CancelledDuringRetryDelay(t *testing.T) {
	retryStarted := make(chan struct{})
	withDumpWriteRetryDelay(t, func(int) time.Duration {
		select {
		case <-retryStarted:
		default:
			close(retryStarted)
		}
		return time.Second
	})

	s := miniredis.RunT(t)
	c, err := store.NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	writer := store.NewWriter(c)
	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	s.SetError("transient write failure")

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345": "tt0111161",
	})

	var dumpData bytes.Buffer
	dumpData.WriteString("[\n")
	dumpData.Write(movie)
	dumpData.WriteString("\n]\n")

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-retryStarted
		cancel()
	}()

	seeder := NewSeeder(writer, nil, DumpFormatGZ)
	start := time.Now()
	_, _, err = seeder.processDumpStream(ctx, &dumpData, 0, nil)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("processDumpStream took %v after cancellation; expected prompt return", elapsed)
	}
}

func TestSeederSeedWithMockServer(t *testing.T) {
	writer := newTestStoreWriter(t)

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")

	compressed := compressGZ(t, dumpBuf.Bytes())

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", dumpTime.Format(http.TimeFormat))
		w.Write(compressed)
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	withStaticDumpLocator(seeder, server.URL, dumpTime.UTC().Format("20060102"))

	err := seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	reader := store.NewReaderFromWriter(writer)

	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected entity to be found")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}

	// Verify sync timestamps
	state, err := writer.GetSyncState("state")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if state != "seeding" {
		t.Errorf("state = %q, want seeding", state)
	}

	dumpTimeStr, err := writer.GetSyncState("dump_time")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	expectedDumpTime := dumpTime.UTC().Format("2006-01-02") + "T00:00:00Z"
	if dumpTimeStr != expectedDumpTime {
		t.Errorf("dump_time = %q, want %q", dumpTimeStr, expectedDumpTime)
	}

	configHash, err := writer.GetSyncState("config_hash")
	if err != nil {
		t.Fatalf("GetSyncState config_hash: %v", err)
	}
	if configHash != configFingerprint() {
		t.Errorf("config_hash = %q, want %q", configHash, configFingerprint())
	}
}

func TestSeederNeedsSeed(t *testing.T) {
	writer := newTestStoreWriter(t)

	seeder := NewSeeder(writer, nil, "")

	// Empty DB should need seed
	needs, err := seeder.NeedsSeed()
	if err != nil {
		t.Fatalf("NeedsSeed: %v", err)
	}
	if !needs {
		t.Error("empty DB should need seed")
	}

	// Recent sync with current config should not need seed
	writer.SetSyncState("dump_time", "2026-03-11T00:00:00Z")
	writer.SetSyncState("config_hash", configFingerprint())
	needs, err = seeder.NeedsSeed()
	if err != nil {
		t.Fatalf("NeedsSeed: %v", err)
	}
	if needs {
		t.Error("recent sync should not need seed")
	}

	// Stale config hash should trigger reseed
	writer.SetSyncState("config_hash", "stale-hash")
	needs, err = seeder.NeedsSeed()
	if err != nil {
		t.Fatalf("NeedsSeed: %v", err)
	}
	if !needs {
		t.Error("stale config hash should need seed")
	}
}

func TestSeederUsesInjectedLocator(t *testing.T) {
	writer := newTestStoreWriter(t)

	// Build a minimal dump
	movie := buildDumpEntityJSON("Q1", "Test", map[string]string{"P345": "tt0000001"})
	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")
	compressed := compressGZ(t, dumpBuf.Bytes())

	// Create mock server
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Write(compressed)
	}))
	defer server.Close()

	// Create seeder and inject custom locator
	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	locatorCalled := atomic.Bool{}
	seeder.dumpLocator = func(ctx context.Context) (string, string, bool) {
		locatorCalled.Store(true)
		return server.URL, "20260401", false
	}

	// Run Seed
	err := seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// Assert injected locator was actually called (not real discovery)
	if !locatorCalled.Load() {
		t.Error("expected injected dumpLocator to be called, but it wasn't")
	}
}

func TestSeederSeedServerError(t *testing.T) {
	writer := newTestStoreWriter(t)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusServiceUnavailable)
		io.WriteString(w, "service unavailable")
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatBZ2)
	withStaticDumpLocator(seeder, server.URL, "20260310")

	err := seeder.Seed(context.Background())
	if err == nil {
		t.Fatal("expected error for server error response")
	}
	if !strings.Contains(err.Error(), "503") {
		t.Errorf("error should mention status code: %v", err)
	}
}

func TestSeederSeedFlushesExistingData(t *testing.T) {
	writer := newTestStoreWriter(t)

	// Pre-populate an entity
	err := writer.UpsertEntitiesBatch([]store.EntityRecord{{
		WikidataID: "Q999999",
		Mappings:   []string{"P345:tt9999999"},
	}})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	// Build dump with only one entity
	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")

	compressed := compressGZ(t, dumpBuf.Bytes())

	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Last-Modified", dumpTime.Format(http.TimeFormat))
		w.Write(compressed)
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	withStaticDumpLocator(seeder, server.URL, dumpTime.UTC().Format("20060102"))

	err = seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	reader := store.NewReaderFromWriter(writer)

	// The imported entity should exist
	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty imported: %v", err)
	}
	if result == nil {
		t.Fatal("expected imported entity to exist")
	}

	// The pre-existing entity should be gone (flushed)
	stale, err := reader.LookupByProperty(345, "tt9999999")
	if err != nil {
		t.Fatalf("LookupByProperty stale: %v", err)
	}
	if stale != nil {
		t.Error("expected pre-existing entity to be removed after seed flush")
	}
}

// --- Resumable download tests ---

// failingReader returns the first n bytes normally, then returns an error.
type failingReader struct {
	data []byte
	pos  int
	fail int // byte offset at which to fail
}

func (r *failingReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	if r.pos >= r.fail {
		return 0, fmt.Errorf("simulated connection drop")
	}
	n := copy(p, r.data[r.pos:])
	if r.pos+n > r.fail {
		n = r.fail - r.pos
	}
	r.pos += n
	return n, nil
}

func (r *failingReader) Close() error { return nil }

func TestResumableBodyResumesOnDrop(t *testing.T) {
	withDumpResumeRetryDelay(t, func(int) time.Duration { return time.Millisecond })

	data := bytes.Repeat([]byte("abcdefghij"), 100) // 1000 bytes

	var requestCount atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := requestCount.Add(1)
		if r.Header.Get("If-Match") != "" {
			// Resume request — validate ETag
			if r.Header.Get("If-Match") != `"test-etag"` {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
		}
		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			var start int
			fmt.Sscanf(rangeHeader, "bytes=%d-", &start)
			w.Header().Set("ETag", `"test-etag"`)
			w.WriteHeader(http.StatusPartialContent)
			w.Write(data[start:])
			return
		}
		// First request: serve first half then close
		w.Header().Set("ETag", `"test-etag"`)
		if req == 1 {
			w.Write(data[:500])
			// Connection will close, causing an error on client side
			return
		}
		w.Write(data)
	}))
	defer server.Close()

	// Create a resumableBody with a reader that fails at byte 500
	body := io.NopCloser(&failingReader{data: data, fail: 500})
	rb := newResumableBody(context.Background(), body, server.URL, `"test-etag"`, server.Client())

	result, err := io.ReadAll(rb)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("got %d bytes, want %d bytes", len(result), len(data))
	}
}

func TestResumableBodyETagMismatch(t *testing.T) {
	withDumpResumeRetryDelay(t, func(int) time.Duration { return time.Millisecond })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("If-Match") != "" {
			w.WriteHeader(http.StatusPreconditionFailed)
			return
		}
		w.Header().Set("ETag", `"original-etag"`)
		w.Write([]byte("data"))
	}))
	defer server.Close()

	// Create a body that fails immediately
	body := io.NopCloser(&failingReader{data: []byte("data"), fail: 0})
	rb := newResumableBody(context.Background(), body, server.URL, `"original-etag"`, server.Client())

	_, err := io.ReadAll(rb)
	if !errors.Is(err, ErrDumpChanged) {
		t.Errorf("expected ErrDumpChanged, got: %v", err)
	}
}

func TestResumableBodyNoETagFallback(t *testing.T) {
	data := []byte("hello world")
	body := io.NopCloser(bytes.NewReader(data))
	// Empty etag means resumption is disabled
	rb := newResumableBody(context.Background(), body, "http://example.com", "", http.DefaultClient)

	result, err := io.ReadAll(rb)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if !bytes.Equal(result, data) {
		t.Errorf("got %q, want %q", result, data)
	}
}

func TestSeederSeedResumesOnDrop(t *testing.T) {
	withDumpResumeRetryDelay(t, func(int) time.Duration { return time.Millisecond })

	writer := newTestStoreWriter(t)

	movie := buildDumpEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
	})

	var dumpBuf bytes.Buffer
	dumpBuf.WriteString("[\n")
	dumpBuf.Write(movie)
	dumpBuf.WriteString("\n]\n")

	compressed := compressGZ(t, dumpBuf.Bytes())

	var requestCount atomic.Int32
	dumpTime := time.Date(2026, 3, 10, 12, 0, 0, 0, time.UTC)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		req := requestCount.Add(1)
		w.Header().Set("ETag", `"dump-etag-123"`)
		w.Header().Set("Last-Modified", dumpTime.Format(http.TimeFormat))
		w.Header().Set("Content-Type", "application/octet-stream")

		rangeHeader := r.Header.Get("Range")
		if rangeHeader != "" {
			if r.Header.Get("If-Match") != `"dump-etag-123"` {
				w.WriteHeader(http.StatusPreconditionFailed)
				return
			}
			var start int
			fmt.Sscanf(rangeHeader, "bytes=%d-", &start)
			if start >= len(compressed) {
				w.WriteHeader(http.StatusRequestedRangeNotSatisfiable)
				return
			}
			w.WriteHeader(http.StatusPartialContent)
			w.Write(compressed[start:])
			return
		}

		// First request: cut off partway through
		if req == 1 {
			cutoff := len(compressed) / 2
			w.Header().Set("Content-Length", fmt.Sprintf("%d", len(compressed)))
			w.Write(compressed[:cutoff])
			return
		}
		w.Write(compressed)
	}))
	defer server.Close()

	seeder := NewSeeder(writer, server.Client(), DumpFormatGZ)
	withStaticDumpLocator(seeder, server.URL, dumpTime.UTC().Format("20060102"))

	err := seeder.Seed(context.Background())
	if err != nil {
		t.Fatalf("Seed: %v", err)
	}

	// Should have made more than 1 request (initial + resume)
	if requestCount.Load() < 2 {
		t.Errorf("expected at least 2 requests (initial + resume), got %d", requestCount.Load())
	}

	reader := store.NewReaderFromWriter(writer)
	result, err := reader.LookupByProperty(345, "tt0111161")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if result == nil {
		t.Fatal("expected entity to be found")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
}

func TestResumableBody_CancelledDuringRetryDelay(t *testing.T) {
	retryStarted := make(chan struct{})
	withDumpResumeRetryDelay(t, func(int) time.Duration {
		select {
		case <-retryStarted:
		default:
			close(retryStarted)
		}
		return time.Second
	})

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		<-retryStarted
		cancel()
	}()

	rb := newResumableBody(ctx, io.NopCloser(&failingReader{data: []byte("data"), fail: 0}), "http://127.0.0.1:1", `"etag"`, http.DefaultClient)
	start := time.Now()
	_, err := rb.Read(make([]byte, 1))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context canceled, got %v", err)
	}
	if elapsed := time.Since(start); elapsed >= 500*time.Millisecond {
		t.Fatalf("Read took %v after cancellation; expected prompt return", elapsed)
	}
}
