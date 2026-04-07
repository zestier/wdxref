package replica

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
	"github.com/redis/go-redis/v9"

	"github.com/ekeid/ekeid/internal/replicate"
	"github.com/ekeid/ekeid/internal/store"
)

func newTestClient(t *testing.T) (*Client, *miniredis.Miniredis, *redis.Client) {
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

	rdb := redis.NewClient(&redis.Options{Addr: s.Addr(), Protocol: 2})
	t.Cleanup(func() { rdb.Close() })

	client := NewClient(w, rdb, "http://localhost")
	return client, s, rdb
}

// xRangeAll returns all entries from a Redis stream via the go-redis client.
func xRangeAll(t *testing.T, rdb *redis.Client, stream string) []redis.XMessage {
	t.Helper()
	msgs, err := rdb.XRange(context.Background(), stream, "-", "+").Result()
	if err != nil {
		t.Fatalf("XRange(%s): %v", stream, err)
	}
	return msgs
}

// ---------------------------------------------------------------------------
// applySnapshotEntity
// ---------------------------------------------------------------------------

func TestApplySnapshotEntity_Basic(t *testing.T) {
	c, s, rdb := newTestClient(t)
	ctx := context.Background()

	if err := c.applySnapshotEntity(ctx, 42, `["P213:abc123","P214:xyz"]`); err != nil {
		t.Fatalf("applySnapshotEntity: %v", err)
	}

	// Entity should exist in the entities hash.
	mStr := s.HGet("entities", "42")
	if mStr == "" {
		t.Fatal("HGet entities 42: empty")
	}
	var got []string
	if err := json.Unmarshal([]byte(mStr), &got); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(got) != 2 {
		t.Errorf("unexpected mappings: %v", got)
	}

	// Property indexes.
	if v := s.HGet("p:213", "abc123"); v != "42" {
		t.Errorf("p:213 abc123 = %q; want 42", v)
	}
	if v := s.HGet("p:214", "xyz"); v != "42" {
		t.Errorf("p:214 xyz = %q; want 42", v)
	}

	// Snapshot entities now produce changelog entries like normal writes.
	msgs := xRangeAll(t, rdb, "changelog")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 changelog entry for snapshot entity, got %d", len(msgs))
	}
}

func TestApplySnapshotEntity_EmptyMappings(t *testing.T) {
	c, _, _ := newTestClient(t)
	ctx := context.Background()

	if err := c.applySnapshotEntity(ctx, 1, "[]"); err != nil {
		t.Fatalf("applySnapshotEntity: %v", err)
	}
}

// ---------------------------------------------------------------------------
// applyChange — upsert
// ---------------------------------------------------------------------------

func TestApplyChange_UpsertNew(t *testing.T) {
	c, s, rdb := newTestClient(t)
	ctx := context.Background()

	if err := c.applyChange(ctx, 99, `["P213:val1","P213:val2"]`); err != nil {
		t.Fatalf("applyChange: %v", err)
	}

	// Entity in entities hash.
	mStr := s.HGet("entities", "99")
	var got []string
	json.Unmarshal([]byte(mStr), &got)
	if len(got) != 2 {
		t.Errorf("expected 2 vals, got %v", got)
	}

	// Property indexes.
	for _, val := range []string{"val1", "val2"} {
		if v := s.HGet("p:213", val); v != "99" {
			t.Errorf("p:213 %s = %q, want 99", val, v)
		}
	}

	// Counters.
	if ec := s.HGet("meta", "entity_count"); ec != "1" {
		t.Errorf("entity_count = %q, want 1", ec)
	}

	// Changelog.
	msgs := xRangeAll(t, rdb, "changelog")
	if len(msgs) != 1 {
		t.Fatalf("expected 1 changelog entry, got %d", len(msgs))
	}
}

func TestApplyChange_UpsertExisting(t *testing.T) {
	c, s, _ := newTestClient(t)
	ctx := context.Background()

	// Create initial entity.
	if err := c.applyChange(ctx, 50, `["P213:old_val"]`); err != nil {
		t.Fatalf("first upsert: %v", err)
	}

	// Update entity with new mappings.
	if err := c.applyChange(ctx, 50, `["P213:new_val","P214:extra"]`); err != nil {
		t.Fatalf("second upsert: %v", err)
	}

	// Old property index should be removed.
	if v := s.HGet("p:213", "old_val"); v != "" {
		t.Errorf("expected old_val to be removed from p:213, got %q", v)
	}

	// New property indexes should exist.
	if v := s.HGet("p:213", "new_val"); v != "50" {
		t.Errorf("p:213 new_val = %q, want 50", v)
	}
	if v := s.HGet("p:214", "extra"); v != "50" {
		t.Errorf("p:214 extra = %q, want 50", v)
	}

	// entity_count should still be 1 (not re-added).
	if ec := s.HGet("meta", "entity_count"); ec != "1" {
		t.Errorf("entity_count = %q, want 1", ec)
	}
}

// ---------------------------------------------------------------------------
// applyChange — delete
// ---------------------------------------------------------------------------

func TestApplyChange_Delete(t *testing.T) {
	c, s, rdb := newTestClient(t)
	ctx := context.Background()

	// Create entity first.
	if err := c.applyChange(ctx, 77, `["P213:val1","P300:valA","P300:valB"]`); err != nil {
		t.Fatalf("upsert: %v", err)
	}

	// Delete it.
	if err := c.applyChange(ctx, 77, ""); err != nil {
		t.Fatalf("delete: %v", err)
	}

	// Entity should be gone from the entities hash.
	if s.HGet("entities", "77") != "" {
		t.Error("expected entity 77 to be deleted")
	}

	// Property indexes should be cleaned up.
	if v := s.HGet("p:213", "val1"); v != "" {
		t.Errorf("expected val1 removed from p:213, got %q", v)
	}
	if v := s.HGet("p:300", "valA"); v != "" {
		t.Errorf("expected valA removed from p:300, got %q", v)
	}

	// entity_count should be 0.
	if ec := s.HGet("meta", "entity_count"); ec != "0" {
		t.Errorf("entity_count = %q, want 0", ec)
	}

	// Changelog should have upsert + delete entries.
	msgs := xRangeAll(t, rdb, "changelog")
	if len(msgs) != 2 {
		t.Fatalf("expected 2 changelog entries, got %d", len(msgs))
	}
}

func TestApplyChange_DeleteNonexistent(t *testing.T) {
	c, _, _ := newTestClient(t)
	ctx := context.Background()

	if err := c.applyChange(ctx, 999, ""); err != nil {
		t.Fatalf("delete nonexistent should be no-op, got: %v", err)
	}
}

func TestApplyChange_UnknownAction(t *testing.T) {
	c, _, _ := newTestClient(t)
	ctx := context.Background()

	// upsert with empty array should not error.
	if err := c.applyChange(ctx, 1, `[]`); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

// ---------------------------------------------------------------------------
// fetchSnapshot
// ---------------------------------------------------------------------------

func buildZstdSnapshot(t *testing.T, entities []store.SnapshotEntity, streamID string) []byte {
	t.Helper()
	frameSize := len(entities) + 1
	if frameSize <= 0 {
		frameSize = 1
	}
	body, _ := buildMultiFrameZstdSnapshot(t, entities, frameSize, streamID)
	return body
}

func buildZstdSnapshotWithoutDone(t *testing.T, entities []store.SnapshotEntity) []byte {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	var lineBuf []byte
	lineBuf, err = replicate.AppendSnapshotCheckpointLine(lineBuf[:0], 0, 0)
	if err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}
	if _, err := w.Write(lineBuf); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}
	for _, ent := range entities {
		lineBuf = replicate.AppendSnapshotEntityLine(lineBuf[:0], ent.QID, ent.RawMappings)
		if _, err := w.Write(lineBuf); err != nil {
			t.Fatalf("write snapshot line: %v", err)
		}
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes()
}

func TestFetchSnapshot_Success(t *testing.T) {
	c, s, _ := newTestClient(t)
	ctx := context.Background()

	entities := []store.SnapshotEntity{
		{QID: 1, RawMappings: `["P213:a"]`},
		{QID: 2, RawMappings: `["P214:b","P214:c"]`},
	}
	body := buildZstdSnapshot(t, entities, "500-0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/replicate/snapshot" {
			http.Error(w, "not found", 404)
			return
		}
		w.Header().Set("ETag", `"snapshot-test.zst"`)
		w.WriteHeader(200)
		w.Write(body)
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	snapshotID, err := c.fetchSnapshot(ctx)
	if err != nil {
		t.Fatalf("fetchSnapshot: %v", err)
	}
	if snapshotID != "500-0" {
		t.Errorf("snapshotID = %q, want 500-0", snapshotID)
	}

	// Entities should be in the entities hash.
	if s.HGet("entities", "1") == "" {
		t.Error("expected entity 1 to exist")
	}
	if s.HGet("entities", "2") == "" {
		t.Error("expected entity 2 to exist")
	}
	// Meta should have correct state.
	if state := s.HGet("meta", "state"); state != "streaming" {
		t.Errorf("state = %q, want streaming", state)
	}
	if lastID := s.HGet("meta", "last_replicated_id"); lastID != "500-0" {
		t.Errorf("last_replicated_id = %q, want 500-0", lastID)
	}
	if ec := s.HGet("meta", "entity_count"); ec != "2" {
		t.Errorf("entity_count = %q, want 2", ec)
	}
	if sv := s.HGet("meta", "schema_version"); sv != store.SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", sv, store.SchemaVersion())
	}
}

func TestFetchSnapshot_503(t *testing.T) {
	c, _, _ := newTestClient(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(503)
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	_, err := c.fetchSnapshot(ctx)
	if err == nil {
		t.Fatal("expected error on 503")
	}
	if got := err.Error(); got != "upstream not ready (503)" {
		t.Errorf("error = %q", got)
	}
}

func TestFetchSnapshot_MissingDoneControlLine(t *testing.T) {
	c, _, _ := newTestClient(t)
	ctx := context.Background()

	body := buildZstdSnapshotWithoutDone(t, []store.SnapshotEntity{{QID: 1, RawMappings: `[]`}})
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"snapshot-test.zst"`)
		w.WriteHeader(200)
		w.Write(body)
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	_, err := c.fetchSnapshot(ctx)
	if err == nil {
		t.Fatal("expected error for missing done control line")
	}
}

// ---------------------------------------------------------------------------
// connectStream
// ---------------------------------------------------------------------------

func TestConnectStream_UpsertEvents(t *testing.T) {
	c, s, _ := newTestClient(t)

	sseBody := fmt.Sprintf(
		"event: change\ndata: %s\n\nevent: change\ndata: %s\n\n",
		replicate.FormatStreamChangeData("100-0", 10, `{"213":["v1"]}`),
		replicate.FormatStreamChangeData("101-0", 20, `{"214":["v2"]}`),
	)

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.Write([]byte(sseBody))
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := c.connectStream(ctx, "0-0")
	// Stream ends → "stream ended unexpectedly"
	if err == nil {
		t.Fatal("expected error on stream end")
	}

	// Entities should be applied.
	if s.HGet("entities", "10") == "" {
		t.Error("expected entity 10 to exist")
	}
	if s.HGet("entities", "20") == "" {
		t.Error("expected entity 20 to exist")
	}

	// last_replicated_id should be updated to ev2.
	if lastID := s.HGet("meta", "last_replicated_id"); lastID != "101-0" {
		t.Errorf("last_replicated_id = %q, want 101-0", lastID)
	}
}

func TestConnectStream_ResetEvent(t *testing.T) {
	c, s, _ := newTestClient(t)
	ctx := context.Background()

	// Pre-populate some data.
	s.Set("some_key", "some_value")

	sseBody := "event: reset\ndata: {}\n\n"

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(200)
		w.Write([]byte(sseBody))
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	err := c.connectStream(ctx, "0-0")
	if err != nil {
		t.Fatalf("expected nil on reset, got: %v", err)
	}

	// FlushDB should have been called — pre-existing key should be gone.
	if s.Exists("some_key") {
		t.Error("expected some_key to be flushed")
	}
}

func TestConnectStream_NonOKStatus(t *testing.T) {
	c, _, _ := newTestClient(t)
	ctx := context.Background()

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	err := c.connectStream(ctx, "0-0")
	if err == nil {
		t.Fatal("expected error on 500")
	}
}

// ---------------------------------------------------------------------------
// sleepWithContext
// ---------------------------------------------------------------------------

func TestSleepWithContext_CancelledImmediately(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel before sleep

	start := time.Now()
	sleepWithContext(ctx, 10*time.Second)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("sleepWithContext took %v; should return immediately on cancelled ctx", elapsed)
	}
}

func TestSleepWithContext_Timeout(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	start := time.Now()
	sleepWithContext(ctx, 10*time.Second)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("sleepWithContext took %v; should stop after context timeout", elapsed)
	}
}

// ---------------------------------------------------------------------------
// zstd frame resume
// ---------------------------------------------------------------------------

// buildMultiFrameZstdSnapshot creates a zstd compressed snapshot with
// checkpoint control lines at each frame start and a trailing done control
// line. It returns the compressed bytes and the byte offsets where each frame
// starts.
func buildMultiFrameZstdSnapshot(t *testing.T, entities []store.SnapshotEntity, frameSize int, streamID string) ([]byte, []int64) {
	t.Helper()
	var buf bytes.Buffer
	w, err := zstd.NewWriter(&buf)
	if err != nil {
		t.Fatalf("zstd.NewWriter: %v", err)
	}
	offsets := []int64{0} // frame 0 starts at 0
	var lineBuf []byte
	lineBuf, err = replicate.AppendSnapshotCheckpointLine(lineBuf[:0], 0, 0)
	if err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}
	if _, err := w.Write(lineBuf); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	entitiesInFrame := 0
	count := 0
	for _, ent := range entities {
		if entitiesInFrame == frameSize {
			if err := w.Close(); err != nil {
				t.Fatalf("zstd close frame: %v", err)
			}
			offsets = append(offsets, int64(buf.Len()))
			w.Reset(&buf)
			entitiesInFrame = 0
			lineBuf, err = replicate.AppendSnapshotCheckpointLine(lineBuf[:0], offsets[len(offsets)-1], int64(count))
			if err != nil {
				t.Fatalf("append checkpoint: %v", err)
			}
			if _, err := w.Write(lineBuf); err != nil {
				t.Fatalf("write checkpoint: %v", err)
			}
		}

		lineBuf = replicate.AppendSnapshotEntityLine(lineBuf[:0], ent.QID, ent.RawMappings)
		if _, err := w.Write(lineBuf); err != nil {
			t.Fatalf("write snapshot line: %v", err)
		}
		count++
		entitiesInFrame++
	}

	lineBuf, err = replicate.AppendSnapshotDoneLine(lineBuf[:0], streamID, int64(count))
	if err != nil {
		t.Fatalf("append done: %v", err)
	}
	if _, err := w.Write(lineBuf); err != nil {
		t.Fatalf("write done: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("zstd close: %v", err)
	}
	return buf.Bytes(), offsets
}

// TestZstdFrameResumeDecompression proves that decompressing a zstd file from
// an independent frame boundary produces valid snapshot lines for the remaining entities.
func TestZstdFrameResumeDecompression(t *testing.T) {
	frameSize := 3
	entities := make([]store.SnapshotEntity, 12)
	for i := range entities {
		entities[i] = store.SnapshotEntity{
			QID:         int64(i + 1),
			RawMappings: fmt.Sprintf(`["P213:v%d"]`, i),
		}
	}

	fullData, offsets := buildMultiFrameZstdSnapshot(t, entities, frameSize, "500-0")

	// There should be 4 frame starts for 12 entities with 3 entities per frame.
	if len(offsets) < 4 {
		t.Fatalf("expected at least 4 frame offsets, got %d", len(offsets))
	}

	// Test resuming from each frame boundary.
	for frameIdx := 1; frameIdx < len(offsets); frameIdx++ {
		offset := offsets[frameIdx]
		partial := fullData[offset:]

		zr, err := zstd.NewReader(bytes.NewReader(partial))
		if err != nil {
			t.Fatalf("frame %d: zstd.NewReader: %v", frameIdx, err)
		}

		var decoded []store.SnapshotEntity
		var seenCheckpoint bool
		var seenDone bool
		scanner := bufio.NewScanner(zr)
		for scanner.Scan() {
			line := scanner.Bytes()
			switch replicate.ClassifySnapshotLine(line) {
			case replicate.SnapshotLineTypeControl:
				control, err := replicate.ParseSnapshotControl(line)
				if err != nil {
					t.Fatalf("frame %d: parse control line: %v", frameIdx, err)
				}
				switch control.Type {
				case replicate.SnapshotControlTypeCheckpoint:
					checkpoint, err := replicate.ParseSnapshotCheckpoint(line)
					if err != nil {
						t.Fatalf("frame %d: parse checkpoint line: %v", frameIdx, err)
					}
					if !seenCheckpoint {
						if checkpoint.Offset != offset {
							t.Fatalf("frame %d: checkpoint offset = %d, want %d", frameIdx, checkpoint.Offset, offset)
						}
						wantEntitiesBefore := int64(frameIdx * frameSize)
						if checkpoint.EntitiesBefore != wantEntitiesBefore {
							t.Fatalf("frame %d: checkpoint entities_before = %d, want %d", frameIdx, checkpoint.EntitiesBefore, wantEntitiesBefore)
						}
					}
					seenCheckpoint = true
				case replicate.SnapshotControlTypeDone:
					seenDone = true
				default:
					t.Fatalf("frame %d: unexpected control type %q", frameIdx, control.Type)
				}

			case replicate.SnapshotLineTypeEntity:
				qid, rawMappings, err := replicate.ParseSnapshotEntityLine(line)
				if err != nil {
					t.Fatalf("frame %d: parse snapshot line: %v", frameIdx, err)
				}
				decoded = append(decoded, store.SnapshotEntity{QID: qid, RawMappings: rawMappings})

			default:
				t.Fatalf("frame %d: invalid snapshot line", frameIdx)
			}
		}
		if err := scanner.Err(); err != nil {
			t.Fatalf("frame %d: scan error: %v", frameIdx, err)
		}
		zr.Close()
		if !seenCheckpoint {
			t.Fatalf("frame %d: missing checkpoint line", frameIdx)
		}
		if !seenDone {
			t.Fatalf("frame %d: missing done line", frameIdx)
		}

		// Entities from frame[frameIdx] onward should be present.
		expectedStart := frameIdx * frameSize
		expectedCount := len(entities) - expectedStart
		if len(decoded) != expectedCount {
			t.Errorf("frame %d: got %d entities, want %d", frameIdx, len(decoded), expectedCount)
			continue
		}
		for i, ent := range decoded {
			wantQID := int64(expectedStart + i + 1)
			if ent.QID != wantQID {
				t.Errorf("frame %d entity %d: QID=%d, want %d", frameIdx, i, ent.QID, wantQID)
			}
		}
	}
}

// TestFetchSnapshot_ResumeFromFrame tests the full fetchSnapshot resume flow:
// progress state is pre-set, and the server honors the frame-based resume.
func TestFetchSnapshot_ResumeFromFrame(t *testing.T) {
	c, s, _ := newTestClient(t)
	ctx := context.Background()

	frameSize := 3
	entities := make([]store.SnapshotEntity, 9) // 3 frames
	for i := range entities {
		entities[i] = store.SnapshotEntity{
			QID:         int64(i + 1),
			RawMappings: fmt.Sprintf(`["P213:v%d"]`, i),
		}
	}

	fullData, offsets := buildMultiFrameZstdSnapshot(t, entities, frameSize, "500-0")

	// Pre-apply the first frame (entities 1-3) to simulate partial progress.
	for i := 0; i < frameSize; i++ {
		s.HSet("entities", fmt.Sprintf("%d", i+1), fmt.Sprintf(`["P213:v%d"]`, i))
		s.HSet("p:213", fmt.Sprintf("v%d", i), fmt.Sprintf("%d", i+1))
	}
	s.HSet("meta", "entity_count", fmt.Sprintf("%d", frameSize))

	// Set progress state as if a previous download applied 1 frame.
	s.HSet("meta", "snapshot_etag", `"snapshot-test.zst"`)
	s.HSet("meta", "snapshot_resume_offset", fmt.Sprintf("%d", offsets[1]))
	s.HSet("meta", "snapshot_resume_entities", fmt.Sprintf("%d", frameSize))
	s.HSet("meta", "schema_version", store.SchemaVersion())

	var gotIfRange, gotRange string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotIfRange = r.Header.Get("If-Range")
		gotRange = r.Header.Get("Range")
		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set("Content-Encoding", "identity")
		w.Header().Set("ETag", `"snapshot-test.zst"`)
		http.ServeContent(w, r, "snapshot-test.zst", time.Unix(0, 0), bytes.NewReader(fullData))
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	snapshotID, err := c.fetchSnapshot(ctx)
	if err != nil {
		t.Fatalf("fetchSnapshot: %v", err)
	}
	if snapshotID != "500-0" {
		t.Errorf("snapshotID = %q, want 500-0", snapshotID)
	}

	// Verify resume headers were sent.
	if gotIfRange != `"snapshot-test.zst"` {
		t.Errorf("If-Range = %q, want %q", gotIfRange, `"snapshot-test.zst"`)
	}
	wantRange := fmt.Sprintf("bytes=%d-", offsets[1])
	if gotRange != wantRange {
		t.Errorf("Range = %q, want %q", gotRange, wantRange)
	}

	// All 9 entities should exist.
	for i := 1; i <= 9; i++ {
		eid := fmt.Sprintf("%d", i)
		if s.HGet("entities", eid) == "" {
			t.Errorf("expected entity %s to exist", eid)
		}
	}

	// Meta should have correct final state.
	if state := s.HGet("meta", "state"); state != "streaming" {
		t.Errorf("state = %q, want streaming", state)
	}
	if ec := s.HGet("meta", "entity_count"); ec != "9" {
		t.Errorf("entity_count = %q, want 9", ec)
	}
}

// TestFetchSnapshot_ResumeSchemaVersionSurvivesRestart verifies that
// schema_version is set immediately after FlushDB so MigrateSchema
// won't wipe progress on restart.
func TestFetchSnapshot_ResumeSchemaVersionSurvivesRestart(t *testing.T) {
	c, s, _ := newTestClient(t)
	ctx := context.Background()

	entities := []store.SnapshotEntity{
		{QID: 1, RawMappings: `["P213:a"]`},
	}
	body := buildZstdSnapshot(t, entities, "600-0")

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("ETag", `"snapshot-test.zst"`)
		w.WriteHeader(200)
		w.Write(body)
	}))
	defer srv.Close()

	c.upstreamURL = srv.URL

	if _, err := c.fetchSnapshot(ctx); err != nil {
		t.Fatalf("fetchSnapshot: %v", err)
	}

	// Simulate restart: MigrateSchema should be a no-op since
	// schema_version was set during snapshot apply.
	restartClient, err := store.NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient for restart: %v", err)
	}
	defer restartClient.Close()
	w := store.NewWriter(restartClient)
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// Data should still exist after MigrateSchema.
	if s.HGet("entities", "1") == "" {
		t.Error("expected entity 1 to survive MigrateSchema after snapshot")
	}
	if state := s.HGet("meta", "state"); state != "streaming" {
		t.Errorf("state = %q, want streaming", state)
	}
}
