package replicate

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"
	"github.com/redis/go-redis/v9"

	"github.com/ekeid/ekeid/internal/store"
)

// testSetup creates a miniredis instance, store writer and reader for testing.
func testSetup(t *testing.T) (*store.Writer, *store.Reader, *redis.Client, *miniredis.Miniredis) {
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
	r := store.NewReaderFromWriter(w)
	return w, r, c.Redis(), s
}

// --- compareStreamIDs ---

func TestCompareStreamIDs(t *testing.T) {
	tests := []struct {
		a, b string
		want int
	}{
		{"100-0", "100-0", 0},
		{"100-0", "200-0", -1},
		{"200-0", "100-0", 1},
		{"100-1", "100-2", -1},
		{"100-2", "100-1", 1},
		{"99-0", "100-0", -1},  // shorter ms string → smaller
		{"100-0", "99-0", 1},   // longer ms string → larger
		{"100", "100-0", 0},    // missing seq defaults to "0"
		{"100-0", "100", 0},    // symmetric
		{"1000-0", "999-0", 1}, // 4 chars > 3 chars
	}
	for _, tt := range tests {
		t.Run(tt.a+"_vs_"+tt.b, func(t *testing.T) {
			got := compareStreamIDs(tt.a, tt.b)
			if got != tt.want {
				t.Errorf("compareStreamIDs(%q, %q) = %d, want %d", tt.a, tt.b, got, tt.want)
			}
		})
	}
}

// --- parseStreamIDMs ---

func TestParseStreamIDMs(t *testing.T) {
	tests := []struct {
		id   string
		want int64
	}{
		{"1617000000000-0", 1617000000000},
		{"0-0", 0},
		{"12345-99", 12345},
		{"invalid", 0},
		{"", 0},
	}
	for _, tt := range tests {
		name := tt.id
		if name == "" {
			name = "empty"
		}
		t.Run(name, func(t *testing.T) {
			got := parseStreamIDMs(tt.id)
			if got != tt.want {
				t.Errorf("parseStreamIDMs(%q) = %d, want %d", tt.id, got, tt.want)
			}
		})
	}
}

// --- checkStreamGap ---

func TestCheckStreamGap(t *testing.T) {
	_, reader, rdb, _ := testSetup(t)
	ctx := context.Background()

	// since=0 always requires snapshot, even with no stream.
	if err := checkStreamGap(reader, "0"); err == nil {
		t.Error("expected error for since=0")
	}
	if err := checkStreamGap(reader, "0-0"); err == nil {
		t.Error("expected error for since=0-0")
	}

	// No stream exists yet -> cannot prove coverage, must reset.
	if err := checkStreamGap(reader, "100-0"); err == nil {
		t.Error("expected error for since=100-0 with no stream")
	}

	// Add stream entries.
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "100-0",
		Values: map[string]interface{}{"q": "1", "a": "upsert", "m": "{}"},
	})
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "200-0",
		Values: map[string]interface{}{"q": "2", "a": "upsert", "m": "{}"},
	})

	// since=0 still requires snapshot even with data present.
	if err := checkStreamGap(reader, "0"); err == nil {
		t.Error("expected error for since=0 with data")
	}

	if err := checkStreamGap(reader, "50-0"); err == nil {
		t.Error("expected error for since older than oldest entry")
	}

	if err := checkStreamGap(reader, "100-0"); err != nil {
		t.Errorf("unexpected error for since at oldest entry: %v", err)
	}

	if err := checkStreamGap(reader, "150-0"); err != nil {
		t.Errorf("unexpected error for since within retained range: %v", err)
	}
}

// --- ServeStream ---

func TestServeStream(t *testing.T) {
	_, reader, rdb, _ := testSetup(t)
	ctx := context.Background()

	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "100-0",
		Values: map[string]interface{}{"q": "42", "a": "upsert", "m": `["P345:tt0111161"]`},
	})
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "200-0",
		Values: map[string]interface{}{"q": "43", "a": "upsert", "m": `["P345:tt0111162"]`},
	})

	snap := NewSnapshotGenerator(reader, t.TempDir(), time.Hour)
	handler := Handler(reader, snap)
	server := httptest.NewServer(handler)
	defer server.Close()

	t.Run("reset_on_since_zero", func(t *testing.T) {
		resp, err := http.Get(server.URL + "/replicate/stream?since=0")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "event: reset") {
			t.Errorf("expected reset event, got: %s", body)
		}
	})

	t.Run("reset_when_no_retained_entries", func(t *testing.T) {
		_, reader2, _, _ := testSetup(t)
		snap2 := NewSnapshotGenerator(reader2, t.TempDir(), time.Hour)
		handler2 := Handler(reader2, snap2)
		server2 := httptest.NewServer(handler2)
		defer server2.Close()

		resp, err := http.Get(server2.URL + "/replicate/stream?since=100-0")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		if !strings.Contains(string(body), "event: reset") {
			t.Errorf("expected reset event, got: %s", body)
		}
	})

}

// TestServeStreamEvents tests that actual change events are streamed over SSE.
// Uses a separate miniredis that gets closed after events are sent to unblock
// the handler's blocking XRead.
func TestServeStreamEvents(t *testing.T) {
	ms := miniredis.RunT(t)
	c, err := store.NewClient(ms.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	w := store.NewWriter(c)
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	reader := store.NewReaderFromWriter(w)
	rdb := c.Redis()
	ctx := context.Background()

	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "100-0",
		Values: map[string]interface{}{"q": "42", "a": "upsert", "m": `["P345:tt0111161"]`},
	})
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "200-0",
		Values: map[string]interface{}{"q": "43", "a": "upsert", "m": `["P345:tt0111162"]`},
	})

	snap := NewSnapshotGenerator(reader, t.TempDir(), time.Hour)
	handler := Handler(reader, snap)
	server := httptest.NewServer(handler)
	defer server.Close()

	// Close miniredis after a short delay so the blocking XRead fails
	// and the handler returns after sending the initial events.
	go func() {
		time.Sleep(500 * time.Millisecond)
		ms.Close()
	}()

	resp, err := http.Get(server.URL + "/replicate/stream?since=100-0")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	if ct := resp.Header.Get("Content-Type"); ct != "text/event-stream" {
		t.Errorf("Content-Type = %q, want text/event-stream", ct)
	}

	body, _ := io.ReadAll(resp.Body)
	s := string(body)
	if !strings.Contains(s, "event: change") {
		t.Errorf("expected change event, got: %s", s)
	}
	// Should contain the second entry (after since=100-0).
	if !strings.Contains(s, "tt0111162") {
		t.Errorf("expected second entity data, got: %s", s)
	}
}

// --- SnapshotGenerator.generate ---

func TestSnapshotGeneration(t *testing.T) {
	w, reader, _, _ := testSetup(t)

	// Insert entities via the writer so they appear in the changelog.
	if err := w.UpsertEntity("Q42", []string{"P345:tt0111161"}); err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}
	if err := w.UpsertEntity("Q43", []string{"P345:tt0111162"}); err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}
	if err := w.SetSyncState("state", "streaming"); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	dir := t.TempDir()
	gen := NewSnapshotGenerator(reader, dir, time.Hour)

	if err := gen.generate(context.Background()); err != nil {
		t.Fatalf("generate: %v", err)
	}

	// Verify meta.json.
	metaData, err := os.ReadFile(filepath.Join(dir, "meta.json"))
	if err != nil {
		t.Fatalf("read meta.json: %v", err)
	}
	var meta snapshotMeta
	if err := json.Unmarshal(metaData, &meta); err != nil {
		t.Fatalf("unmarshal meta.json: %v", err)
	}
	if meta.ID == "" {
		t.Error("meta.ID is empty")
	}
	if meta.File == "" {
		t.Error("meta.File is empty")
	}

	// Verify snapshot file is valid zstd containing snapshot lines.
	snapshotPath := filepath.Join(dir, meta.File)
	f, err := os.Open(snapshotPath)
	if err != nil {
		t.Fatalf("open snapshot: %v", err)
	}
	defer f.Close()

	zr, err := zstd.NewReader(f)
	if err != nil {
		t.Fatalf("zstd.NewReader: %v", err)
	}
	defer zr.Close()

	data, err := io.ReadAll(zr)
	if err != nil {
		t.Fatalf("read zstd data: %v", err)
	}

	lines := strings.Split(strings.TrimSpace(string(data)), "\n")
	if len(lines) != 4 {
		t.Fatalf("got %d snapshot lines, want 4; data=%s", len(lines), string(data))
	}

	var entityLines int
	var sawCheckpoint bool
	var sawDone bool
	for i, line := range lines {
		switch ClassifySnapshotLine([]byte(line)) {
		case SnapshotLineTypeControl:
			control, err := ParseSnapshotControl([]byte(line))
			if err != nil {
				t.Fatalf("line %d: parse control: %v", i, err)
			}
			switch control.Type {
			case SnapshotControlTypeCheckpoint:
				checkpoint, err := ParseSnapshotCheckpoint([]byte(line))
				if err != nil {
					t.Fatalf("line %d: parse checkpoint: %v", i, err)
				}
				if !sawCheckpoint {
					if checkpoint.Offset != 0 || checkpoint.EntitiesBefore != 0 {
						t.Fatalf("unexpected initial checkpoint: %+v", checkpoint)
					}
				}
				sawCheckpoint = true
			case SnapshotControlTypeDone:
				done, err := ParseSnapshotDone([]byte(line))
				if err != nil {
					t.Fatalf("line %d: parse done: %v", i, err)
				}
				if done.StreamID != meta.ID || done.Entities != 2 {
					t.Fatalf("unexpected done line: %+v", done)
				}
				sawDone = true
			default:
				t.Fatalf("line %d: unexpected control type %q", i, control.Type)
			}

		case SnapshotLineTypeEntity:
			entityLines++

		default:
			t.Fatalf("line %d: invalid snapshot line %q", i, line)
		}
	}
	if entityLines != 2 {
		t.Fatalf("got %d entity lines, want 2", entityLines)
	}
	if !sawCheckpoint {
		t.Fatal("missing checkpoint line")
	}
	if !sawDone {
		t.Fatal("missing done line")
	}
}

// TestSnapshotGenerationSkipsEmptyChangelog verifies that generate() does not
// produce a snapshot (and therefore never emits a snapshotID of "0-0") when
// the changelog stream is empty. A "0-0" snapshotID causes the replica to loop
// forever: the server correctly rejects since=0-0, the reset clears replica
// state, and the replica immediately fetches the same snapshot again.
func TestSnapshotGenerationSkipsEmptyChangelog(t *testing.T) {
	w, reader, _, _ := testSetup(t)

	// Set state to "streaming" without adding any changelog entries.
	if err := w.SetSyncState("state", "streaming"); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	dir := t.TempDir()
	gen := NewSnapshotGenerator(reader, dir, time.Hour)

	if err := gen.generate(context.Background()); err != nil {
		t.Fatalf("generate returned unexpected error: %v", err)
	}

	// No meta.json should have been written.
	if meta := gen.readMeta(); meta != nil {
		t.Errorf("expected no snapshot to be generated, got meta: %+v", meta)
	}
}

func TestSnapshotCheckpointOffsetsStartFrames(t *testing.T) {
	w, reader, _, _ := testSetup(t)
	ctx := context.Background()

	if err := w.SetSyncState("state", "streaming"); err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	for start := 1; start <= SnapshotFrameSize+1; start += 1000 {
		end := min(start+999, SnapshotFrameSize+1)
		records := make([]store.RawEntityRecord, 0, end-start+1)
		for qid := start; qid <= end; qid++ {
			records = append(records, store.RawEntityRecord{
				WikidataID:  fmt.Sprintf("Q%d", qid),
				RawMappings: fmt.Sprintf(`["P345:tt%07d"]`, qid),
			})
		}
		if err := w.UpsertRawEntitiesBatchContext(ctx, records); err != nil {
			t.Fatalf("UpsertRawEntitiesBatchContext(%d-%d): %v", start, end, err)
		}
	}

	dir := t.TempDir()
	gen := NewSnapshotGenerator(reader, dir, time.Hour)
	if err := gen.generate(ctx); err != nil {
		t.Fatalf("generate: %v", err)
	}

	meta := gen.readMeta()
	if meta == nil {
		t.Fatal("readMeta returned nil")
	}

	fullData, err := os.ReadFile(filepath.Join(dir, meta.File))
	if err != nil {
		t.Fatalf("read snapshot file: %v", err)
	}

	zr, err := zstd.NewReader(bytes.NewReader(fullData))
	if err != nil {
		t.Fatalf("zstd.NewReader(full): %v", err)
	}
	data, err := io.ReadAll(zr)
	zr.Close()
	if err != nil {
		t.Fatalf("read full snapshot: %v", err)
	}

	var checkpointCount int
	var secondCheckpoint SnapshotCheckpoint
	for _, line := range strings.Split(strings.TrimSpace(string(data)), "\n") {
		if ClassifySnapshotLine([]byte(line)) != SnapshotLineTypeControl {
			continue
		}
		control, err := ParseSnapshotControl([]byte(line))
		if err != nil {
			t.Fatalf("ParseSnapshotControl: %v", err)
		}
		if control.Type != SnapshotControlTypeCheckpoint {
			continue
		}
		checkpointCount++
		if checkpointCount == 2 {
			secondCheckpoint, err = ParseSnapshotCheckpoint([]byte(line))
			if err != nil {
				t.Fatalf("ParseSnapshotCheckpoint: %v", err)
			}
			break
		}
	}
	if checkpointCount < 2 {
		t.Fatalf("expected at least 2 checkpoints, got %d", checkpointCount)
	}
	if secondCheckpoint.EntitiesBefore != int64(SnapshotFrameSize) {
		t.Fatalf("second checkpoint entities_before = %d, want %d", secondCheckpoint.EntitiesBefore, SnapshotFrameSize)
	}

	partial := fullData[secondCheckpoint.Offset:]
	zr, err = zstd.NewReader(bytes.NewReader(partial))
	if err != nil {
		t.Fatalf("zstd.NewReader(partial): %v", err)
	}
	partialData, err := io.ReadAll(zr)
	zr.Close()
	if err != nil {
		t.Fatalf("read partial snapshot: %v", err)
	}

	firstLine := strings.SplitN(strings.TrimSpace(string(partialData)), "\n", 2)[0]
	checkpoint, err := ParseSnapshotCheckpoint([]byte(firstLine))
	if err != nil {
		t.Fatalf("ParseSnapshotCheckpoint(first partial line): %v", err)
	}
	if checkpoint.Offset != secondCheckpoint.Offset {
		t.Fatalf("partial checkpoint offset = %d, want %d", checkpoint.Offset, secondCheckpoint.Offset)
	}
	if checkpoint.EntitiesBefore != secondCheckpoint.EntitiesBefore {
		t.Fatalf("partial checkpoint entities_before = %d, want %d", checkpoint.EntitiesBefore, secondCheckpoint.EntitiesBefore)
	}
}

// --- ServeSnapshot ---

// writeTestSnapshot creates a zstd snapshot file in dir with known content
// and writes a matching meta.json. Returns the publication metadata and frame
// start offsets.
func writeTestSnapshot(t *testing.T, dir string, entities []store.SnapshotEntity, streamID string) (snapshotMeta, []int64) {
	t.Helper()

	snapshotName := "snapshot-test.zst"
	snapshotPath := filepath.Join(dir, snapshotName)

	f, err := os.Create(snapshotPath)
	if err != nil {
		t.Fatalf("create snapshot: %v", err)
	}

	cw := &countingWriter{w: f}
	zw, err := zstd.NewWriter(cw)
	if err != nil {
		t.Fatalf("zstd writer: %v", err)
	}

	offsets := []int64{0}
	var lineBuf []byte
	lineBuf, err = AppendSnapshotCheckpointLine(lineBuf[:0], 0, 0)
	if err != nil {
		t.Fatalf("append checkpoint: %v", err)
	}
	if _, err := zw.Write(lineBuf); err != nil {
		t.Fatalf("write checkpoint: %v", err)
	}

	count := 0
	entitiesInFrame := 0
	for _, ent := range entities {
		if entitiesInFrame == 2 {
			if err := zw.Close(); err != nil {
				t.Fatalf("close zstd frame: %v", err)
			}
			offsets = append(offsets, cw.n)
			zw.Reset(cw)
			entitiesInFrame = 0
			lineBuf, err = AppendSnapshotCheckpointLine(lineBuf[:0], cw.n, int64(count))
			if err != nil {
				t.Fatalf("append checkpoint: %v", err)
			}
			if _, err := zw.Write(lineBuf); err != nil {
				t.Fatalf("write checkpoint: %v", err)
			}
		}

		lineBuf = AppendSnapshotEntityLine(lineBuf[:0], ent.QID, ent.RawMappings)
		if _, err := zw.Write(lineBuf); err != nil {
			t.Fatalf("write snapshot line: %v", err)
		}
		count++
		entitiesInFrame++
	}
	lineBuf, err = AppendSnapshotDoneLine(lineBuf[:0], streamID, int64(count))
	if err != nil {
		t.Fatalf("append done: %v", err)
	}
	if _, err := zw.Write(lineBuf); err != nil {
		t.Fatalf("write done: %v", err)
	}
	zw.Close()
	f.Close()

	meta := snapshotMeta{
		File: snapshotName,
		ID:   streamID,
	}
	metaData, _ := json.Marshal(meta)
	os.WriteFile(filepath.Join(dir, "meta.json"), metaData, 0644)
	return meta, offsets
}

func TestServeSnapshot(t *testing.T) {
	_, reader, _, _ := testSetup(t)
	dir := t.TempDir()

	entities := []store.SnapshotEntity{
		{QID: 42, RawMappings: `["P345:tt0111161"]`},
		{QID: 43, RawMappings: `["P345:tt0111162"]`},
	}
	meta, _ := writeTestSnapshot(t, dir, entities, "500-0")

	gen := NewSnapshotGenerator(reader, dir, time.Hour)

	t.Run("serves_snapshot", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/replicate/snapshot", nil)
		w := httptest.NewRecorder()
		gen.ServeSnapshot(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}
		if ct := resp.Header.Get("Content-Type"); ct != "application/zstd" {
			t.Errorf("Content-Type = %q, want application/zstd", ct)
		}
		if etag := resp.Header.Get("ETag"); etag != `"`+meta.File+`"` {
			t.Errorf("ETag = %q, want %q", etag, `"`+meta.File+`"`)
		}

		// Verify body is valid zstd.
		body := resp.Body
		zr, err := zstd.NewReader(body)
		if err != nil {
			t.Fatalf("zstd reader: %v", err)
		}
		defer zr.Close()
		data, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read zstd: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		var entityLines int
		for _, line := range lines {
			if ClassifySnapshotLine([]byte(line)) == SnapshotLineTypeEntity {
				entityLines++
			}
		}
		if entityLines != 2 {
			t.Errorf("got %d entity lines, want 2", entityLines)
		}
	})

	t.Run("503_when_no_snapshot", func(t *testing.T) {
		emptyDir := t.TempDir()
		emptyGen := NewSnapshotGenerator(reader, emptyDir, time.Hour)

		req := httptest.NewRequest("GET", "/replicate/snapshot", nil)
		w := httptest.NewRecorder()
		emptyGen.ServeSnapshot(w, req)

		if w.Code != http.StatusServiceUnavailable {
			t.Errorf("status = %d, want 503", w.Code)
		}
	})
}

// --- ServeSnapshot resume ---

func TestServeSnapshotResume(t *testing.T) {
	_, reader, _, _ := testSetup(t)
	dir := t.TempDir()

	// Create snapshot with 4 entities (frames at offset 0 and after entity 2).
	entities := []store.SnapshotEntity{
		{QID: 1, RawMappings: `["P345:a"]`},
		{QID: 2, RawMappings: `["P345:b"]`},
		{QID: 3, RawMappings: `["P345:c"]`},
		{QID: 4, RawMappings: `["P345:d"]`},
	}
	meta, offsets := writeTestSnapshot(t, dir, entities, "500-0")
	gen := NewSnapshotGenerator(reader, dir, time.Hour)

	t.Run("resume_with_matching_etag", func(t *testing.T) {
		if len(offsets) < 2 {
			t.Skip("not enough frames for resume test")
		}

		// Get the full snapshot for byte comparison.
		fullReq := httptest.NewRequest("GET", "/replicate/snapshot", nil)
		fullW := httptest.NewRecorder()
		gen.ServeSnapshot(fullW, fullReq)
		fullBody := fullW.Body.Bytes()

		// Request with standard range resume headers.
		req := httptest.NewRequest("GET", "/replicate/snapshot", nil)
		req.Header.Set("If-Range", `"`+meta.File+`"`)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offsets[1]))
		w := httptest.NewRecorder()
		gen.ServeSnapshot(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusPartialContent {
			t.Fatalf("status = %d, want 206", resp.StatusCode)
		}

		// Resumed body should match the tail of the full file from the frame offset.
		resumedBody := w.Body.Bytes()
		offset := offsets[1]
		expectedSuffix := fullBody[offset:]
		if !bytes.Equal(resumedBody, expectedSuffix) {
			t.Errorf("resumed body (%d bytes) doesn't match full body from offset %d (%d bytes)",
				len(resumedBody), offset, len(expectedSuffix))
		}
	})

	t.Run("etag_mismatch_serves_full", func(t *testing.T) {
		req := httptest.NewRequest("GET", "/replicate/snapshot", nil)
		req.Header.Set("If-Range", `"wrong-etag"`)
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", offsets[1]))
		w := httptest.NewRecorder()
		gen.ServeSnapshot(w, req)

		resp := w.Result()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("status = %d, want 200", resp.StatusCode)
		}

		// Body should contain all entities.
		zr, err := zstd.NewReader(resp.Body)
		if err != nil {
			t.Fatalf("zstd reader: %v", err)
		}
		defer zr.Close()
		data, err := io.ReadAll(zr)
		if err != nil {
			t.Fatalf("read zstd: %v", err)
		}
		lines := strings.Split(strings.TrimSpace(string(data)), "\n")
		entityLines := 0
		for _, line := range lines {
			if ClassifySnapshotLine([]byte(line)) == SnapshotLineTypeEntity {
				entityLines++
			}
		}
		if entityLines != 4 {
			t.Errorf("got %d entity lines, want 4 (full file)", entityLines)
		}
	})
}

// --- serveHealth ---

func TestServeHealth(t *testing.T) {
	_, reader, rdb, _ := testSetup(t)
	ctx := context.Background()

	// Set up state metadata.
	rdb.HSet(ctx, "meta", "state", "streaming", "entity_count", "100")

	// Add stream entries.
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "100-0",
		Values: map[string]interface{}{"q": "1", "a": "upsert", "m": "{}"},
	})
	rdb.XAdd(ctx, &redis.XAddArgs{
		Stream: "changelog",
		ID:     "200-0",
		Values: map[string]interface{}{"q": "2", "a": "upsert", "m": "{}"},
	})

	// Create a snapshot meta so health reports snapshot info.
	dir := t.TempDir()
	snapshotMeta := snapshotMeta{
		File: "snapshot-test.zst",
		ID:   "200-0",
	}
	metaData, _ := json.Marshal(snapshotMeta)
	os.WriteFile(filepath.Join(dir, "meta.json"), metaData, 0644)

	gen := NewSnapshotGenerator(reader, dir, time.Hour)

	w := httptest.NewRecorder()
	serveHealth(reader, gen, w)

	resp := w.Result()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status = %d, want 200", resp.StatusCode)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}

	var health healthResponse
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if health.State != "streaming" {
		t.Errorf("State = %q, want streaming", health.State)
	}
	if health.StreamLen != 2 {
		t.Errorf("StreamLen = %d, want 2", health.StreamLen)
	}
	// Note: miniredis XINFO STREAM does not populate FirstEntry/LastEntry IDs,
	// so FirstID and LastID are empty in this test environment.
	if health.SnapshotID != "200-0" {
		t.Errorf("SnapshotID = %q, want 200-0", health.SnapshotID)
	}
	if health.EntityCount != 100 {
		t.Errorf("EntityCount = %d, want 100", health.EntityCount)
	}
}
