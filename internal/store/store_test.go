package store

import (
	"context"
	"errors"
	"fmt"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/ekeid/ekeid/internal/model"
)

func newTestSetup(t *testing.T) (*Writer, *Reader) {
	t.Helper()
	s := miniredis.RunT(t)
	c, err := NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { c.Close() })

	w := NewWriter(c)
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	r := NewReader(c)
	return w, r
}

func newTestWriter(t *testing.T) *Writer {
	w, _ := newTestSetup(t)
	return w
}

// Test helpers that wrap Pipe for convenience (replacing the removed Writer methods).

func testUpsertEntity(t *testing.T, w *Writer, wikidataID string, mappings []string) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.UpsertEntity(EntityRecord{WikidataID: wikidataID, Mappings: mappings})
	return p.Exec()
}

func testDeleteEntity(t *testing.T, w *Writer, wikidataID string) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.DeleteEntity(wikidataID)
	return p.Exec()
}

func testUpsertEntitiesBatch(t *testing.T, w *Writer, records []EntityRecord) error {
	t.Helper()
	if len(records) == 0 {
		return nil
	}
	p := w.NewPipe(context.Background())
	for _, rec := range records {
		p.UpsertEntity(rec)
	}
	return p.Exec()
}

func testDeleteEntitiesBatch(t *testing.T, w *Writer, qids []string) error {
	t.Helper()
	if len(qids) == 0 {
		return nil
	}
	p := w.NewPipe(context.Background())
	for _, qid := range qids {
		p.DeleteEntity(qid)
	}
	return p.Exec()
}

func testSetSyncState(t *testing.T, w *Writer, key, value string) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.SetSyncState(key, value)
	return p.Exec()
}

func testEnqueueEntities(t *testing.T, w *Writer, qids []string) error {
	t.Helper()
	if len(qids) == 0 {
		return nil
	}
	p := w.NewPipe(context.Background())
	p.EnqueueEntities(qids)
	return p.Exec()
}

func testAckProcessedEntities(t *testing.T, w *Writer, qids []string) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.AckProcessedEntities(qids)
	return p.Exec()
}

func testRecordFailedEntity(t *testing.T, w *Writer, wikidataID, errMsg string) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.RecordFailedEntity(wikidataID, errMsg)
	return p.Exec()
}

func testDeleteFailedEntity(t *testing.T, w *Writer, wikidataID string) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.DeleteFailedEntity(wikidataID)
	return p.Exec()
}

func testClearSyncCursors(t *testing.T, w *Writer) error {
	t.Helper()
	p := w.NewPipe(context.Background())
	p.DelSyncStates("dump_time", "last_event_id")
	return p.Exec()
}

// testLookupFirst is a convenience helper that calls LookupByProperty and
// returns the first result using the old single-result semantics.
// Returns nil when the result slice is empty.
func testLookupFirst(t *testing.T, r *Reader, property int, value string) *model.LookupResult {
	t.Helper()
	results, err := r.LookupByProperty(property, value)
	if err != nil {
		t.Fatalf("LookupByProperty(%d, %q): %v", property, value, err)
	}
	if len(results) == 0 {
		return nil
	}
	return &results[0]
}

func TestWriterCreatesSchema(t *testing.T) {
	_, r := newTestSetup(t)

	val, err := r.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", val, SchemaVersion())
	}
}

func TestUpsertAndLookup(t *testing.T) {
	w, r := newTestSetup(t)

	ids := []string{"P345:tt0111161", "P4835:2095", "P4947:278", "P8013:the-shawshank-redemption"}
	err := testUpsertEntity(t, w, "Q172241", ids)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Lookup by IMDb property
	result := testLookupFirst(t, r, 345, "tt0111161")
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
	if len(result.Mappings) != 4 {
		t.Errorf("len(Mappings) = %d, want 4", len(result.Mappings))
	}
	if !slices.Contains(result.Mappings, "P4947:278") {
		t.Errorf("TMDB movie mapping missing, got %v", result.Mappings)
	}

	// Lookup by TMDB movie property
	result2 := testLookupFirst(t, r, 4947, "278")
	if result2 == nil {
		t.Fatal("expected result for tmdb lookup, got nil")
	}
	if result2.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result2.WikidataID)
	}
}

func TestLookupNotFound(t *testing.T) {
	_, r := newTestSetup(t)

	result := testLookupFirst(t, r, 345, "tt9999999")
	if result != nil {
		t.Errorf("expected nil result, got %+v", result)
	}
}

func TestLookupByPropertyContextCanceled(t *testing.T) {
	w, r := newTestSetup(t)
	if err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1"}); err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := r.LookupByPropertyContext(ctx, 345, "tt1")
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context canceled", err)
	}
}

func TestUpsertUpdatesEntity(t *testing.T) {
	w, r := newTestSetup(t)

	ids1 := []string{"P345:tt0903747", "P4983:1396"}
	err := testUpsertEntity(t, w, "Q1079", ids1)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	ids2 := []string{"P345:tt0903747", "P4983:1396", "P2638:169", "P4835:81189"}
	err = testUpsertEntity(t, w, "Q1079", ids2)
	if err != nil {
		t.Fatalf("UpsertEntity (update): %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt0903747")
	if len(result.Mappings) != 4 {
		t.Errorf("len(Mappings) = %d, want 4", len(result.Mappings))
	}
	if !slices.Contains(result.Mappings, "P2638:169") {
		t.Errorf("TVMaze mapping missing, got %v", result.Mappings)
	}
}

func TestUpsertRemovesOldIDs(t *testing.T) {
	w, r := newTestSetup(t)

	ids1 := []string{"P345:tt0903747", "P2638:169", "P4983:1396"}
	err := testUpsertEntity(t, w, "Q1079", ids1)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	ids2 := []string{"P345:tt0903747", "P4983:1396"}
	err = testUpsertEntity(t, w, "Q1079", ids2)
	if err != nil {
		t.Fatalf("UpsertEntity (update): %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt0903747")
	if len(result.Mappings) != 2 {
		t.Errorf("len(Mappings) = %d, want 2 (tvmaze should be removed)", len(result.Mappings))
	}
	if slices.Contains(result.Mappings, "P2638:169") {
		t.Error("TVMaze mapping should have been removed")
	}

	// Old property index key should also be gone.
	result2 := testLookupFirst(t, r, 2638, "169")
	if result2 != nil {
		t.Error("old property index should be deleted")
	}
}

func TestLookupByWikidataID(t *testing.T) {
	w, r := newTestSetup(t)

	ids := []string{"P345:tt0111161", "P4947:278"}
	testUpsertEntity(t, w, "Q172241", ids)

	result, err := r.LookupByWikidataID(172241)
	if err != nil {
		t.Fatalf("LookupByWikidataID: %v", err)
	}
	if result == nil {
		t.Fatal("expected result, got nil")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
	if len(result.Mappings) != 2 {
		t.Errorf("len(Mappings) = %d, want 2", len(result.Mappings))
	}
}

func TestLookupByWikidataIDNotFound(t *testing.T) {
	_, r := newTestSetup(t)

	result, err := r.LookupByWikidataID(999999)
	if err != nil {
		t.Fatalf("LookupByWikidataID: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil, got %+v", result)
	}
}

func TestDeleteEntity(t *testing.T) {
	w, r := newTestSetup(t)

	ids := []string{"P345:tt0111161", "P4947:278"}
	err := testUpsertEntity(t, w, "Q172241", ids)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	err = testDeleteEntity(t, w, "Q172241")
	if err != nil {
		t.Fatalf("DeleteEntity: %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt0111161")
	if result != nil {
		t.Errorf("expected nil after delete, got %+v", result)
	}
}

func TestDeleteEntityCascadesMappings(t *testing.T) {
	w, r := newTestSetup(t)

	err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1", "P4835:200", "P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt1")
	if result == nil || len(result.Mappings) != 3 {
		t.Fatalf("expected 3 mappings before delete, got %+v", result)
	}

	err = testDeleteEntity(t, w, "Q1")
	if err != nil {
		t.Fatalf("DeleteEntity: %v", err)
	}

	result = testLookupFirst(t, r, 345, "tt1")
	if result != nil {
		t.Errorf("expected mapping to be deleted")
	}

	result, err = r.LookupByWikidataID(1)
	if err != nil {
		t.Fatalf("LookupByWikidataID after delete: %v", err)
	}
	if result != nil {
		t.Error("expected entity to be deleted")
	}
}

func TestDeleteEntitiesBatchCascade(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})
	testUpsertEntity(t, w, "Q3", []string{"P345:tt3"})

	err := testDeleteEntitiesBatch(t, w, []string{"Q1", "Q3"})
	if err != nil {
		t.Fatalf("DeleteEntitiesBatch: %v", err)
	}

	for _, val := range []string{"tt1", "tt3"} {
		result := testLookupFirst(t, r, 345, val)
		if result != nil {
			t.Errorf("expected %s to be deleted", val)
		}
	}

	result := testLookupFirst(t, r, 345, "tt2")
	if result == nil || result.WikidataID != 2 {
		t.Errorf("expected Q2 to survive, got %+v", result)
	}
}

func TestMigrateSchemaFreshDB(t *testing.T) {
	s := miniredis.RunT(t)
	c, err := NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()
	w := NewWriter(c)
	r := NewReaderFromWriter(w)

	// Before MigrateSchema, schema_version is not set.
	val, err := r.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty schema_version before migrate, got %q", val)
	}

	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	val, err = r.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState after migrate: %v", err)
	}
	if val != SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", val, SchemaVersion())
	}
}

func TestMigrateSchemaMatchingVersion(t *testing.T) {
	w, r := newTestSetup(t)

	err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Second MigrateSchema should be a no-op.
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt1")
	if result == nil {
		t.Error("expected data to survive no-op MigrateSchema")
	}
}

func TestMigrateSchemaDropsOnMismatch(t *testing.T) {
	w, _ := newTestSetup(t)

	err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}
	testSetSyncState(t, w, "schema_version", "old-wrong-version")
	testSetSyncState(t, w, "dump_time", "2026-01-01T00:00:00Z")

	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}

	// Data should be gone (FLUSHDB).
	r := &Reader{rdb: w.rdb, schemaOK: true}
	result := testLookupFirst(t, r, 345, "tt1")
	if result != nil {
		t.Error("expected data to be dropped after schema mismatch")
	}

	val, err := r.GetSyncState("dump_time")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "" {
		t.Errorf("expected dump_time to be cleared, got %q", val)
	}

	val, err = r.GetSyncState("schema_version")
	if err != nil {
		t.Fatalf("GetSyncState schema_version: %v", err)
	}
	if val != SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", val, SchemaVersion())
	}
}

func TestReaderSchemaMismatchLookupByProperty(t *testing.T) {
	w, _ := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testSetSyncState(t, w, "schema_version", "wrong-version")

	// Create a new Reader — it should detect the mismatch.
	c := &Client{rdb: w.rdb}
	r := NewReader(c)

	_, err := r.LookupByProperty(345, "tt1")
	if err != ErrSchemaMismatch {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

func TestReaderSchemaMismatchLookupByWikidataID(t *testing.T) {
	w, _ := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testSetSyncState(t, w, "schema_version", "wrong-version")

	c := &Client{rdb: w.rdb}
	r := NewReader(c)

	_, err := r.LookupByWikidataID(1)
	if err != ErrSchemaMismatch {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

func TestReaderSchemaMismatchHealth(t *testing.T) {
	w, r := newTestSetup(t)
	_ = w

	info, err := r.GetHealth()
	if err != nil {
		t.Fatalf("GetHealth: %v", err)
	}
	if !info.SchemaMatch {
		t.Error("expected SchemaMatch=true with correct version")
	}

	testSetSyncState(t, w, "schema_version", "wrong-version")

	c := &Client{rdb: w.rdb}
	r2 := NewReader(c)
	info2, err := r2.GetHealth()
	if err != nil {
		t.Fatalf("GetHealth: %v", err)
	}
	if info2.SchemaMatch {
		t.Error("expected SchemaMatch=false with wrong version")
	}
}

func TestReaderSchemaVersionMissing(t *testing.T) {
	s := miniredis.RunT(t)
	c, err := NewClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	// Don't call MigrateSchema — no schema_version written.
	r := NewReader(c)
	info, err := r.GetHealth()
	if err != nil {
		t.Fatalf("GetHealth: %v", err)
	}
	if info.SchemaMatch {
		t.Error("expected SchemaMatch=false when schema_version is missing")
	}

	_, err = r.LookupByProperty(345, "tt1")
	if err != ErrSchemaMismatch {
		t.Errorf("expected ErrSchemaMismatch, got %v", err)
	}
}

func TestEntityCountInStats(t *testing.T) {
	w, r := newTestSetup(t)

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 0 {
		t.Errorf("initial EntityCount = %d, want 0", stats.EntityCount)
	}

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})
	testUpsertEntity(t, w, "Q3", []string{"P345:tt3", "P4947:100"})

	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 3 {
		t.Errorf("EntityCount = %d, want 3", stats.EntityCount)
	}

	testDeleteEntity(t, w, "Q2")
	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats after delete: %v", err)
	}
	if stats.EntityCount != 2 {
		t.Errorf("EntityCount after delete = %d, want 2", stats.EntityCount)
	}
}

func TestSchemaVersionDeterministic(t *testing.T) {
	h1 := SchemaVersion()
	h2 := SchemaVersion()
	if h1 != h2 {
		t.Errorf("SchemaVersion not deterministic: %q != %q", h1, h2)
	}
	if h1 == "" {
		t.Error("SchemaVersion must not be empty")
	}
}

func TestMigrateSchemaIdempotent(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})

	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("second MigrateSchema: %v", err)
	}
	if err := w.MigrateSchema(); err != nil {
		t.Fatalf("third MigrateSchema: %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt1")
	if result == nil {
		t.Error("data should survive idempotent MigrateSchema calls")
	}
}

func TestDeleteEntitiesBatchEmpty(t *testing.T) {
	w, _ := newTestSetup(t)

	if err := testDeleteEntitiesBatch(t, w, nil); err != nil {
		t.Fatalf("DeleteEntitiesBatch(nil): %v", err)
	}
	if err := testDeleteEntitiesBatch(t, w, []string{}); err != nil {
		t.Fatalf("DeleteEntitiesBatch(empty): %v", err)
	}
}

func TestSyncState(t *testing.T) {
	w, r := newTestSetup(t)

	val, err := r.GetSyncState("last_sync")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "" {
		t.Errorf("expected empty, got %q", val)
	}

	err = testSetSyncState(t, w, "last_sync", "2026-03-10T14:22:00Z")
	if err != nil {
		t.Fatalf("SetSyncState: %v", err)
	}

	val, err = r.GetSyncState("last_sync")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if val != "2026-03-10T14:22:00Z" {
		t.Errorf("got %q, want %q", val, "2026-03-10T14:22:00Z")
	}

	err = testSetSyncState(t, w, "last_sync", "2026-03-11T10:00:00Z")
	if err != nil {
		t.Fatalf("SetSyncState update: %v", err)
	}

	val, err = r.GetSyncState("last_sync")
	if err != nil {
		t.Fatalf("GetSyncState after update: %v", err)
	}
	if val != "2026-03-11T10:00:00Z" {
		t.Errorf("got %q, want %q", val, "2026-03-11T10:00:00Z")
	}
}

func TestGetStats(t *testing.T) {
	w, r := newTestSetup(t)

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}

	testUpsertEntity(t, w, "Q1", []string{"P345:tt0000001", "P4947:1"})
	testUpsertEntity(t, w, "Q2", []string{"P4947:2"})
	testSetSyncState(t, w, "dump_time", "2026-03-01T00:00:00Z")
	testSetSyncState(t, w, "last_event_id", `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`)

	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if !stats.SchemaMatch {
		t.Error("SchemaMatch should be true")
	}
	if stats.LastEventID == "" {
		t.Error("LastEventID should not be empty")
	}
	if stats.DumpTime.IsZero() {
		t.Error("DumpTime should not be zero")
	}
	if stats.LastEventSync.IsZero() {
		t.Error("LastEventSync should not be zero")
	}
	if stats.StreamLength != 2 {
		t.Errorf("StreamLength = %d, want 2", stats.StreamLength)
	}
	if stats.OldestEvent.IsZero() {
		t.Error("OldestEvent should not be zero")
	}
	if stats.NewestEvent.IsZero() {
		t.Error("NewestEvent should not be zero")
	}
	if stats.NewestEvent.Before(stats.OldestEvent) {
		t.Errorf("NewestEvent = %v, should be >= OldestEvent = %v", stats.NewestEvent, stats.OldestEvent)
	}

	testSetSyncState(t, w, "state", "streaming")
	stats, err = r.GetStats()
	if err != nil {
		t.Fatalf("GetStats with state: %v", err)
	}
	if stats.State != "streaming" {
		t.Errorf("State = %q, want %q", stats.State, "streaming")
	}
}

func TestUpsertConflictingExternalID(t *testing.T) {
	w, r := newTestSetup(t)

	err := testUpsertEntity(t, w, "Q100", []string{"P345:tt1234567", "P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity Q100: %v", err)
	}

	// Q200 claims the same IMDb ID — both are stored in the index.
	err = testUpsertEntity(t, w, "Q200", []string{"P345:tt1234567", "P4947:200"})
	if err != nil {
		t.Fatalf("UpsertEntity Q200 should succeed but got: %v", err)
	}

	// Lookup returns both entities, sorted lexicographically by entity ID.
	results, err := r.LookupByProperty(345, "tt1234567")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results, got %d", len(results))
	}
	if results[0].WikidataID != 100 {
		t.Errorf("results[0].WikidataID = %d, want 100", results[0].WikidataID)
	}
	if results[1].WikidataID != 200 {
		t.Errorf("results[1].WikidataID = %d, want 200", results[1].WikidataID)
	}

	// Both entities are still reachable via their unique TMDB IDs.
	result2 := testLookupFirst(t, r, 4947, "100")
	if result2 == nil {
		t.Fatal("Q100 should still exist via TMDB ID")
	}
	if result2.WikidataID != 100 {
		t.Errorf("WikidataID = %d, want 100", result2.WikidataID)
	}

	// Removing Q100's claim on the shared IMDb ID should leave only Q200.
	err = testUpsertEntity(t, w, "Q100", []string{"P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity Q100 update: %v", err)
	}

	results, err = r.LookupByProperty(345, "tt1234567")
	if err != nil {
		t.Fatalf("LookupByProperty after Q100 update: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result after Q100 update, got %d", len(results))
	}
	if results[0].WikidataID != 200 {
		t.Errorf("WikidataID = %d, want 200 (Q100 no longer claims this ID)", results[0].WikidataID)
	}
}

func TestDeleteSharedExternalID(t *testing.T) {
	w, r := newTestSetup(t)

	// Two entities share the same IMDb ID.
	testUpsertEntity(t, w, "Q100", []string{"P345:tt1234567", "P4947:100"})
	testUpsertEntity(t, w, "Q200", []string{"P345:tt1234567", "P4947:200"})

	results, err := r.LookupByProperty(345, "tt1234567")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results before delete, got %d", len(results))
	}

	// Delete Q100 — Q200 must remain reachable via the shared ID.
	if err := testDeleteEntity(t, w, "Q100"); err != nil {
		t.Fatalf("DeleteEntity Q100: %v", err)
	}

	results, err = r.LookupByProperty(345, "tt1234567")
	if err != nil {
		t.Fatalf("LookupByProperty after delete: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result after delete, got %d", len(results))
	}
	if results[0].WikidataID != 200 {
		t.Errorf("WikidataID = %d, want 200", results[0].WikidataID)
	}

	// Q100's unique TMDB ID should also be gone.
	result := testLookupFirst(t, r, 4947, "100")
	if result != nil {
		t.Errorf("expected Q100 TMDB index to be deleted, got %+v", result)
	}

	// Delete Q200 — shared ID index should now be empty.
	if err := testDeleteEntity(t, w, "Q200"); err != nil {
		t.Fatalf("DeleteEntity Q200: %v", err)
	}

	results, err = r.LookupByProperty(345, "tt1234567")
	if err != nil {
		t.Fatalf("LookupByProperty after both deleted: %v", err)
	}
	if len(results) != 0 {
		t.Errorf("expected 0 results after both deleted, got %d", len(results))
	}
}

func TestThreeWaySharedExternalID(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q10", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q20", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q30", []string{"P345:tt1"})

	results, err := r.LookupByProperty(345, "tt1")
	if err != nil {
		t.Fatalf("LookupByProperty: %v", err)
	}
	if len(results) != 3 {
		t.Fatalf("expected 3 results, got %d", len(results))
	}

	// Remove the middle entity.
	if err := testDeleteEntity(t, w, "Q20"); err != nil {
		t.Fatalf("DeleteEntity Q20: %v", err)
	}

	results, err = r.LookupByProperty(345, "tt1")
	if err != nil {
		t.Fatalf("LookupByProperty after delete: %v", err)
	}
	if len(results) != 2 {
		t.Fatalf("expected 2 results after delete, got %d", len(results))
	}
	if results[0].WikidataID != 10 || results[1].WikidataID != 30 {
		t.Errorf("results = [%d, %d], want [10, 30]", results[0].WikidataID, results[1].WikidataID)
	}

	// Update Q10 to drop its claim — only Q30 remains.
	testUpsertEntity(t, w, "Q10", []string{"P4947:999"})

	results, err = r.LookupByProperty(345, "tt1")
	if err != nil {
		t.Fatalf("LookupByProperty after Q10 update: %v", err)
	}
	if len(results) != 1 {
		t.Fatalf("expected 1 result, got %d", len(results))
	}
	if results[0].WikidataID != 30 {
		t.Errorf("WikidataID = %d, want 30", results[0].WikidataID)
	}
}

func TestPropertyDisambiguation(t *testing.T) {
	w, r := newTestSetup(t)

	movieIDs := []string{"P4947:278"}
	err := testUpsertEntity(t, w, "Q172241", movieIDs)
	if err != nil {
		t.Fatalf("UpsertEntity movie: %v", err)
	}

	tvIDs := []string{"P4983:278"}
	err = testUpsertEntity(t, w, "Q999999", tvIDs)
	if err != nil {
		t.Fatalf("UpsertEntity tv: %v", err)
	}

	movieResult := testLookupFirst(t, r, 4947, "278")
	if movieResult.WikidataID != 172241 {
		t.Errorf("movie WikidataID = %d, want 172241", movieResult.WikidataID)
	}

	tvResult := testLookupFirst(t, r, 4983, "278")
	if tvResult.WikidataID != 999999 {
		t.Errorf("tv WikidataID = %d, want 999999", tvResult.WikidataID)
	}
}

func TestUpsertEntitiesBatch(t *testing.T) {
	w, r := newTestSetup(t)

	records := []EntityRecord{
		{WikidataID: "Q172241",
			Mappings: []string{"P345:tt0111161", "P4947:278"}},
		{WikidataID: "Q1079",
			Mappings: []string{"P345:tt0903747", "P4983:1396"}},
		{WikidataID: "Q47740",
			Mappings: []string{"P1733:620", "P5794:72"}},
	}

	if err := testUpsertEntitiesBatch(t, w, records); err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	movie := testLookupFirst(t, r, 345, "tt0111161")
	if movie == nil || movie.WikidataID != 172241 {
		t.Errorf("expected 172241, got %+v", movie)
	}
	if !slices.Contains(movie.Mappings, "P4947:278") {
		t.Errorf("expected P4947:278 in mappings, got %v", movie.Mappings)
	}

	tv := testLookupFirst(t, r, 345, "tt0903747")
	if tv == nil || tv.WikidataID != 1079 {
		t.Errorf("expected 1079, got %+v", tv)
	}

	game := testLookupFirst(t, r, 1733, "620")
	if game == nil || game.WikidataID != 47740 {
		t.Errorf("expected 47740, got %+v", game)
	}
}

func TestUpsertEntitiesBatchEmpty(t *testing.T) {
	w, _ := newTestSetup(t)

	if err := testUpsertEntitiesBatch(t, w, nil); err != nil {
		t.Fatalf("UpsertEntitiesBatch(nil): %v", err)
	}
	if err := testUpsertEntitiesBatch(t, w, []EntityRecord{}); err != nil {
		t.Fatalf("UpsertEntitiesBatch(empty): %v", err)
	}
}

func TestUpsertEntitiesBatchUpdatesExisting(t *testing.T) {
	w, r := newTestSetup(t)

	if err := testUpsertEntity(t, w, "Q172241", []string{"P345:tt0111161"}); err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	if err := testUpsertEntitiesBatch(t, w, []EntityRecord{{
		WikidataID: "Q172241",
		Mappings:   []string{"P345:tt0111161", "P4947:278"},
	}}); err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	result := testLookupFirst(t, r, 345, "tt0111161")
	if len(result.Mappings) != 2 {
		t.Errorf("Mappings = %d, want 2", len(result.Mappings))
	}
}

func TestMultiValuedProperty(t *testing.T) {
	w, r := newTestSetup(t)

	ids := []string{"P345:tt0111161", "P345:tt9999999"}
	err := testUpsertEntity(t, w, "Q172241", ids)
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	result1 := testLookupFirst(t, r, 345, "tt0111161")
	if result1 == nil || result1.WikidataID != 172241 {
		t.Errorf("expected 172241 for first IMDb, got %+v", result1)
	}

	result2 := testLookupFirst(t, r, 345, "tt9999999")
	if result2 == nil || result2.WikidataID != 172241 {
		t.Errorf("expected 172241 for second IMDb, got %+v", result2)
	}
}

func TestPendingQueue(t *testing.T) {
	w, r := newTestSetup(t)

	err := testEnqueueEntities(t, w, []string{"Q1", "Q2", "Q3"})
	if err != nil {
		t.Fatalf("EnqueueEntities: %v", err)
	}

	count, err := r.PendingCount()
	if err != nil {
		t.Fatalf("PendingCount: %v", err)
	}
	if count != 3 {
		t.Errorf("PendingCount = %d, want 3", count)
	}

	batch, err := w.ClaimPendingBatch(2)
	if err != nil {
		t.Fatalf("ClaimPendingBatch: %v", err)
	}
	if len(batch) != 2 {
		t.Errorf("batch len = %d, want 2", len(batch))
	}

	// ClaimPendingBatch moves items out of pending.
	count, _ = r.PendingCount()
	if count != 1 {
		t.Errorf("PendingCount after claim = %d, want 1", count)
	}

	if err := testAckProcessedEntities(t, w, batch); err != nil {
		t.Fatalf("AckProcessedEntities: %v", err)
	}

	// Duplicate insert is idempotent.
	_ = testEnqueueEntities(t, w, []string{"Q1", "Q1", "Q1"})
}

func TestFailedEntities(t *testing.T) {
	w, r := newTestSetup(t)

	if err := testRecordFailedEntity(t, w, "Q1", "timeout"); err != nil {
		t.Fatalf("RecordFailedEntity: %v", err)
	}
	if err := testRecordFailedEntity(t, w, "Q2", "404"); err != nil {
		t.Fatalf("RecordFailedEntity Q2: %v", err)
	}

	failed, err := r.LastFailedEntities(10)
	if err != nil {
		t.Fatalf("LastFailedEntities: %v", err)
	}
	if len(failed) != 2 {
		t.Errorf("failed = %d, want 2", len(failed))
	}

	if err := testDeleteFailedEntity(t, w, "Q1"); err != nil {
		t.Fatalf("DeleteFailedEntity: %v", err)
	}

	failed, _ = r.LastFailedEntities(10)
	if len(failed) != 1 {
		t.Errorf("failed after delete = %d, want 1", len(failed))
	}
}

func TestClearSyncCursors(t *testing.T) {
	w, r := newTestSetup(t)

	testSetSyncState(t, w, "dump_time", "2026-01-01T00:00:00Z")
	testSetSyncState(t, w, "last_event_id", "some-id")

	if err := testClearSyncCursors(t, w); err != nil {
		t.Fatalf("ClearSyncCursors: %v", err)
	}

	dt, _ := r.GetSyncState("dump_time")
	if dt != "" {
		t.Errorf("dump_time should be cleared, got %q", dt)
	}
	eid, _ := r.GetSyncState("last_event_id")
	if eid != "" {
		t.Errorf("last_event_id should be cleared, got %q", eid)
	}

	// Schema version should still be present.
	sv, _ := r.GetSyncState("schema_version")
	if sv != SchemaVersion() {
		t.Errorf("schema_version = %q, want %q", sv, SchemaVersion())
	}
}

// --- UpsertEntitiesBatch additional tests ---

func TestUpsertEntitiesBatchDoesNotDoubleCountExistingEntity(t *testing.T) {
	w, r := newTestSetup(t)

	records := []EntityRecord{{
		WikidataID: "Q1",
		Mappings:   []string{"P345:tt1"},
	}}

	if err := testUpsertEntitiesBatch(t, w, records); err != nil {
		t.Fatalf("first UpsertEntitiesBatch: %v", err)
	}
	if err := testUpsertEntitiesBatch(t, w, records); err != nil {
		t.Fatalf("second UpsertEntitiesBatch: %v", err)
	}

	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 1 {
		t.Errorf("EntityCount = %d, want 1", stats.EntityCount)
	}
}

func TestUpsertEntitiesBatchContextCanceled(t *testing.T) {
	w, _ := newTestSetup(t)

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	p := w.NewPipe(ctx)
	p.UpsertEntity(EntityRecord{
		WikidataID: "Q1",
		Mappings:   []string{"P345:tt1"},
	})
	err := p.Exec()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v, want context canceled", err)
	}
}

// --- Stream/Changelog tests ---

func TestUpsertWritesToChangelog(t *testing.T) {
	w, r := newTestSetup(t)

	err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1", "P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	events, err := r.StreamRead(context.Background(), "0", 10, time.Millisecond)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(events) != 1 {
		t.Fatalf("events = %d, want 1", len(events))
	}
	if events[0].QID != 1 {
		t.Errorf("QID = %d, want 1", events[0].QID)
	}
	if events[0].RawMappings == "" {
		t.Error("expected non-empty RawMappings for upsert")
	}
	if events[0].ID == "" {
		t.Error("expected non-empty event ID")
	}
	if events[0].RawMappings == "" {
		t.Error("expected non-empty RawMappings")
	}
}

func TestDeleteWritesToChangelog(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testDeleteEntity(t, w, "Q1")

	events, err := r.StreamRead(context.Background(), "0", 10, 0)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(events) != 2 {
		t.Fatalf("events = %d, want 2 (upsert + delete)", len(events))
	}
	if events[1].RawMappings != "" {
		t.Errorf("expected empty RawMappings for delete, got %q", events[1].RawMappings)
	}
	if events[1].QID != 1 {
		t.Errorf("QID = %d, want 1", events[1].QID)
	}
}

func TestStreamReadStartsAfterSince(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})
	testUpsertEntity(t, w, "Q3", []string{"P345:tt3"})

	// Read all events first.
	all, err := r.StreamRead(context.Background(), "0", 10, 0)
	if err != nil {
		t.Fatalf("StreamRead all: %v", err)
	}
	if len(all) != 3 {
		t.Fatalf("all events = %d, want 3", len(all))
	}

	// Read starting after the first event.
	after, err := r.StreamRead(context.Background(), all[0].ID, 10, 0)
	if err != nil {
		t.Fatalf("StreamRead after: %v", err)
	}
	if len(after) != 2 {
		t.Errorf("events after first = %d, want 2", len(after))
	}
	if after[0].QID != 2 {
		t.Errorf("first event QID = %d, want 2", after[0].QID)
	}
}

func TestStreamReadEmpty(t *testing.T) {
	_, r := newTestSetup(t)

	// Use short block duration; block=0 means "block forever" in Redis.
	events, err := r.StreamRead(context.Background(), "0", 10, time.Millisecond)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if events != nil {
		t.Errorf("expected nil events on empty stream, got %d", len(events))
	}
}

func TestStreamInfoEmpty(t *testing.T) {
	_, r := newTestSetup(t)

	firstID, lastID, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if firstID != "" || lastID != "" || length != 0 {
		t.Errorf("expected empty stream info, got first=%q last=%q len=%d", firstID, lastID, length)
	}
}

func TestStreamInfoAfterMultipleWrites(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})
	testUpsertEntity(t, w, "Q3", []string{"P345:tt3"})

	// Verify via StreamRead that 3 events exist.
	events, err := r.StreamRead(context.Background(), "0", 10, 0)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(events) != 3 {
		t.Errorf("events = %d, want 3", len(events))
	}
	if events[0].ID == events[2].ID {
		t.Errorf("first and last event IDs should differ")
	}
}

func TestStreamTrim(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})
	testUpsertEntity(t, w, "Q3", []string{"P345:tt3"})

	all, _ := r.StreamRead(context.Background(), "0", 10, 0)
	if len(all) != 3 {
		t.Fatalf("expected 3 events, got %d", len(all))
	}

	// Trim everything before the last event. The XRANGE finds 2 entries
	// before all[2]; XTRIM MINID uses the last one's ID, so that entry
	// survives until the next tick.
	n, err := w.StreamTrimOlderThan(context.Background(), all[2].ID, 100)
	if err != nil {
		t.Fatalf("StreamTrimOlderThan: %v", err)
	}
	if n != 1 {
		t.Errorf("trimmed = %d, want 1", n)
	}

	_, _, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo after trim: %v", err)
	}
	if length != 2 {
		t.Errorf("length after trim = %d, want 2", length)
	}
}

func TestStreamTrimRespectsLimit(t *testing.T) {
	w, r := newTestSetup(t)

	for i := 1; i <= 10; i++ {
		testUpsertEntity(t, w, fmt.Sprintf("Q%d", i), []string{fmt.Sprintf("P345:tt%d", i)})
	}

	all, _ := r.StreamRead(context.Background(), "0", 20, 0)
	if len(all) != 10 {
		t.Fatalf("expected 10 events, got %d", len(all))
	}

	// Trim with limit of 3 — XRANGE returns limit+1 entries, XTRIM MINID
	// uses the last one's ID so exactly 3 are removed.
	n, err := w.StreamTrimOlderThan(context.Background(), all[7].ID, 3)
	if err != nil {
		t.Fatalf("StreamTrimOlderThan: %v", err)
	}
	if n != 3 {
		t.Errorf("trimmed = %d, want 3", n)
	}

	_, _, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if length != 7 {
		t.Errorf("length = %d, want 7", length)
	}
}

func TestStreamTrimNothingToTrim(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})

	all, _ := r.StreamRead(context.Background(), "0", 10, 0)
	if len(all) != 1 {
		t.Fatalf("expected 1 event, got %d", len(all))
	}

	// Trim with the only entry's ID — nothing should be removed.
	n, err := w.StreamTrimOlderThan(context.Background(), all[0].ID, 100)
	if err != nil {
		t.Fatalf("StreamTrimOlderThan: %v", err)
	}
	if n != 0 {
		t.Errorf("trimmed = %d, want 0", n)
	}

	_, _, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if length != 1 {
		t.Errorf("length = %d, want 1", length)
	}
}

func TestStreamTrimEmptyStream(t *testing.T) {
	w, _ := newTestSetup(t)

	n, err := w.StreamTrimOlderThan(context.Background(), "99999999999-0", 100)
	if err != nil {
		t.Fatalf("StreamTrimOlderThan: %v", err)
	}
	if n != 0 {
		t.Errorf("trimmed = %d, want 0", n)
	}
}

// --- ScanEntities tests ---

func TestScanEntitiesBasic(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2", "P4947:100"})
	testUpsertEntity(t, w, "Q3", []string{"P4947:200"})

	var allEntities []SnapshotEntity
	var cursor uint64
	for {
		entities, next, err := r.ScanEntities(context.Background(), cursor, 100)
		if err != nil {
			t.Fatalf("ScanEntities: %v", err)
		}
		allEntities = append(allEntities, entities...)
		cursor = next
		if cursor == 0 {
			break
		}
	}

	if len(allEntities) != 3 {
		t.Errorf("scanned %d entities, want 3", len(allEntities))
	}

	qids := map[int64]bool{}
	for _, e := range allEntities {
		qids[e.QID] = true
	}
	for _, expected := range []int64{1, 2, 3} {
		if !qids[expected] {
			t.Errorf("missing QID %d in scan results", expected)
		}
	}
}

func TestScanEntitiesEmpty(t *testing.T) {
	_, r := newTestSetup(t)

	entities, next, err := r.ScanEntities(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ScanEntities: %v", err)
	}
	if len(entities) != 0 {
		t.Errorf("expected 0 entities, got %d", len(entities))
	}
	if next != 0 {
		t.Errorf("expected cursor 0, got %d", next)
	}
}

func TestScanEntitiesPreservesMappings(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q42", []string{"P345:tt42", "P4947:42", "P4835:420"})

	entities, _, err := r.ScanEntities(context.Background(), 0, 100)
	if err != nil {
		t.Fatalf("ScanEntities: %v", err)
	}
	if len(entities) != 1 {
		t.Fatalf("expected 1 entity, got %d", len(entities))
	}
	if entities[0].QID != 42 {
		t.Errorf("QID = %d, want 42", entities[0].QID)
	}
	if entities[0].RawMappings == "" {
		t.Error("expected non-empty RawMappings")
	}
	if !strings.Contains(entities[0].RawMappings, "tt42") {
		t.Errorf("RawMappings = %q, want to contain tt42", entities[0].RawMappings)
	}
}

// --- NewReaderFromWriter tests ---

func TestNewReaderFromWriter(t *testing.T) {
	w, _ := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})

	r := NewReaderFromWriter(w)
	result := testLookupFirst(t, r, 345, "tt1")
	if result == nil {
		t.Fatal("expected result from reader-from-writer, got nil")
	}
	if result.WikidataID != 1 {
		t.Errorf("WikidataID = %d, want 1", result.WikidataID)
	}
}

// --- Helper function tests ---

func TestEventIDToTime(t *testing.T) {
	tests := []struct {
		name    string
		eventID string
		wantMs  int64 // expected unix milliseconds, 0 = zero time
	}{
		{
			name:    "valid single partition",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1773152520000}]`,
			wantMs:  1773152520000,
		},
		{
			name:    "multiple partitions picks earliest",
			eventID: `[{"topic":"a","partition":0,"timestamp":2000000000000},{"topic":"b","partition":1,"timestamp":1000000000000}]`,
			wantMs:  1000000000000,
		},
		{
			name:    "null timestamp ignored",
			eventID: `[{"topic":"a","partition":0,"timestamp":null},{"topic":"b","partition":1,"timestamp":1500000000000}]`,
			wantMs:  1500000000000,
		},
		{
			name:    "all null timestamps",
			eventID: `[{"topic":"a","partition":0,"timestamp":null}]`,
			wantMs:  0,
		},
		{
			name:    "zero timestamp ignored",
			eventID: `[{"topic":"a","partition":0,"timestamp":0}]`,
			wantMs:  0,
		},
		{
			name:    "negative timestamp ignored",
			eventID: `[{"topic":"a","partition":0,"timestamp":-1}]`,
			wantMs:  0,
		},
		{
			name:    "empty JSON array",
			eventID: `[]`,
			wantMs:  0,
		},
		{
			name:    "invalid JSON",
			eventID: `not json`,
			wantMs:  0,
		},
		{
			name:    "empty string",
			eventID: ``,
			wantMs:  0,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := eventIDToTime(tc.eventID)
			if tc.wantMs == 0 {
				if !got.IsZero() {
					t.Errorf("expected zero time, got %v", got)
				}
			} else {
				want := time.UnixMilli(tc.wantMs)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			}
		})
	}
}

func TestParseInfoDBSize(t *testing.T) {
	tests := []struct {
		name string
		info string
		want int64
	}{
		{
			name: "typical kvrocks output",
			info: "# Keyspace\r\ndb0:keys=0,expires=0\r\nused_db_size:580961786\r\nmax_db_size:0\r\n",
			want: 580961786,
		},
		{
			name: "zero size",
			info: "# Keyspace\r\nused_db_size:0\r\n",
			want: 0,
		},
		{
			name: "no used_db_size line",
			info: "# Keyspace\r\ndb0:keys=0,expires=0\r\n",
			want: 0,
		},
		{
			name: "empty string",
			info: "",
			want: 0,
		},
		{
			name: "large size",
			info: "used_db_size:107374182400\n",
			want: 107374182400,
		},
		{
			name: "unix line endings",
			info: "# Keyspace\ndb0:keys=0\nused_db_size:1024\nmax_db_size:0\n",
			want: 1024,
		},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := parseInfoDBSize(tc.info)
			if got != tc.want {
				t.Errorf("parseInfoDBSize = %d, want %d", got, tc.want)
			}
		})
	}
}

func TestQidToInt(t *testing.T) {
	tests := []struct {
		input   string
		want    int64
		wantErr bool
	}{
		{"Q1", 1, false},
		{"Q172241", 172241, false},
		{"q100", 100, false}, // lowercase q
		{"", 0, true},
		{"1", 0, true},    // no Q prefix
		{"P345", 0, true}, // wrong prefix
		{"Qabc", 0, true}, // non-numeric
		{"Q", 0, true},    // Q with nothing after
		{"Q1.5", 0, true}, // float
		{"Q 1", 0, true},  // space
	}

	for _, tc := range tests {
		t.Run(tc.input, func(t *testing.T) {
			got, err := qidToInt(tc.input)
			if tc.wantErr {
				if err == nil {
					t.Errorf("expected error for %q, got %d", tc.input, got)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for %q: %v", tc.input, err)
				}
				if got != tc.want {
					t.Errorf("qidToInt(%q) = %d, want %d", tc.input, got, tc.want)
				}
			}
		})
	}
}

func TestIntToQid(t *testing.T) {
	tests := []struct {
		input int64
		want  string
	}{
		{1, "Q1"},
		{172241, "Q172241"},
		{0, "Q0"},
	}
	for _, tc := range tests {
		got := intToQid(tc.input)
		if got != tc.want {
			t.Errorf("intToQid(%d) = %q, want %q", tc.input, got, tc.want)
		}
	}
}

// --- Edge case tests ---

func TestDeleteNonExistentEntity(t *testing.T) {
	w, _ := newTestSetup(t)

	// Should not error — skip silently.
	if err := testDeleteEntity(t, w, "Q999999"); err != nil {
		t.Fatalf("DeleteEntity non-existent: %v", err)
	}
}

func TestDeleteNonExistentDoesNotWriteChangelog(t *testing.T) {
	w, r := newTestSetup(t)

	testDeleteEntity(t, w, "Q999999")

	_, _, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if length != 0 {
		t.Errorf("changelog length = %d, want 0 (no-op delete)", length)
	}
}

func TestUpsertIdenticalMappingsDoesNotWriteChangelog(t *testing.T) {
	w, r := newTestSetup(t)

	// Initial upsert should write to changelog.
	err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1", "P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	_, _, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if length != 1 {
		t.Fatalf("changelog length = %d, want 1 after first upsert", length)
	}

	// Second upsert with identical mappings should NOT write to changelog.
	err = testUpsertEntity(t, w, "Q1", []string{"P345:tt1", "P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity (no-op): %v", err)
	}

	_, _, length, err = r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if length != 1 {
		t.Errorf("changelog length = %d, want 1 (no-op upsert should not add entry)", length)
	}
}

func TestUpsertChangedMappingsWritesChangelog(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q1", []string{"P345:tt1", "P4947:100"})

	_, _, length, err := r.StreamInfo()
	if err != nil {
		t.Fatalf("StreamInfo: %v", err)
	}
	if length != 2 {
		t.Errorf("changelog length = %d, want 2 (initial + changed mappings)", length)
	}
}

func TestUpsertEntityWithEmptyMappings(t *testing.T) {
	w, r := newTestSetup(t)

	err := testUpsertEntity(t, w, "Q1", []string{})
	if err != nil {
		t.Fatalf("UpsertEntity empty mappings: %v", err)
	}

	// Entity exists but LookupByWikidataID should return nil (empty mappings filtered).
	result, err := r.LookupByWikidataID(1)
	if err != nil {
		t.Fatalf("LookupByWikidataID: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for entity with empty mappings, got %+v", result)
	}
}

func TestUpsertEntityWithNilMappings(t *testing.T) {
	w, r := newTestSetup(t)

	// nil and []string{} should behave identically: entity exists with empty mappings.
	err := testUpsertEntity(t, w, "Q1", nil)
	if err != nil {
		t.Fatalf("UpsertEntity nil mappings: %v", err)
	}

	result, err := r.LookupByWikidataID(1)
	if err != nil {
		t.Fatalf("LookupByWikidataID: %v", err)
	}
	if result != nil {
		t.Errorf("expected nil for entity with nil mappings, got %+v", result)
	}

	// Verify entity hash exists (not deleted, just empty).
	stats, err := r.GetStats()
	if err != nil {
		t.Fatalf("GetStats: %v", err)
	}
	if stats.EntityCount != 1 {
		t.Errorf("EntityCount = %d, want 1 (entity exists with empty mappings)", stats.EntityCount)
	}
}

func TestUpsertThenClearMappings(t *testing.T) {
	w, r := newTestSetup(t)

	// Create entity with mappings.
	err := testUpsertEntity(t, w, "Q1", []string{"P345:tt1", "P4947:100"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}
	result := testLookupFirst(t, r, 345, "tt1")
	if result == nil {
		t.Fatal("expected entity to exist")
	}

	// Update with empty mappings — should clear property indexes.
	err = testUpsertEntity(t, w, "Q1", []string{})
	if err != nil {
		t.Fatalf("UpsertEntity empty: %v", err)
	}

	result = testLookupFirst(t, r, 345, "tt1")
	if result != nil {
		t.Error("expected property index to be cleared")
	}

	result = testLookupFirst(t, r, 4947, "100")
	if result != nil {
		t.Error("expected second property index to be cleared")
	}
}

func TestEmptyMappingsVsDelete(t *testing.T) {
	w, r := newTestSetup(t)

	// Create entity with mappings.
	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})

	stats, _ := r.GetStats()
	if stats.EntityCount != 1 {
		t.Fatalf("EntityCount = %d, want 1", stats.EntityCount)
	}

	// Upsert with empty mappings: entity still exists, count unchanged.
	testUpsertEntity(t, w, "Q1", []string{})

	stats, _ = r.GetStats()
	if stats.EntityCount != 1 {
		t.Errorf("EntityCount after empty-mappings upsert = %d, want 1", stats.EntityCount)
	}

	// Property index should be gone.
	result := testLookupFirst(t, r, 345, "tt1")
	if result != nil {
		t.Error("expected property index cleared after empty-mappings upsert")
	}

	// But the entity hash still exists (lookup by QID returns nil because
	// the reader filters empty mappings, but entity_count proves it's there).

	// Now delete: entity is truly gone, count decrements.
	testDeleteEntity(t, w, "Q1")

	stats, _ = r.GetStats()
	if stats.EntityCount != 0 {
		t.Errorf("EntityCount after delete = %d, want 0", stats.EntityCount)
	}
}

func TestRecordFailedEntityIncrementsAttempts(t *testing.T) {
	w, r := newTestSetup(t)

	err := testRecordFailedEntity(t, w, "Q1", "timeout")
	if err != nil {
		t.Fatalf("RecordFailedEntity first: %v", err)
	}

	// Record again — should increment attempts.
	err = testRecordFailedEntity(t, w, "Q1", "connection refused")
	if err != nil {
		t.Fatalf("RecordFailedEntity second: %v", err)
	}

	// Should still show as 1 entry.
	failed, err := r.LastFailedEntities(10)
	if err != nil {
		t.Fatalf("LastFailedEntities: %v", err)
	}
	if len(failed) != 1 {
		t.Errorf("failed count = %d, want 1 (same entity)", len(failed))
	}
}

func TestRecordFailedEntityRotatesToBack(t *testing.T) {
	w, r := newTestSetup(t)

	// Q1 fails first, then Q2, then Q3.
	if err := testRecordFailedEntity(t, w, "Q1", "err"); err != nil {
		t.Fatal(err)
	}
	if err := testRecordFailedEntity(t, w, "Q2", "err"); err != nil {
		t.Fatal(err)
	}
	if err := testRecordFailedEntity(t, w, "Q3", "err"); err != nil {
		t.Fatal(err)
	}

	// Q1 should be at front (oldest).
	first, err := r.LastFailedEntities(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0] != "Q1" {
		t.Fatalf("expected Q1 at front, got %v", first)
	}

	// Q1 fails again — should rotate to the back.
	if err := testRecordFailedEntity(t, w, "Q1", "still failing"); err != nil {
		t.Fatal(err)
	}

	// Now Q2 should be at front (Q1 moved to back).
	first, err = r.LastFailedEntities(1)
	if err != nil {
		t.Fatal(err)
	}
	if len(first) != 1 || first[0] != "Q2" {
		t.Fatalf("expected Q2 at front after Q1 re-failed, got %v", first)
	}

	// All three still present.
	all, err := r.LastFailedEntities(10)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 3 {
		t.Fatalf("expected 3 failed entities, got %d", len(all))
	}
}

func TestEnqueueEntitiesEmpty(t *testing.T) {
	w, _ := newTestSetup(t)

	err := testEnqueueEntities(t, w, nil)
	if err != nil {
		t.Fatalf("EnqueueEntities(nil): %v", err)
	}

	err = testEnqueueEntities(t, w, []string{})
	if err != nil {
		t.Fatalf("EnqueueEntities(empty): %v", err)
	}
}

func TestAckProcessedEntitiesEmpty(t *testing.T) {
	w, _ := newTestSetup(t)

	if err := testAckProcessedEntities(t, w, nil); err != nil {
		t.Fatalf("AckProcessedEntities(nil): %v", err)
	}
	if err := testAckProcessedEntities(t, w, []string{}); err != nil {
		t.Fatalf("AckProcessedEntities(empty): %v", err)
	}
}

func TestEnqueueEntitiesDeduplication(t *testing.T) {
	w, r := newTestSetup(t)

	err := testEnqueueEntities(t, w, []string{"Q1", "Q2", "Q3"})
	if err != nil {
		t.Fatalf("first EnqueueEntities: %v", err)
	}

	// Re-enqueue overlapping set.
	err = testEnqueueEntities(t, w, []string{"Q2", "Q3", "Q4"})
	if err != nil {
		t.Fatalf("second EnqueueEntities: %v", err)
	}

	count, _ := r.PendingCount()
	if count != 4 {
		t.Errorf("PendingCount = %d, want 4", count)
	}
}

func TestUpsertInvalidQID(t *testing.T) {
	w, _ := newTestSetup(t)

	err := testUpsertEntity(t, w, "INVALID", []string{})
	if err == nil {
		t.Error("expected error for invalid QID")
	}

	err = testUpsertEntity(t, w, "", []string{})
	if err == nil {
		t.Error("expected error for empty QID")
	}
}

func TestDeleteInvalidQID(t *testing.T) {
	w, _ := newTestSetup(t)

	err := testDeleteEntity(t, w, "INVALID")
	if err == nil {
		t.Error("expected error for invalid QID in delete")
	}
}

func TestEnqueueInvalidQID(t *testing.T) {
	w, _ := newTestSetup(t)

	err := testEnqueueEntities(t, w, []string{"INVALID"})
	if err == nil {
		t.Error("expected error for invalid QID in enqueue")
	}
}

func TestAckProcessedInvalidQID(t *testing.T) {
	w, _ := newTestSetup(t)

	err := testAckProcessedEntities(t, w, []string{"INVALID"})
	if err == nil {
		t.Error("expected error for invalid QID in ack processed")
	}
}

func TestRecoverProcessing(t *testing.T) {
	w, r := newTestSetup(t)

	// Enqueue 3, claim 2 (moves them to processing).
	testEnqueueEntities(t, w, []string{"Q1", "Q2", "Q3"})
	claimed, err := w.ClaimPendingBatch(2)
	if err != nil {
		t.Fatalf("ClaimPendingBatch: %v", err)
	}
	if len(claimed) != 2 {
		t.Fatalf("claimed = %d, want 2", len(claimed))
	}

	// Simulate crash: pending has 1, processing has 2.
	count, _ := r.PendingCount()
	if count != 1 {
		t.Fatalf("PendingCount before recover = %d, want 1", count)
	}

	// Recover should move processing back to pending.
	if err := w.RecoverProcessing(); err != nil {
		t.Fatalf("RecoverProcessing: %v", err)
	}

	count, _ = r.PendingCount()
	if count != 3 {
		t.Errorf("PendingCount after recover = %d, want 3", count)
	}

	// Claim new entity added during processing is preserved.
	testEnqueueEntities(t, w, []string{"Q4"})
	count, _ = r.PendingCount()
	if count != 4 {
		t.Errorf("PendingCount after adding Q4 = %d, want 4", count)
	}
}

func TestClaimAllowsReEnqueue(t *testing.T) {
	// This tests the race condition fix: once a QID is claimed (moved to
	// processing), a new SADD for the same QID should succeed in pending.
	w, r := newTestSetup(t)

	testEnqueueEntities(t, w, []string{"Q42"})

	// Claim Q42 — moves it from pending to processing.
	claimed, err := w.ClaimPendingBatch(10)
	if err != nil {
		t.Fatalf("ClaimPendingBatch: %v", err)
	}
	if len(claimed) != 1 || claimed[0] != "Q42" {
		t.Fatalf("claimed = %v, want [Q42]", claimed)
	}

	// Pending should be empty now.
	count, _ := r.PendingCount()
	if count != 0 {
		t.Fatalf("PendingCount after claim = %d, want 0", count)
	}

	// Re-enqueue Q42 (simulates a new event arriving during processing).
	err = testEnqueueEntities(t, w, []string{"Q42"})
	if err != nil {
		t.Fatalf("re-EnqueueEntities: %v", err)
	}

	// Ack the original claim.
	if err := testAckProcessedEntities(t, w, claimed); err != nil {
		t.Fatalf("AckProcessedEntities: %v", err)
	}

	// Q42 should still be in pending for the next cycle.
	count, _ = r.PendingCount()
	if count != 1 {
		t.Errorf("PendingCount after ack = %d, want 1", count)
	}
}

func TestRequeueAfterClaim(t *testing.T) {
	// Simulates requeueOnFailure: claim a batch, then re-enqueue + ack.
	w, r := newTestSetup(t)

	testEnqueueEntities(t, w, []string{"Q1", "Q2", "Q3"})
	claimed, _ := w.ClaimPendingBatch(2)

	// Re-enqueue the claimed batch (simulates batch failure).
	testEnqueueEntities(t, w, claimed)
	testAckProcessedEntities(t, w, claimed)

	// All 3 should be back in pending.
	count, _ := r.PendingCount()
	if count != 3 {
		t.Errorf("PendingCount after requeue = %d, want 3", count)
	}
}

func TestMultipleEntityBatchChangelogEvents(t *testing.T) {
	w, r := newTestSetup(t)

	err := testUpsertEntitiesBatch(t, w, []EntityRecord{
		{WikidataID: "Q1", Mappings: []string{"P345:tt1"}},
		{WikidataID: "Q2", Mappings: []string{"P345:tt2"}},
		{WikidataID: "Q3", Mappings: []string{"P345:tt3"}},
	})
	if err != nil {
		t.Fatalf("UpsertEntitiesBatch: %v", err)
	}

	events, err := r.StreamRead(context.Background(), "0", 10, 0)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(events) != 3 {
		t.Errorf("events = %d, want 3", len(events))
	}

	// Verify each event has the right QID.
	qids := map[int64]bool{}
	for _, e := range events {
		qids[e.QID] = true
		if e.RawMappings == "" {
			t.Error("expected non-empty RawMappings for upsert")
		}
	}
	for _, expected := range []int64{1, 2, 3} {
		if !qids[expected] {
			t.Errorf("missing changelog event for Q%d", expected)
		}
	}
}

func TestDeleteBatchWritesChangelogPerEntity(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})

	// Get stream position after upserts.
	all, _ := r.StreamRead(context.Background(), "0", 10, 0)
	lastUpsertID := all[len(all)-1].ID

	testDeleteEntitiesBatch(t, w, []string{"Q1", "Q2"})

	events, err := r.StreamRead(context.Background(), lastUpsertID, 10, 0)
	if err != nil {
		t.Fatalf("StreamRead: %v", err)
	}
	if len(events) != 2 {
		t.Errorf("delete events = %d, want 2", len(events))
	}
	for _, e := range events {
		if e.RawMappings != "" {
			t.Errorf("expected empty RawMappings for delete, got %q", e.RawMappings)
		}
	}
}

func TestEntityCountAfterDeleteBatch(t *testing.T) {
	w, r := newTestSetup(t)

	testUpsertEntity(t, w, "Q1", []string{"P345:tt1"})
	testUpsertEntity(t, w, "Q2", []string{"P345:tt2"})
	testUpsertEntity(t, w, "Q3", []string{"P345:tt3"})

	stats, _ := r.GetStats()
	if stats.EntityCount != 3 {
		t.Errorf("initial EntityCount = %d, want 3", stats.EntityCount)
	}

	testDeleteEntitiesBatch(t, w, []string{"Q1", "Q3"})
	stats, _ = r.GetStats()
	if stats.EntityCount != 1 {
		t.Errorf("after delete EntityCount = %d, want 1", stats.EntityCount)
	}
}

func TestIsStreamNotFoundError(t *testing.T) {
	tests := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{fmt.Errorf("ERR no such key"), true},
		{fmt.Errorf("ERR no longer exist"), true},
		{fmt.Errorf("some other error"), false},
	}
	for _, tc := range tests {
		got := isStreamNotFoundError(tc.err)
		if got != tc.want {
			t.Errorf("isStreamNotFoundError(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

// Tests below exercise specific merge-walk branches in the upsert Lua script.

func TestUpsertDrainRemainingNew(t *testing.T) {
	w, r := newTestSetup(t)

	// Start with one mapping whose key sorts first.
	err := testUpsertEntity(t, w, "Q1", []string{"P1:a"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Update: keep the old mapping and add two that sort after it.
	// After the equal match on P1:a, the merge-walk drains P2:b and P3:c.
	err = testUpsertEntity(t, w, "Q1", []string{"P1:a", "P2:b", "P3:c"})
	if err != nil {
		t.Fatalf("UpsertEntity (drain new): %v", err)
	}

	for _, tc := range []struct {
		prop int
		val  string
	}{{2, "b"}, {3, "c"}} {
		result := testLookupFirst(t, r, tc.prop, tc.val)
		if result == nil || result.WikidataID != 1 {
			t.Errorf("P%d:%s: expected Q1, got %+v", tc.prop, tc.val, result)
		}
	}
}

func TestUpsertDrainRemainingOld(t *testing.T) {
	w, r := newTestSetup(t)

	// Start with three mappings.
	err := testUpsertEntity(t, w, "Q1", []string{"P1:a", "P2:b", "P3:c"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Update: keep only the first. After the equal match on P1:a,
	// the merge-walk drains P2:b and P3:c from old.
	err = testUpsertEntity(t, w, "Q1", []string{"P1:a"})
	if err != nil {
		t.Fatalf("UpsertEntity (drain old): %v", err)
	}

	result := testLookupFirst(t, r, 1, "a")
	if result == nil || result.WikidataID != 1 {
		t.Errorf("P1:a: expected Q1, got %+v", result)
	}

	for _, tc := range []struct {
		prop int
		val  string
	}{{2, "b"}, {3, "c"}} {
		result := testLookupFirst(t, r, tc.prop, tc.val)
		if result != nil {
			t.Errorf("P%d:%s: expected nil (removed), got %+v", tc.prop, tc.val, result)
		}
	}
}

func TestUpsertCompleteReplacement(t *testing.T) {
	w, r := newTestSetup(t)

	// Start with mappings that all sort before the replacements.
	err := testUpsertEntity(t, w, "Q1", []string{"P1:x", "P2:y"})
	if err != nil {
		t.Fatalf("UpsertEntity: %v", err)
	}

	// Replace with entirely different mappings that sort after.
	err = testUpsertEntity(t, w, "Q1", []string{"P3:a", "P4:b"})
	if err != nil {
		t.Fatalf("UpsertEntity (replace): %v", err)
	}

	// Old indexes should be gone.
	for _, tc := range []struct {
		prop int
		val  string
	}{{1, "x"}, {2, "y"}} {
		result := testLookupFirst(t, r, tc.prop, tc.val)
		if result != nil {
			t.Errorf("P%d:%s: expected nil (removed), got %+v", tc.prop, tc.val, result)
		}
	}

	// New indexes should exist.
	for _, tc := range []struct {
		prop int
		val  string
	}{{3, "a"}, {4, "b"}} {
		result := testLookupFirst(t, r, tc.prop, tc.val)
		if result == nil || result.WikidataID != 1 {
			t.Errorf("P%d:%s: expected Q1, got %+v", tc.prop, tc.val, result)
		}
	}
}
