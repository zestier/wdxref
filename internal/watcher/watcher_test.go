package watcher

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// buildTestEntityJSON constructs a bare Wikidata entity JSON object for
// testing. Each property is given datatype "external-id" so extractExternalIDs
// picks it up. All claims default to rank "normal". Returns the entity
// object directly (no API wrapper).
func buildTestEntityJSON(qid, label string, properties map[string]string) []byte {
	claims := make(map[string]interface{})
	for propID, value := range properties {
		claims[propID] = []map[string]interface{}{
			{
				"mainsnak": map[string]interface{}{
					"datatype": "external-id",
					"datavalue": map[string]interface{}{
						"value": value,
						"type":  "string",
					},
				},
				"rank": "normal",
			},
		}
	}

	entity := map[string]interface{}{
		"id": qid,
		"labels": map[string]interface{}{
			"en": map[string]string{"value": label},
		},
		"claims": claims,
	}

	data, _ := json.Marshal(entity)
	return data
}

// buildTestAPIResponse wraps one or more bare entity objects in the
// wbgetentities response envelope {"entities":{...}}.
func buildTestAPIResponse(entities map[string][]byte) []byte {
	inner := make(map[string]json.RawMessage, len(entities))
	for qid, data := range entities {
		inner[qid] = json.RawMessage(data)
	}
	resp := map[string]interface{}{"entities": inner}
	data, _ := json.Marshal(resp)
	return data
}

func TestParseEntityJSON_Movie(t *testing.T) {
	data := buildTestEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
		"P4835": "2095",
		"P8013": "the-shawshank-redemption",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
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
	if !slices.Contains(entity.Mappings, "P4835:2095") {
		t.Errorf("expected P4835:2095 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P8013:the-shawshank-redemption") {
		t.Errorf("expected P8013:the-shawshank-redemption in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_TVSeries(t *testing.T) {
	data := buildTestEntityJSON("Q1079", "Breaking Bad", map[string]string{
		"P345":  "tt0903747",
		"P4983": "1396",
		"P2638": "169",
		"P4835": "81189",
		"P8013": "breaking-bad",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}

	if len(entity.Mappings) != 5 {
		t.Errorf("len(Mappings) = %d, want 5", len(entity.Mappings))
	}
	if !slices.Contains(entity.Mappings, "P345:tt0903747") {
		t.Errorf("expected P345:tt0903747 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P4983:1396") {
		t.Errorf("expected P4983:1396 in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_NoExternalIDs(t *testing.T) {
	// Entity with no external-id claims should return entity with empty mappings.
	entity := map[string]interface{}{
		"id": "Q42",
		"labels": map[string]interface{}{
			"en": map[string]string{"value": "Douglas Adams"},
		},
		"claims": map[string]interface{}{
			"P31": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
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

func TestExtractExternalIDs_NoClaims(t *testing.T) {
	result := extractExternalIDs(nil)
	if result == nil {
		t.Fatal("expected non-nil empty slice, got nil")
	}
	if len(result) != 0 {
		t.Errorf("expected empty slice, got %v", result)
	}
}

func TestExtractExternalIDs_RankOrdering(t *testing.T) {
	// Build an entity with multiple claims per property at different ranks.
	// P345 has preferred + normal; P100 has deprecated + normal.
	// Expected order: P100 (numeric < P345), normal before deprecated;
	// then P345, preferred before normal.
	entity := map[string]interface{}{
		"id": "Q1",
		"claims": map[string]interface{}{
			"P345": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "normal-val", "type": "string"},
					},
					"rank": "normal",
				},
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "preferred-val", "type": "string"},
					},
					"rank": "preferred",
				},
			},
			"P100": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "deprecated-val", "type": "string"},
					},
					"rank": "deprecated",
				},
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "normal-val", "type": "string"},
					},
					"rank": "normal",
				},
			},
		},
	}
	data, _ := json.Marshal(entity)
	parsed, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}

	want := []string{
		"P100:normal-val",
		"P100:deprecated-val",
		"P345:preferred-val",
		"P345:normal-val",
	}
	if !slices.Equal(parsed.Mappings, want) {
		t.Errorf("rank ordering:\ngot  %v\nwant %v", parsed.Mappings, want)
	}
}

func TestExtractExternalIDs_Dedup(t *testing.T) {
	// Same property+value at two different ranks should be deduplicated.
	// The sort places higher ranks first, so the dedup naturally keeps the
	// highest-ranked occurrence.
	entity := map[string]interface{}{
		"id": "Q2",
		"claims": map[string]interface{}{
			"P345": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "tt123", "type": "string"},
					},
					"rank": "preferred",
				},
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "tt123", "type": "string"},
					},
					"rank": "normal",
				},
			},
		},
	}
	data, _ := json.Marshal(entity)
	parsed, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}

	if len(parsed.Mappings) != 1 {
		t.Fatalf("expected 1 mapping after dedup, got %d: %v", len(parsed.Mappings), parsed.Mappings)
	}
	if parsed.Mappings[0] != "P345:tt123" {
		t.Errorf("got %q, want P345:tt123", parsed.Mappings[0])
	}
}

func TestExtractExternalIDs_ValueSubsort(t *testing.T) {
	// Multiple distinct values at the same rank for the same property should
	// be sorted alphabetically, ensuring deterministic output regardless of
	// JSON map iteration order.
	entity := map[string]interface{}{
		"id": "Q4",
		"claims": map[string]interface{}{
			"P345": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "tt999", "type": "string"},
					},
					"rank": "normal",
				},
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "tt111", "type": "string"},
					},
					"rank": "normal",
				},
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "tt555", "type": "string"},
					},
					"rank": "normal",
				},
			},
		},
	}
	data, _ := json.Marshal(entity)
	parsed, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}

	want := []string{"P345:tt111", "P345:tt555", "P345:tt999"}
	if !slices.Equal(parsed.Mappings, want) {
		t.Errorf("value subsort:\ngot  %v\nwant %v", parsed.Mappings, want)
	}
}

func TestExtractExternalIDs_MissingRank(t *testing.T) {
	// Claims without a "rank" field should be treated as normal (rank 0).
	// Older dumps or hand-crafted JSON may omit it entirely.
	entity := map[string]interface{}{
		"id": "Q3",
		"claims": map[string]interface{}{
			"P345": []map[string]interface{}{
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "no-rank", "type": "string"},
					},
					// rank field intentionally absent
				},
				{
					"mainsnak": map[string]interface{}{
						"datatype":  "external-id",
						"datavalue": map[string]interface{}{"value": "preferred-val", "type": "string"},
					},
					"rank": "preferred",
				},
			},
		},
	}
	data, _ := json.Marshal(entity)
	parsed, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}

	want := []string{"P345:preferred-val", "P345:no-rank"}
	if !slices.Equal(parsed.Mappings, want) {
		t.Errorf("missing rank ordering:\ngot  %v\nwant %v", parsed.Mappings, want)
	}
}

func TestParseEntityJSON_SingleID(t *testing.T) {
	data := buildTestEntityJSON("Q999", "Obscure Movie", map[string]string{
		"P345": "tt9999999",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity with 1 ID, got nil")
	}
	if !slices.Contains(entity.Mappings, "P345:tt9999999") {
		t.Errorf("expected P345:tt9999999 in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_VideoGame(t *testing.T) {
	data := buildTestEntityJSON("Q47740", "Portal 2", map[string]string{
		"P1733": "620",
		"P5794": "72",
		"P1933": "50538",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if !slices.Contains(entity.Mappings, "P1733:620") {
		t.Errorf("expected P1733:620 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P5794:72") {
		t.Errorf("expected P5794:72 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P1933:50538") {
		t.Errorf("expected P1933:50538 in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_Person(t *testing.T) {
	data := buildTestEntityJSON("Q42", "Douglas Adams", map[string]string{
		"P345":  "nm0010930",
		"P4985": "42",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if !slices.Contains(entity.Mappings, "P345:nm0010930") {
		t.Errorf("expected P345:nm0010930 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P4985:42") {
		t.Errorf("expected P4985:42 in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_Book(t *testing.T) {
	data := buildTestEntityJSON("Q8337", "Harry Potter and the Philosopher's Stone", map[string]string{
		"P212":  "978-0747532743",
		"P648":  "OL82563W",
		"P2969": "72193",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if !slices.Contains(entity.Mappings, "P212:978-0747532743") {
		t.Errorf("expected P212:978-0747532743 in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P648:OL82563W") {
		t.Errorf("expected P648:OL82563W in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_MusicGroup(t *testing.T) {
	data := buildTestEntityJSON("Q11036", "Radiohead", map[string]string{
		"P434":  "a74b1b7f-71a5-4011-9441-d0b5e4122711",
		"P1902": "4Z8W4fKeB5YxbusRsdQVPb",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if !slices.Contains(entity.Mappings, "P434:a74b1b7f-71a5-4011-9441-d0b5e4122711") {
		t.Errorf("expected P434:UUID in mappings, got %v", entity.Mappings)
	}
	if !slices.Contains(entity.Mappings, "P1902:4Z8W4fKeB5YxbusRsdQVPb") {
		t.Errorf("expected P1902:4Z8W4fKeB5YxbusRsdQVPb in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_MusicAlbum(t *testing.T) {
	data := buildTestEntityJSON("Q190588", "OK Computer", map[string]string{
		"P436": "b1392450-e666-3926-a536-22c65f834433",
	})

	entity, err := ParseEntityJSON(data)
	if err != nil {
		t.Fatalf("ParseEntityJSON: %v", err)
	}
	if entity == nil {
		t.Fatal("expected entity, got nil")
	}
	if !slices.Contains(entity.Mappings, "P436:b1392450-e666-3926-a536-22c65f834433") {
		t.Errorf("expected P436:UUID in mappings, got %v", entity.Mappings)
	}
}

func TestParseEntityJSON_InvalidJSON(t *testing.T) {
	_, err := ParseEntityJSON([]byte("not valid json"))
	if err == nil {
		t.Error("expected error for invalid JSON")
	}
}

func TestProcessorWithMockServer(t *testing.T) {
	writer, reader := newTestStoreWriter(t)

	entityObj := buildTestEntityJSON("Q172241", "The Shawshank Redemption", map[string]string{
		"P345":  "tt0111161",
		"P4947": "278",
		"P4835": "2095",
	})

	apiResp := buildTestAPIResponse(map[string][]byte{"Q172241": entityObj})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(apiResp)
	}))
	defer server.Close()

	client := NewWikidataClient(server.Client())
	client.baseURL = server.URL

	processor := NewProcessor(writer, client)

	err := processor.ProcessEntity("Q172241")
	if err != nil {
		t.Fatalf("ProcessEntity: %v", err)
	}

	result := lookupFirst(t, reader, 345, "tt0111161")
	if result == nil {
		t.Fatal("expected result after processing, got nil")
	}
	if result.WikidataID != 172241 {
		t.Errorf("WikidataID = %d, want 172241", result.WikidataID)
	}
	// Check that TMDB movie mapping is present
	if mappings := decodeMappings(t, result); !slices.Contains(mappings, "P4947:278") {
		t.Errorf("expected P4947:278 in mappings, got %v", mappings)
	}
}

// TestFetchEntitiesRaw_APIErrorDoesNotDelete verifies that a non-maxlag API
// error (e.g. readonly, internal-api-error) is returned as a batch error
// instead of silently marking every entity as missing (which would delete them).
func TestFetchEntitiesRaw_APIErrorDoesNotDelete(t *testing.T) {
	apiError := `{"error":{"code":"readonly","info":"The wiki is currently in read-only mode.","docref":"See https://www.wikidata.org/w/api.php for API usage"}}`

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(apiError))
	}))
	defer server.Close()

	client := NewWikidataClient(server.Client())
	client.baseURL = server.URL

	_, err := client.FetchEntitiesRaw([]string{"Q1", "Q2", "Q3"})
	if err == nil {
		t.Fatal("expected error for API error response, got nil")
	}
	if strings.Contains(err.Error(), "missing") {
		t.Errorf("API error should not produce 'missing' error: %v", err)
	}
	if !strings.Contains(err.Error(), "readonly") {
		t.Errorf("expected error to mention 'readonly', got: %v", err)
	}
}

// TestFetchEntitiesRaw_EmptyEntitiesReturnsError verifies that a response
// with an empty entities map returns a batch error instead of marking
// everything as missing.
func TestFetchEntitiesRaw_EmptyEntitiesReturnsError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write([]byte(`{"entities":{}}`))
	}))
	defer server.Close()

	client := NewWikidataClient(server.Client())
	client.baseURL = server.URL

	_, err := client.FetchEntitiesRaw([]string{"Q1"})
	if err == nil {
		t.Fatal("expected error for empty entities response, got nil")
	}
}

// TestProcessEntities_BatchWithMixedResults verifies that a batch where some
// entities have external IDs and some don't produces the correct mix of
// upserts (not deletes).
func TestProcessEntities_BatchWithMixedResults(t *testing.T) {
	writer, reader := newTestStoreWriter(t)

	// Q1 has external IDs, Q2 has none, Q999 is missing.
	q1Obj := buildTestEntityJSON("Q1", "Entity with IDs", map[string]string{
		"P345": "tt1234567",
	})
	q2Obj := []byte(`{"id":"Q2","claims":{"P31":[{"mainsnak":{"datatype":"wikibase-item","datavalue":{"value":{"id":"Q5"},"type":"wikibase-entityid"}}}]}}`)
	q999Obj := []byte(`{"missing":""}`)

	apiResp := buildTestAPIResponse(map[string][]byte{
		"Q1":   q1Obj,
		"Q2":   q2Obj,
		"Q999": q999Obj,
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(apiResp)
	}))
	defer server.Close()

	client := NewWikidataClient(server.Client())
	client.baseURL = server.URL
	processor := NewProcessor(writer, client)

	perErrors, err := processor.ProcessEntities([]string{"Q1", "Q2", "Q999"})
	if err != nil {
		t.Fatalf("ProcessEntities: %v", err)
	}

	// Q1 should be upserted successfully.
	if perErrors["Q1"] != nil {
		t.Errorf("Q1 should succeed, got: %v", perErrors["Q1"])
	}

	// Q2 should be upserted (with empty mappings), not deleted.
	if perErrors["Q2"] != nil {
		t.Errorf("Q2 should succeed, got: %v", perErrors["Q2"])
	}

	// Q999 should be deleted (missing).
	if perErrors["Q999"] != nil {
		t.Errorf("Q999 should succeed (delete), got: %v", perErrors["Q999"])
	}

	result := lookupFirst(t, reader, 345, "tt1234567")
	if result == nil {
		t.Fatal("Q1 should be stored")
	}
}

// TestProcessEntities_AbsentFromResponseIsError verifies that a QID missing
// from the API response (not returned at all, as opposed to returned with
// a "missing" key) is treated as a per-entity error, not a delete.
func TestProcessEntities_AbsentFromResponseIsError(t *testing.T) {
	writer, reader := newTestStoreWriter(t)

	// Seed Q50 first so we can verify it's NOT deleted.
	testWriterUpsertEntity(t, writer, "Q50", []string{"P345:tt9999999"})

	// API response contains Q1 but not Q50 — simulates partial response.
	q1Obj := buildTestEntityJSON("Q1", "Entity", map[string]string{"P345": "tt1"})
	apiResp := buildTestAPIResponse(map[string][]byte{"Q1": q1Obj})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Write(apiResp)
	}))
	defer server.Close()

	client := NewWikidataClient(server.Client())
	client.baseURL = server.URL
	processor := NewProcessor(writer, client)

	perErrors, err := processor.ProcessEntities([]string{"Q1", "Q50"})
	if err != nil {
		t.Fatalf("ProcessEntities: %v", err)
	}

	// Q50 should be an error, not silently deleted.
	if perErrors["Q50"] == nil {
		t.Error("Q50 absent from response should be a per-entity error, not silent success")
	}

	// Verify Q50 was NOT deleted.
	result := lookupFirst(t, reader, 345, "tt9999999")
	if result == nil {
		t.Fatal("Q50 should NOT have been deleted")
	}
}

// sseEvent formats a single SSE event with id, data, and trailing blank line.
func sseEvent(id string, data []byte) string {
	return fmt.Sprintf("id: %s\ndata: %s\n\n", id, data)
}

func TestConnectAndProcess_StreamGapTriggersReseed(t *testing.T) {
	writer, reader := newTestStoreWriter(t)

	// Set last_sync to 30 days ago so the stream can't possibly go back that far.
	oldDumpTime := time.Now().Add(-30 * 24 * time.Hour).UTC().Format(time.RFC3339)
	testWriterSetSyncState(t, writer, "dump_time", oldDumpTime)

	// Create SSE server that sends one event with a recent timestamp.
	recentTS := time.Now().Unix()
	recentTSMs := recentTS * 1000
	eventData, _ := json.Marshal(map[string]interface{}{
		"meta":      map[string]string{"domain": "www.wikidata.org"},
		"wiki":      "wikidatawiki",
		"namespace": 0,
		"title":     "Q42",
		"timestamp": recentTS,
	})
	eventID := fmt.Sprintf(`[{"topic":"test","partition":0,"timestamp":%d}]`, recentTSMs)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, sseEvent(eventID, eventData))
	}))
	defer server.Close()

	processor := NewProcessor(writer, nil)
	esWatcher := NewEventStreamWatcher(processor, writer, reader, server.Client())
	esWatcher.streamURL = server.URL

	ctx := context.Background()
	err := esWatcher.connectAndProcess(ctx)

	if !errors.Is(err, ErrStreamTooOld) {
		t.Fatalf("expected ErrStreamTooOld, got: %v", err)
	}

	// Verify dump_time was cleared.
	dumpTime, err := reader.GetSyncState("dump_time")
	if err != nil {
		t.Fatalf("GetSyncState: %v", err)
	}
	if dumpTime != "" {
		t.Errorf("dump_time should be cleared, got %q", dumpTime)
	}
}

func TestConnectAndProcess_NoGapWhenStreamIsFresh(t *testing.T) {
	writer, reader := newTestStoreWriter(t)

	// Set dump_time close to now so the requested sinceTime stays well within
	// the 1h stream-gap threshold.
	recentDumpTime := time.Now().Add(72*time.Hour + 5*time.Minute).UTC().Format(time.RFC3339)
	testWriterSetSyncState(t, writer, "dump_time", recentDumpTime)

	// SSE server sends one event with timestamp matching ~now, then closes.
	eventData, _ := json.Marshal(map[string]interface{}{
		"meta":      map[string]string{"domain": "www.wikidata.org"},
		"wiki":      "wikidatawiki",
		"namespace": 0,
		"title":     "Q42",
		"timestamp": time.Now().Unix(),
	})

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, sseEvent("evt1", eventData))
	}))
	defer server.Close()

	processor := NewProcessor(writer, nil)
	esWatcher := NewEventStreamWatcher(processor, writer, reader, server.Client())
	esWatcher.streamURL = server.URL

	ctx := context.Background()
	err := esWatcher.connectAndProcess(ctx)

	// Should get "stream ended" (server closed), NOT ErrStreamTooOld.
	if errors.Is(err, ErrStreamTooOld) {
		t.Fatal("should not get ErrStreamTooOld when stream is fresh")
	}
	if err == nil {
		t.Fatal("expected some error (stream ended), got nil")
	}
}

func TestConnectAndProcess_EnqueueFailureReturnsError(t *testing.T) {
	s := miniredis.RunT(t)
	c, err := store.NewTestClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	writer := store.NewWriter(c)
	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	reader := store.NewReaderFromWriter(writer)

	recentDumpTime := time.Now().Add(72*time.Hour + 5*time.Minute).UTC().Format(time.RFC3339)
	testWriterSetSyncState(t, writer, "dump_time", recentDumpTime)

	closed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		if !closed {
			closed = true
			s.Close()
		}

		for i := 1; i <= enqueueBatchSize; i++ {
			eventData, _ := json.Marshal(map[string]interface{}{
				"meta":      map[string]string{"domain": "www.wikidata.org"},
				"wiki":      "wikidatawiki",
				"namespace": 0,
				"title":     fmt.Sprintf("Q%d", i),
				"timestamp": time.Now().Unix(),
			})
			fmt.Fprint(w, sseEvent(fmt.Sprintf("evt%d", i), eventData))
		}
	}))
	defer server.Close()

	processor := NewProcessor(writer, nil)
	esWatcher := NewEventStreamWatcher(processor, writer, reader, server.Client())
	esWatcher.streamURL = server.URL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = esWatcher.connectAndProcess(ctx)
	if err == nil {
		t.Fatal("expected enqueue writer error, got nil")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connectAndProcess timed out instead of returning enqueue error: %v", err)
	}
	if !strings.Contains(err.Error(), "enqueue writer:") {
		t.Fatalf("expected enqueue writer error, got: %v", err)
	}
}

func TestConnectAndProcess_FinalEnqueueFlushFailureReturnsError(t *testing.T) {
	s := miniredis.RunT(t)
	c, err := store.NewTestClient(s.Addr())
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer c.Close()

	writer := store.NewWriter(c)
	if err := writer.MigrateSchema(); err != nil {
		t.Fatalf("MigrateSchema: %v", err)
	}
	reader := store.NewReaderFromWriter(writer)

	recentDumpTime := time.Now().Add(72*time.Hour + 5*time.Minute).UTC().Format(time.RFC3339)
	testWriterSetSyncState(t, writer, "dump_time", recentDumpTime)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)

		eventData, _ := json.Marshal(map[string]interface{}{
			"meta":      map[string]string{"domain": "www.wikidata.org"},
			"wiki":      "wikidatawiki",
			"namespace": 0,
			"title":     "Q1",
			"timestamp": time.Now().Unix(),
		})
		fmt.Fprint(w, sseEvent("evt1", eventData))

		// Force the only enqueue flush to happen during the deferred close path.
		s.Close()
	}))
	defer server.Close()

	processor := NewProcessor(writer, nil)
	esWatcher := NewEventStreamWatcher(processor, writer, reader, server.Client())
	esWatcher.streamURL = server.URL

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err = esWatcher.connectAndProcess(ctx)
	if err == nil {
		t.Fatal("expected enqueue writer error, got nil")
	}
	if errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("connectAndProcess timed out instead of returning enqueue error: %v", err)
	}
	if !strings.Contains(err.Error(), "enqueue writer:") {
		t.Fatalf("expected enqueue writer error, got: %v", err)
	}
	if strings.Contains(err.Error(), "stream ended") {
		t.Fatalf("expected deferred flush error to win over stream ended, got: %v", err)
	}
}

func TestExtractEventIDTime(t *testing.T) {
	tests := []struct {
		name    string
		eventID string
		wantMS  int64 // expected unix millis, 0 means zero time
	}{
		{
			name:    "real event ID with one timestamp",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":1773879470875}]`,
			wantMS:  1773879470875,
		},
		{
			name:    "both entries have timestamps, picks min",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"timestamp":1000},{"topic":"codfw.mediawiki.recentchange","partition":0,"timestamp":2000}]`,
			wantMS:  1000,
		},
		{
			name:    "no timestamps at all",
			eventID: `[{"topic":"eqiad.mediawiki.recentchange","partition":0,"offset":-1},{"topic":"codfw.mediawiki.recentchange","partition":0,"offset":-1}]`,
			wantMS:  0,
		},
		{
			name:    "empty array",
			eventID: `[]`,
			wantMS:  0,
		},
		{
			name:    "invalid JSON",
			eventID: `not json`,
			wantMS:  0,
		},
		{
			name:    "empty string",
			eventID: "",
			wantMS:  0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := extractEventIDTime(tt.eventID)
			if tt.wantMS == 0 {
				if !got.IsZero() {
					t.Errorf("expected zero time, got %v", got)
				}
			} else {
				want := time.UnixMilli(tt.wantMS)
				if !got.Equal(want) {
					t.Errorf("got %v, want %v", got, want)
				}
			}
		})
	}
}
