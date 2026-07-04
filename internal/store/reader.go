package store

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/ekeid/ekeid/internal/model"
	"github.com/redis/go-redis/v9"
)

// lookupByPropertyScript resolves (property, value) → entities in a single
// round-trip.  KEYS[1] = property hash key.  ARGV[1] = value.
// Returns a flat array of {qid, mappings_json, qid, mappings_json, ...}
// pairs for all entities in the index, or nil when not found.
// Entities with empty mappings are skipped.
var lookupByPropertyScript = redis.NewScript(`
local raw = redis.call('HGET', KEYS[1], ARGV[1])
if not raw then return nil end
local result = {}
for eid in string.gmatch(raw, '[^,]+') do
  local m = redis.call('HGET', 'entities', eid)
  if m then
    result[#result + 1] = eid
    result[#result + 1] = m
  end
end
if #result == 0 then return nil end
return result
`)

// Reader provides read-only access to Kvrocks (used by the API server and replicator).
type Reader struct {
	rdb      *redis.Client
	schemaOK bool
}

// NewReader creates a Reader that connects to Kvrocks via the given Client.
// It checks the schema version on creation.
func NewReader(c *Client) *Reader {
	ctx := context.Background()
	stored, err := c.Redis().HGet(ctx, metaKey, "schema_version").Result()
	schemaOK := err == nil && stored == SchemaVersion()
	return &Reader{rdb: c.Redis(), schemaOK: schemaOK}
}

// NewReaderFromWriter creates a Reader that shares the same connection as the
// given Writer. This is intended for use in tests where a Writer and Reader
// need to operate on the same data.
func NewReaderFromWriter(w *Writer) *Reader {
	ctx := context.Background()
	stored, err := w.rdb.HGet(ctx, metaKey, "schema_version").Result()
	schemaOK := err == nil && stored == SchemaVersion()
	return &Reader{rdb: w.rdb, schemaOK: schemaOK}
}

// LookupByProperty finds all entities mapped to (property, value) and returns
// each entity's mappings in a single round-trip via Lua.
// Returns an empty slice when no mappings are found.
func (r *Reader) LookupByProperty(property int, value string) ([]model.LookupResult, error) {
	return r.LookupByPropertyContext(context.Background(), property, value)
}

// GetSyncState retrieves a value from the meta hash. Returns "" if not found.
func (r *Reader) GetSyncState(key string) (string, error) {
	val, err := r.rdb.HGet(context.Background(), metaKey, key).Result()
	if err == redis.Nil {
		return "", nil
	}
	return val, err
}

// GetSyncStates retrieves multiple values from the meta hash in a single
// round-trip. Missing keys are returned as empty strings.
func (r *Reader) GetSyncStates(keys ...string) (map[string]string, error) {
	vals, err := r.rdb.HMGet(context.Background(), metaKey, keys...).Result()
	if err != nil {
		return nil, err
	}
	result := make(map[string]string, len(keys))
	for i, key := range keys {
		if i < len(vals) && vals[i] != nil {
			result[key] = vals[i].(string)
		}
	}
	return result, nil
}

// PendingCount returns the number of entities in the pending set.
func (r *Reader) PendingCount() (int64, error) {
	return r.rdb.SCard(context.Background(), pendingKey).Result()
}

// LastFailedEntities returns up to limit failed entity IDs for retry,
// oldest first (by first_failed time, which is the ZSET score).
func (r *Reader) LastFailedEntities(limit int) ([]string, error) {
	ctx := context.Background()
	members, err := r.rdb.ZRange(ctx, failedZsetKey, 0, int64(limit-1)).Result()
	if err != nil {
		return nil, err
	}
	qids := make([]string, 0, len(members))
	for _, m := range members {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			continue
		}
		qids = append(qids, intToQid(id))
	}
	return qids, nil
}

// LookupByPropertyContext finds all entities mapped to (property, value) and
// returns each entity's mappings in a single round-trip via Lua.
// Returns an empty slice when no mappings are found.
func (r *Reader) LookupByPropertyContext(ctx context.Context, property int, value string) ([]model.LookupResult, error) {
	if !r.schemaOK {
		return nil, ErrSchemaMismatch
	}

	res, err := lookupByPropertyScript.RunRO(ctx, r.rdb,
		[]string{propKey(property)}, value).StringSlice()
	if err == redis.Nil {
		return []model.LookupResult{}, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup by property: %w", err)
	}
	if len(res) < 2 {
		return []model.LookupResult{}, nil
	}

	results := make([]model.LookupResult, 0, len(res)/2)
	for i := 0; i+1 < len(res); i += 2 {
		qid, err := strconv.ParseInt(res[i], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("parse QID from script: %w", err)
		}

		raw := res[i+1]
		if raw == "" || raw == "[]" {
			continue
		}

		results = append(results, model.LookupResult{
			WikidataID:  qid,
			RawMappings: raw,
		})
	}

	return results, nil
}

// LookupByWikidataID finds the entity by its Wikidata numeric ID and returns
// all of its mappings.
func (r *Reader) LookupByWikidataID(wikidataID int64) (*model.LookupResult, error) {
	return r.LookupByWikidataIDContext(context.Background(), wikidataID)
}

// LookupByWikidataIDContext finds the entity by its Wikidata numeric ID and
// returns all of its mappings.
func (r *Reader) LookupByWikidataIDContext(ctx context.Context, wikidataID int64) (*model.LookupResult, error) {
	if !r.schemaOK {
		return nil, ErrSchemaMismatch
	}
	return r.lookupEntity(ctx, wikidataID)
}

// lookupEntity reads and returns the entity data for a given QID.
func (r *Reader) lookupEntity(ctx context.Context, qid int64) (*model.LookupResult, error) {
	mStr, err := r.rdb.HGet(ctx, entitiesKey, strconv.FormatInt(qid, 10)).Result()
	if err == redis.Nil {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get entity: %w", err)
	}

	if mStr == "" || mStr == "[]" {
		return nil, nil
	}

	return &model.LookupResult{
		WikidataID:  qid,
		RawMappings: mStr,
	}, nil
}

// GetHealth returns lightweight health information.
func (r *Reader) GetHealth() (*model.HealthInfo, error) {
	return r.GetHealthContext(context.Background())
}

// GetHealthContext returns lightweight health information.
func (r *Reader) GetHealthContext(ctx context.Context) (*model.HealthInfo, error) {
	info := &model.HealthInfo{
		SchemaMatch: r.schemaOK,
	}

	// Get database size from INFO.
	infoStr, err := r.rdb.Info(ctx, "keyspace").Result()
	if err == nil {
		info.DatabaseSize = parseInfoDBSize(infoStr)
	}

	// Get metadata fields.
	meta, err := r.rdb.HGetAll(ctx, metaKey).Result()
	if err != nil {
		return nil, fmt.Errorf("get meta: %w", err)
	}

	if dt, ok := meta["dump_time"]; ok && dt != "" {
		if t, parseErr := time.Parse(time.RFC3339, dt); parseErr == nil {
			info.DumpTime = t
		}
	}
	if eid, ok := meta["last_event_id"]; ok && eid != "" {
		info.LastEventID = eid
		info.LastEventSync = eventIDToTime(eid)
	}
	if s, ok := meta["state"]; ok {
		info.State = s
	}

	return info, nil
}

// GetStats returns database statistics.
func (r *Reader) GetStats() (*model.DBStats, error) {
	return r.GetStatsContext(context.Background())
}

// GetStatsContext returns database statistics.
func (r *Reader) GetStatsContext(ctx context.Context) (*model.DBStats, error) {
	info, err := r.GetHealthContext(ctx)
	if err != nil {
		return nil, err
	}

	stats := &model.DBStats{HealthInfo: *info}

	meta, err := r.rdb.HGetAll(ctx, metaKey).Result()
	if err != nil {
		return nil, fmt.Errorf("get meta: %w", err)
	}
	if ec, ok := meta["entity_count"]; ok {
		stats.EntityCount, _ = strconv.ParseInt(ec, 10, 64)
	}

	// Get counts from data structures.
	stats.PendingCount, _ = r.rdb.SCard(ctx, pendingKey).Result()
	stats.ProcessingCount, _ = r.rdb.SCard(ctx, processingKey).Result()
	stats.FailedCount, _ = r.rdb.ZCard(ctx, failedZsetKey).Result()

	firstID, lastID, length, err := r.StreamInfo()
	if err == nil {
		stats.StreamLength = length
		stats.OldestEvent = streamIDToTime(firstID)
		stats.NewestEvent = streamIDToTime(lastID)
	}

	return stats, nil
}

// ScanEntities iterates entity fields in the entities hash for snapshot
// generation. Returns entities, the next cursor, and any error.
func (r *Reader) ScanEntities(ctx context.Context, cursor uint64, count int64) ([]SnapshotEntity, uint64, error) {
	// HSCAN returns alternating field, value pairs.
	fields, next, err := r.rdb.HScan(ctx, entitiesKey, cursor, "*", count).Result()
	if err != nil {
		return nil, 0, fmt.Errorf("scan entities: %w", err)
	}

	entities := make([]SnapshotEntity, 0, len(fields)/2)
	for i := 0; i+1 < len(fields); i += 2 {
		qid, err := strconv.ParseInt(fields[i], 10, 64)
		if err != nil {
			continue
		}
		entities = append(entities, SnapshotEntity{
			QID:         qid,
			RawMappings: fields[i+1],
		})
	}

	return entities, next, nil
}

// StreamRead reads entries from the changelog stream starting after since.
// Blocks up to the given duration waiting for new entries. Returns the
// events read and any error.
func (r *Reader) StreamRead(ctx context.Context, since string, count int64, block time.Duration) ([]ChangeEvent, error) {
	// Kvrocks returns stream entries as flat arrays, not maps.
	// Use raw command to get proper parsing.
	var rawResult interface{}
	var err error

	if block > 0 {
		// XREAD with BLOCK
		rawResult, err = r.rdb.Do(ctx, "XREAD", "COUNT", count, "BLOCK", block.Milliseconds(), "STREAMS", changelogKey, since).Result()
	} else {
		// XREAD without BLOCK
		rawResult, err = r.rdb.Do(ctx, "XREAD", "COUNT", count, "STREAMS", changelogKey, since).Result()
	}
	if err == redis.Nil {
		return nil, nil // timeout, no new entries
	}
	if err != nil {
		return nil, fmt.Errorf("xread: %w", err)
	}

	if rawResult == nil {
		return nil, nil
	}

	// XREAD returns: [[streamKey, [[id, [key1, val1, ...]], ...]]]
	// We need to extract the messages from the nested array
	streams, ok := rawResult.([]interface{})
	if !ok || len(streams) == 0 {
		return nil, nil
	}

	// Get the stream data (second element of first stream)
	streamData, ok := streams[0].([]interface{})
	if !ok || len(streamData) < 2 {
		return nil, nil
	}

	messages, ok := streamData[1].([]interface{})
	if !ok || len(messages) == 0 {
		return nil, nil
	}

	events := make([]ChangeEvent, 0, len(messages))
	for _, m := range messages {
		msg, ok := m.([]interface{})
		if !ok || len(msg) < 2 {
			continue
		}

		id, ok := msg[0].(string)
		if !ok {
			continue
		}

		// Parse the flat array of key-value pairs directly into event
		ev := ChangeEvent{ID: id}
		if vals, ok := msg[1].([]interface{}); ok {
			for i := 0; i < len(vals)-1; i += 2 {
				if k, ok := vals[i].(string); ok {
					switch k {
					case "q":
						if v, ok := vals[i+1].(string); ok {
							ev.QID, _ = strconv.ParseInt(v, 10, 64)
						}
					case "m":
						if v, ok := vals[i+1].(string); ok {
							ev.RawMappings = v
						}
					}
				}
			}
		}

		events = append(events, ev)
	}
	return events, nil
}

// StreamInfo returns the first entry ID, last entry ID, and length of the
// changelog stream. Returns empty strings and zero if the stream doesn't exist.
func (r *Reader) StreamInfo() (firstID, lastID string, length int64, err error) {
	ctx := context.Background()
	info, err := r.rdb.XInfoStream(ctx, changelogKey).Result()
	if err != nil {
		if isStreamNotFoundError(err) {
			return "", "", 0, nil
		}
		return "", "", 0, fmt.Errorf("xinfo stream: %w", err)
	}
	firstID = info.FirstEntry.ID
	lastID = info.LastEntry.ID

	if info.Length > 0 && firstID == "" {
		msgs, err := r.rdb.XRangeN(ctx, changelogKey, "-", "+", 1).Result()
		if err != nil {
			return "", "", 0, fmt.Errorf("xrange first entry: %w", err)
		}
		if len(msgs) > 0 {
			firstID = msgs[0].ID
		}
	}

	if info.Length > 0 && lastID == "" {
		msgs, err := r.rdb.XRevRangeN(ctx, changelogKey, "+", "-", 1).Result()
		if err != nil {
			return "", "", 0, fmt.Errorf("xrevrange last entry: %w", err)
		}
		if len(msgs) > 0 {
			lastID = msgs[0].ID
		}
	}

	return firstID, lastID, info.Length, nil
}

// Close is a no-op for Reader; the underlying Client manages the connection.
func (r *Reader) Close() error {
	return nil
}

// eventIDToTime extracts the earliest Kafka timestamp from an SSE event ID.
func eventIDToTime(eventID string) time.Time {
	var offsets []struct {
		Timestamp *int64 `json:"timestamp"`
	}
	if json.Unmarshal([]byte(eventID), &offsets) != nil {
		return time.Time{}
	}
	var minTS int64
	for _, o := range offsets {
		if o.Timestamp == nil || *o.Timestamp <= 0 {
			continue
		}
		if minTS == 0 || *o.Timestamp < minTS {
			minTS = *o.Timestamp
		}
	}
	if minTS == 0 {
		return time.Time{}
	}
	return time.UnixMilli(minTS)
}

func streamIDToTime(streamID string) time.Time {
	parts := strings.SplitN(streamID, "-", 2)
	if len(parts) == 0 || parts[0] == "" {
		return time.Time{}
	}

	ms, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil || ms <= 0 {
		return time.Time{}
	}

	return time.UnixMilli(ms)
}

// parseInfoDBSize attempts to parse database size from INFO output.
// Looks for db0:keys=N,... line and uses DBSIZE as approximation.
func parseInfoDBSize(info string) int64 {
	// Kvrocks reports used_db_size (RocksDB disk usage) in the Keyspace section.
	for _, line := range strings.Split(info, "\n") {
		line = strings.TrimRight(line, "\r")
		if strings.HasPrefix(line, "used_db_size:") {
			n, _ := strconv.ParseInt(line[len("used_db_size:"):], 10, 64)
			return n
		}
	}
	return 0
}

// isStreamNotFoundError checks if the error indicates the stream doesn't exist.
func isStreamNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	msg := err.Error()
	return strings.Contains(msg, "ERR no such key") ||
		strings.Contains(msg, "ERR no longer exist")
}
