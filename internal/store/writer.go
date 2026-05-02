package store

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

const (
	changelogKey  = "changelog"
	entitiesKey   = "entities"
	metaKey       = "meta"
	pendingKey    = "pending"
	processingKey = "processing"
	failedZsetKey = "failed_zset"
)

// Writer provides read-write access to Kvrocks (used by the primary and replica).
type Writer struct {
	rdb           *redis.Client
	scriptsMu     sync.Mutex
	scriptsLoaded atomic.Bool
}

// Lua scripts executed atomically on the server.
//
// upsertScript: KEYS = [changelog], ARGV = [new_m, eid]
//
//	Reads old entity from the "entities" hash, compares mappings string.
//	If unchanged, returns 0. If changed, rewrites indexes, appends to the
//	changelog, and updates counters.
//	Mappings are stored as a flat JSON array of "P<id>:<value>" strings.
var upsertScript = redis.NewScript(`
local old_m = redis.call('HGET', 'entities', ARGV[2])
local new_m = ARGV[1]
local eid   = ARGV[2]

local is_new = not old_m
local changed = is_new or (old_m ~= new_m)

if not changed then
  return 0
end

redis.call('HSET', 'entities', eid, new_m)

-- Parse a "P<id>:<value>" entry into the property key and value.
local function parse(e)
  local colon = string.find(e, ':', 2)
  return 'p:' .. string.sub(e, 2, colon - 1), string.sub(e, colon + 1)
end

-- Insert eid into a comma-separated sorted list stored at hash key pk, field val.
local function add_idx(e)
  local pk, val = parse(e)
  local cur = redis.call('HGET', pk, val)
  if not cur then
    redis.call('HSET', pk, val, eid)
    return
  end
  local parts = {}
  local found = false
  for s in string.gmatch(cur, '[^,]+') do
    if s == eid then found = true end
    parts[#parts + 1] = s
  end
  if found then return end
  parts[#parts + 1] = eid
  table.sort(parts)
  redis.call('HSET', pk, val, table.concat(parts, ','))
end

-- Remove eid from a comma-separated sorted list stored at hash key pk, field val.
local function del_idx(e)
  local pk, val = parse(e)
  local cur = redis.call('HGET', pk, val)
  if not cur then return end
  local parts = {}
  for s in string.gmatch(cur, '[^,]+') do
    if s ~= eid then parts[#parts + 1] = s end
  end
  if #parts == 0 then
    redis.call('HDEL', pk, val)
  else
    redis.call('HSET', pk, val, table.concat(parts, ','))
  end
end

local new_parsed = cjson.decode(new_m)

-- Diff old and new arrays via set difference.
if is_new then
  for _, entry in ipairs(new_parsed) do add_idx(entry) end
else
  local old_parsed = cjson.decode(old_m)
  local old_set = {}
  for _, e in ipairs(old_parsed) do old_set[e] = true end
  local new_set = {}
  for _, e in ipairs(new_parsed) do new_set[e] = true end
  for _, e in ipairs(old_parsed) do
    if not new_set[e] then del_idx(e) end
  end
  for _, e in ipairs(new_parsed) do
    if not old_set[e] then add_idx(e) end
  end
end

redis.call('XADD', KEYS[1], '*', 'q', eid, 'm', new_m)

if is_new then
  redis.call('HINCRBY', 'meta', 'entity_count', 1)
end

return 1
`)

// deleteScript: KEYS = [changelog], ARGV = [eid]
//
//	Reads entity from the "entities" hash, removes indexes, appends
//	delete to changelog, updates counters. Returns 0 if entity was
//	already gone.
var deleteScript = redis.NewScript(`
local eid = ARGV[1]
local old_m = redis.call('HGET', 'entities', eid)
if not old_m then return 0 end

redis.call('HDEL', 'entities', eid)

local old = cjson.decode(old_m)
for _, entry in ipairs(old) do
  local colon = string.find(entry, ':', 2)
  local prop = string.sub(entry, 2, colon - 1)
  local val  = string.sub(entry, colon + 1)
  local pk = 'p:' .. prop
  local cur = redis.call('HGET', pk, val)
  if cur then
    local parts = {}
    for s in string.gmatch(cur, '[^,]+') do
      if s ~= eid then parts[#parts + 1] = s end
    end
    if #parts == 0 then
      redis.call('HDEL', pk, val)
    else
      redis.call('HSET', pk, val, table.concat(parts, ','))
    end
  end
end

redis.call('XADD', KEYS[1], '*', 'q', eid, 'm', '')

redis.call('HINCRBY', 'meta', 'entity_count', -1)

return 1
`)

// NewWriter creates a Writer that connects to Kvrocks via the given Client.
func NewWriter(c *Client) *Writer {
	return &Writer{rdb: c.Redis()}
}

// loadScripts ensures all Lua scripts are loaded into the server cache.
// This is necessary before using scripts in pipelines, which cannot
// fallback from EVALSHA to EVAL automatically.
// Scripts are loaded once and retried on subsequent calls if loading fails.
func (w *Writer) loadScripts(ctx context.Context) error {
	if w.scriptsLoaded.Load() {
		return nil
	}
	w.scriptsMu.Lock()
	defer w.scriptsMu.Unlock()
	if w.scriptsLoaded.Load() {
		return nil
	}
	for _, s := range []*redis.Script{upsertScript, deleteScript, recordFailedScript} {
		if err := s.Load(ctx, w.rdb).Err(); err != nil {
			return fmt.Errorf("load lua script: %w", err)
		}
	}
	w.scriptsLoaded.Store(true)
	return nil
}

// EntityRecord holds the data for a single entity upsert within a batch.
// Mappings is a flat array of "P<id>:<value>" strings.
type EntityRecord struct {
	WikidataID string
	Mappings   []string
}

// RawEntityRecord holds a single entity with canonical mappings JSON already
// encoded and ready to persist.
type RawEntityRecord struct {
	WikidataID  string
	RawMappings string
}

// scriptKeys returns the KEYS shared by upsert/delete Lua scripts.
func scriptKeys() []string {
	return []string{changelogKey}
}

// Pipe wraps a Redis pipeline with store-level operations using a sticky
// error pattern. All methods silently no-op after the first error.
// Call Exec to flush the pipeline and return the first error, if any.
type Pipe struct {
	ctx  context.Context
	pipe redis.Pipeliner
	w    *Writer
	err  error
}

// NewPipe creates a Pipe that queues store operations into a single Redis
// pipeline round-trip. The caller must call Exec to flush.
// If script loading fails, the error is captured in the Pipe and returned
// by Exec, so callers never need to check NewPipe for errors.
func (w *Writer) NewPipe(ctx context.Context) *Pipe {
	p := &Pipe{ctx: ctx, w: w}
	if err := w.loadScripts(ctx); err != nil {
		p.err = err
		return p
	}
	p.pipe = w.rdb.Pipeline()
	return p
}

// UpsertRawEntity queues an upsert of an entity with pre-encoded mappings JSON.
func (p *Pipe) UpsertRawEntity(rec RawEntityRecord) {
	if p.err != nil {
		return
	}
	id, err := qidToInt(rec.WikidataID)
	if err != nil {
		p.err = fmt.Errorf("convert QID %s: %w", rec.WikidataID, err)
		return
	}
	if rec.RawMappings == "" {
		p.err = fmt.Errorf("raw mappings empty for %s", rec.WikidataID)
		return
	}
	eid := strconv.FormatInt(id, 10)
	upsertScript.Run(p.ctx, p.pipe, scriptKeys(), rec.RawMappings, eid)
}

// UpsertEntity queues an upsert of an entity, encoding its mappings to
// canonical JSON.
func (p *Pipe) UpsertEntity(rec EntityRecord) {
	if p.err != nil {
		return
	}
	id, err := qidToInt(rec.WikidataID)
	if err != nil {
		p.err = fmt.Errorf("convert QID %s: %w", rec.WikidataID, err)
		return
	}
	for _, entry := range rec.Mappings {
		if _, _, err := parseFlatMappingEntry(entry); err != nil {
			p.err = fmt.Errorf("invalid mapping for %s: %q: %w", rec.WikidataID, entry, err)
			return
		}
	}
	mJSON := canonicalMappingsJSON(rec.Mappings)
	eid := strconv.FormatInt(id, 10)
	upsertScript.Run(p.ctx, p.pipe, scriptKeys(), mJSON, eid)
}

// DeleteEntity queues a delete of an entity by QID string (e.g. "Q123").
func (p *Pipe) DeleteEntity(qid string) {
	if p.err != nil {
		return
	}
	id, err := qidToInt(qid)
	if err != nil {
		p.err = err
		return
	}
	eid := strconv.FormatInt(id, 10)
	deleteScript.Run(p.ctx, p.pipe, scriptKeys(), eid)
}

// SetSyncState queues an HSET on the meta hash.
func (p *Pipe) SetSyncState(key, value string) {
	if p.err != nil {
		return
	}
	p.pipe.HSet(p.ctx, metaKey, key, value)
}

// DelSyncStates queues an HDEL on the meta hash for the given keys.
func (p *Pipe) DelSyncStates(keys ...string) {
	if p.err != nil {
		return
	}
	p.pipe.HDel(p.ctx, metaKey, keys...)
}

// EnqueueEntities queues an SADD of QIDs into the pending set.
func (p *Pipe) EnqueueEntities(qids []string) {
	if p.err != nil {
		return
	}
	if len(qids) == 0 {
		return
	}
	members := make([]interface{}, 0, len(qids))
	for _, qid := range qids {
		id, err := qidToInt(qid)
		if err != nil {
			p.err = err
			return
		}
		members = append(members, strconv.FormatInt(id, 10))
	}
	p.pipe.SAdd(p.ctx, pendingKey, members...)
}

// AckProcessedEntities queues an SREM of QIDs from the processing set.
func (p *Pipe) AckProcessedEntities(qids []string) {
	if p.err != nil {
		return
	}
	if len(qids) == 0 {
		return
	}
	members := make([]interface{}, 0, len(qids))
	for _, qid := range qids {
		id, err := qidToInt(qid)
		if err != nil {
			p.err = err
			return
		}
		members = append(members, strconv.FormatInt(id, 10))
	}
	p.pipe.SRem(p.ctx, processingKey, members...)
}

// RecordFailedEntity queues a Lua script that records a processing failure.
func (p *Pipe) RecordFailedEntity(wikidataID, errMsg string) {
	if p.err != nil {
		return
	}
	id, err := qidToInt(wikidataID)
	if err != nil {
		p.err = err
		return
	}
	now := time.Now().UTC()
	idStr := strconv.FormatInt(id, 10)
	failKey := fmt.Sprintf("failed:%d", id)
	recordFailedScript.Run(p.ctx, p.pipe,
		[]string{failedZsetKey, failKey},
		idStr, errMsg, now.Format(time.RFC3339), now.UnixMicro(),
	)
}

// DeleteFailedEntity queues removal of a failure record (ZREM + DEL).
func (p *Pipe) DeleteFailedEntity(wikidataID string) {
	if p.err != nil {
		return
	}
	id, err := qidToInt(wikidataID)
	if err != nil {
		p.err = err
		return
	}
	p.pipe.ZRem(p.ctx, failedZsetKey, strconv.FormatInt(id, 10))
	p.pipe.Del(p.ctx, fmt.Sprintf("failed:%d", id))
}

// Exec flushes the pipeline and returns the first error encountered during
// queueing or execution.
func (p *Pipe) Exec() error {
	if p.err != nil {
		return p.err
	}
	cmds, err := p.pipe.Exec(p.ctx)
	if err != nil && err != redis.Nil {
		for _, cmd := range cmds {
			if cmd.Err() != nil && cmd.Err() != redis.Nil {
				return cmd.Err()
			}
		}
		return err
	}
	return nil
}

// recordFailedScript atomically records a failure: creates or updates the
// failed:<id> hash and adds the entity to the failed ZSET.
// KEYS[1] = failedZsetKey, KEYS[2] = failed:<id>
// ARGV[1] = id (string), ARGV[2] = errMsg, ARGV[3] = now (RFC3339), ARGV[4] = unix timestamp
var recordFailedScript = redis.NewScript(`
local exists = redis.call('EXISTS', KEYS[2])
if exists == 1 then
  redis.call('HSET', KEYS[2], 'error', ARGV[2], 'last_failed', ARGV[3])
  redis.call('HINCRBY', KEYS[2], 'attempts', 1)
else
  redis.call('HSET', KEYS[2], 'error', ARGV[2], 'attempts', 1, 'first_failed', ARGV[3], 'last_failed', ARGV[3])
end
redis.call('ZADD', KEYS[1], ARGV[4], ARGV[1])
return 1
`)

// FlushData drops all data and re-initialises the schema version.
func (w *Writer) FlushData() error {
	return w.FlushDataContext(context.Background())
}

// FlushDataContext drops all data and re-initialises the schema version
// using the provided context.
func (w *Writer) FlushDataContext(ctx context.Context) error {
	if err := w.rdb.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flush database: %w", err)
	}
	return w.rdb.HSet(ctx, metaKey, "schema_version", SchemaVersion()).Err()
}

const (
	// DefaultChangelogRetention is how long changelog entries are kept.
	DefaultChangelogRetention = 168 * time.Hour // 7 days

	// trimBatchSize bounds a single XRANGE+XTRIM cycle. It must be large
	// enough that the trimmer can keep up with seed-time write rates (the
	// seed upsert path XADDs to the changelog for every entity, typically
	// well over 1k/s), but small enough that the XRANGE response and per-
	// call XTRIM work stay modest. Entries are small (QID + mappings JSON),
	// so 10k per cycle is a few MB on the wire.
	trimBatchSize = 10000
	trimInterval  = 5 * time.Minute
	// trimBacklogDelay is a short yield between cycles while a backlog is
	// being drained. It exists only to keep the select loop responsive to
	// ctx.Done(); we do not want to rate-limit draining itself, since the
	// write side (especially during seeding with CHANGELOG_RETENTION=0)
	// can easily outpace a once-per-second trimmer.
	trimBacklogDelay = 10 * time.Millisecond
	trimErrorDelay   = 5 * time.Minute
)

// RunChangelogTrimmer periodically trims the changelog stream, removing
// entries older than the retention period. It adapts its tick rate: when a
// full batch is removed (backlog), the next tick fires quickly; otherwise
// it waits trimInterval. On errors it backs off before retrying.
func RunChangelogTrimmer(ctx context.Context, w *Writer, retention time.Duration) {
	slog.Info("trimmer: starting", "retention", retention, "interval", trimInterval)
	timer := time.NewTimer(0) // fire immediately on start
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			cutoff := time.Now().Add(-retention)
			minID := fmt.Sprintf("%d-0", cutoff.UnixMilli())
			n, err := w.StreamTrimOlderThan(ctx, minID, trimBatchSize)
			if err != nil {
				slog.Error("trimmer: trim failed", "error", err)
				timer.Reset(trimErrorDelay)
			} else if n >= trimBatchSize {
				slog.Info("trimmer: trimmed old entries (backlog)", "removed", n, "retention", retention)
				timer.Reset(trimBacklogDelay)
			} else {
				if n > 0 {
					slog.Info("trimmer: trimmed old entries", "removed", n, "retention", retention)
				}
				timer.Reset(trimInterval)
			}
		}
	}
}

// TrimChangelog removes all entries from the changelog stream.
func (w *Writer) TrimChangelog() error {
	ctx := context.Background()
	return w.rdb.Del(ctx, changelogKey).Err()
}

// StreamTrimOlderThan removes up to limit changelog entries with IDs strictly
// less than minID. It uses XRANGE to find a bounded batch of old entries,
// then XTRIM MINID to remove them from the head. This avoids passing a
// far-in-the-past cutoff directly to XTRIM MINID, which could try to delete
// millions of entries at once and OOM the server.
// Returns the number of entries removed.
func (w *Writer) StreamTrimOlderThan(ctx context.Context, minID string, limit int64) (int64, error) {
	old, err := w.rdb.XRangeN(ctx, changelogKey, "-", "("+minID, limit+1).Result()
	if err != nil {
		return 0, fmt.Errorf("xrange: %w", err)
	}
	if len(old) == 0 {
		return 0, nil
	}

	// Use the last returned entry's ID as the MINID cutoff. XTRIM MINID
	// keeps entries with ID >= minID, so this entry itself survives and
	// will be cleaned up on the next tick.
	trimID := old[len(old)-1].ID
	return w.rdb.XTrimMinID(ctx, changelogKey, trimID).Result()
}

// claimScript atomically pops up to ARGV[1] members from KEYS[1] (pending),
// adds them to KEYS[2] (processing), and returns them.
var claimScript = redis.NewScript(`
local members = redis.call('SPOP', KEYS[1], ARGV[1])
if #members == 0 then return {} end
for _, m in ipairs(members) do
  redis.call('SADD', KEYS[2], m)
end
return members
`)

// ClaimPendingBatch atomically moves up to limit QIDs from the pending set
// into the processing set and returns them. This is crash-safe: on recovery,
// anything left in processing can be moved back to pending.
func (w *Writer) ClaimPendingBatch(limit int) ([]string, error) {
	ctx := context.Background()
	result, err := claimScript.Run(ctx, w.rdb, []string{pendingKey, processingKey}, limit).StringSlice()
	if err != nil && err != redis.Nil {
		return nil, err
	}
	qids := make([]string, 0, len(result))
	for _, m := range result {
		id, err := strconv.ParseInt(m, 10, 64)
		if err != nil {
			continue
		}
		qids = append(qids, intToQid(id))
	}
	return qids, nil
}

// RecoverProcessing moves any QIDs left in the processing set back to
// pending. This must be called at startup to recover from a previous crash.
func (w *Writer) RecoverProcessing() error {
	ctx := context.Background()
	// SUNIONSTORE pending pending processing → merges processing into pending.
	if err := w.rdb.SUnionStore(ctx, pendingKey, pendingKey, processingKey).Err(); err != nil {
		return fmt.Errorf("recover processing set: %w", err)
	}
	return w.rdb.Del(ctx, processingKey).Err()
}

// Close is a no-op for Writer; the underlying Client manages the connection.
func (w *Writer) Close() error {
	return nil
}

// MigrateSchema checks the stored schema version and flushes all data if it
// does not match the current SchemaVersion().
func (w *Writer) MigrateSchema() error {
	ctx := context.Background()
	expected := SchemaVersion()

	stored, err := w.rdb.HGet(ctx, metaKey, "schema_version").Result()
	if err != nil && err != redis.Nil {
		return fmt.Errorf("read schema version: %w", err)
	}

	if stored == expected {
		slog.Debug("schema version already up to date", "version", expected)
		return nil
	}

	if err == redis.Nil {
		slog.Info("schema version missing; initializing metadata", "expected", expected)
	} else {
		slog.Info("schema version mismatch", "expected", expected, "found", stored)
	}

	slog.Info("schema migration: flushing database")
	if err := w.rdb.FlushDB(ctx).Err(); err != nil {
		return fmt.Errorf("flush database: %w", err)
	}

	return w.rdb.HSet(ctx, metaKey, "schema_version", expected).Err()
}

// propKey returns the Kvrocks hash key for a property reverse-index.
func propKey(property int) string {
	return fmt.Sprintf("p:%d", property)
}

// canonicalMappingsJSON produces a deterministic JSON encoding of mappings
// as a flat array of "P<id>:<value>" strings (e.g. ["P345:tt0111161"]).
// The input is expected to already be in canonical order (sorted by numeric
// property, rank, then value) as produced by extractExternalIDs.
// Byte-level string comparison in Lua detects no-ops reliably.
func canonicalMappingsJSON(entries []string) string {
	if len(entries) == 0 {
		return "[]"
	}
	data, _ := json.Marshal(entries)
	return string(data)
}

func parseFlatMappingEntry(entry string) (string, string, error) {
	if !strings.HasPrefix(entry, "P") {
		return "", "", fmt.Errorf("must start with P")
	}

	colonIdx := strings.IndexByte(entry, ':')
	if colonIdx < 2 || colonIdx == len(entry)-1 {
		return "", "", fmt.Errorf("must be in P<id>:<value> format")
	}

	prop := entry[1:colonIdx]
	if _, err := strconv.Atoi(prop); err != nil {
		return "", "", fmt.Errorf("invalid property %q", prop)
	}

	val := entry[colonIdx+1:]
	return prop, val, nil
}

// qidToInt converts a Wikidata QID string (e.g., "Q172241") to an integer.
func qidToInt(qid string) (int64, error) {
	if len(qid) == 0 || (qid[0] != 'Q' && qid[0] != 'q') {
		return 0, fmt.Errorf("invalid QID format: %s", qid)
	}
	n, err := strconv.ParseInt(qid[1:], 10, 64)
	if err != nil {
		return 0, fmt.Errorf("parse QID number: %w", err)
	}
	return n, nil
}

// intToQid converts an integer to a Wikidata QID string.
func intToQid(n int64) string {
	return fmt.Sprintf("Q%d", n)
}
