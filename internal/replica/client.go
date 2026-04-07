// Package replica implements the replica sync client that fetches snapshots
// and streams changes from an upstream replicator, applying them to a local
// Kvrocks instance.
package replica

import (
	"bufio"
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"

	"github.com/goccy/go-json"
	"github.com/redis/go-redis/v9"

	"github.com/ekeid/ekeid/internal/replicate"
	"github.com/ekeid/ekeid/internal/store"
)

const (
	retryBaseDelay = 1 * time.Second
	retryMaxDelay  = 60 * time.Second
)

// Client implements the replica state machine that syncs from an upstream
// replicator into a local Kvrocks instance.
type Client struct {
	writer      *store.Writer
	rdb         *redis.Client
	upstreamURL string
	httpClient  *http.Client
}

// NewClient creates a new replica client.
func NewClient(writer *store.Writer, rdb *redis.Client, upstreamURL string) *Client {
	return &Client{
		writer:      writer,
		rdb:         rdb,
		upstreamURL: strings.TrimRight(upstreamURL, "/"),
		httpClient: &http.Client{
			Timeout: 0, // No timeout for streaming
		},
	}
}

// Run executes the replica state machine until ctx is cancelled.
func (c *Client) Run(ctx context.Context) error {
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		lastID, err := c.writer.GetSyncState("last_replicated_id")
		if err != nil {
			return fmt.Errorf("get last_replicated_id: %w", err)
		}

		if lastID == "" {
			// First run — need snapshot.
			slog.Info("replica: no last_replicated_id, fetching snapshot")
			snapshotID, err := c.fetchSnapshot(ctx)
			if err != nil {
				if ctx.Err() != nil {
					return ctx.Err()
				}
				slog.Error("replica: snapshot failed, retrying", "error", err)
				sleepWithContext(ctx, 10*time.Second)
				continue
			}
			lastID = snapshotID
		}

		slog.Info("replica: connecting stream", "since", lastID)
		err = c.connectStream(ctx, lastID)
		if err != nil {
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("replica: stream error, retrying", "error", err)
			sleepWithContext(ctx, retryBaseDelay)
			continue
		}
	}
}

// fetchSnapshot downloads the snapshot from upstream, applies it, and returns
// the snapshot's stream ID. The snapshot stream contains checkpoint control
// lines at zstd frame boundaries and a final done control line. Resume uses
// standard HTTP Range and If-Range over the raw compressed bytes.
func (c *Client) fetchSnapshot(ctx context.Context) (string, error) {
	url := c.upstreamURL + "/replicate/snapshot"

	// Check for in-progress snapshot state from a previous attempt.
	prevETag, _ := c.writer.GetSyncState("snapshot_etag")
	prevOffset, _ := c.writer.GetSyncState("snapshot_resume_offset")
	prevEntities, _ := c.writer.GetSyncState("snapshot_resume_entities")
	if prevEntities == "" {
		prevEntities, _ = c.writer.GetSyncState("snapshot_entities_applied")
	}

	var resumeOffset int64
	if prevOffset != "" {
		resumeOffset, _ = strconv.ParseInt(prevOffset, 10, 64)
	}
	var resumeEntities int64
	if prevEntities != "" {
		resumeEntities, _ = strconv.ParseInt(prevEntities, 10, 64)
	}

	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return "", fmt.Errorf("create request: %w", err)
	}

	// If we have a previous checkpoint, request resume from that compressed
	// byte offset. The checkpoint also records the entity count before the
	// frame so the client can validate progress after reconnect.
	resumeRequested := prevETag != "" && resumeOffset > 0 && prevEntities != ""
	if resumeRequested {
		req.Header.Set("Range", fmt.Sprintf("bytes=%d-", resumeOffset))
		req.Header.Set("If-Range", prevETag)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return "", fmt.Errorf("fetch snapshot: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusServiceUnavailable {
		return "", fmt.Errorf("upstream not ready (503)")
	}
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusPartialContent {
		return "", fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	etag := resp.Header.Get("ETag")
	if etag == "" {
		return "", fmt.Errorf("missing ETag header")
	}

	var baseEntities int64
	if resumeRequested && resp.StatusCode == http.StatusPartialContent {
		baseEntities = resumeEntities
		slog.Info("replica: resuming from snapshot checkpoint", "offset", resumeOffset, "base_entities", baseEntities)
	} else {
		// Full file — flush and start over.
		if err := c.rdb.FlushDB(ctx).Err(); err != nil {
			return "", fmt.Errorf("flush local DB: %w", err)
		}
		// Set schema_version immediately so MigrateSchema won't flush
		// our progress state on restart.
		if err := c.writer.SetSyncState("schema_version", store.SchemaVersion()); err != nil {
			return "", fmt.Errorf("set schema version after flush: %w", err)
		}
		resumeOffset = 0
		baseEntities = 0
		slog.Info("replica: starting fresh snapshot", "etag", etag)
	}

	// Persist sync state for potential future resume.
	_ = c.writer.SetSyncState("snapshot_etag", etag)
	_ = c.writer.SetSyncState("snapshot_resume_offset", strconv.FormatInt(resumeOffset, 10))
	_ = c.writer.SetSyncState("snapshot_resume_entities", strconv.FormatInt(baseEntities, 10))

	zr, err := zstd.NewReader(resp.Body)
	if err != nil {
		return "", fmt.Errorf("open zstd: %w", err)
	}
	defer zr.Close()

	scanner := bufio.NewScanner(zr)
	scanner.Buffer(make([]byte, 1024*1024), 4*1024*1024)

	var appliedThisRun int64
	var lineNumber int64
	var seenDone bool
	var snapshotID string

	for scanner.Scan() {
		if seenDone {
			return "", fmt.Errorf("snapshot contains data after done control line")
		}

		line := scanner.Bytes()
		lineNumber++

		switch replicate.ClassifySnapshotLine(line) {
		case replicate.SnapshotLineTypeEntity:
			qid, rawMappings, err := replicate.ParseSnapshotEntityLine(line)
			if err != nil {
				return "", fmt.Errorf("parse snapshot entity line %d: %w", lineNumber, err)
			}
			if err := c.applySnapshotEntity(ctx, qid, rawMappings); err != nil {
				return "", fmt.Errorf("apply entity Q%d: %w", qid, err)
			}
			appliedThisRun++

		case replicate.SnapshotLineTypeControl:
			control, err := replicate.ParseSnapshotControl(line)
			if err != nil {
				return "", fmt.Errorf("parse snapshot control line %d: %w", lineNumber, err)
			}

			switch control.Type {
			case replicate.SnapshotControlTypeCheckpoint:
				checkpoint, err := replicate.ParseSnapshotCheckpoint(line)
				if err != nil {
					return "", fmt.Errorf("parse snapshot checkpoint line %d: %w", lineNumber, err)
				}
				totalApplied := baseEntities + appliedThisRun
				if totalApplied != checkpoint.EntitiesBefore {
					return "", fmt.Errorf("snapshot checkpoint mismatch at offset %d: have %d entities, want %d", checkpoint.Offset, totalApplied, checkpoint.EntitiesBefore)
				}
				_ = c.writer.SetSyncState("snapshot_resume_offset", strconv.FormatInt(checkpoint.Offset, 10))
				_ = c.writer.SetSyncState("snapshot_resume_entities", strconv.FormatInt(checkpoint.EntitiesBefore, 10))
				slog.Info("replica: snapshot checkpoint", "offset", checkpoint.Offset, "entities_before", checkpoint.EntitiesBefore)

			case replicate.SnapshotControlTypeDone:
				done, err := replicate.ParseSnapshotDone(line)
				if err != nil {
					return "", fmt.Errorf("parse snapshot done line %d: %w", lineNumber, err)
				}
				totalApplied := baseEntities + appliedThisRun
				if totalApplied != done.Entities {
					return "", fmt.Errorf("snapshot done mismatch: have %d entities, want %d", totalApplied, done.Entities)
				}
				snapshotID = done.StreamID
				seenDone = true

			default:
				return "", fmt.Errorf("unknown snapshot control type %q", control.Type)
			}

		default:
			return "", fmt.Errorf("invalid snapshot line %d", lineNumber)
		}
	}
	if err := scanner.Err(); err != nil {
		return "", fmt.Errorf("scan snapshot: %w", err)
	}
	if !seenDone {
		return "", fmt.Errorf("snapshot missing done control line")
	}

	// Snapshot fully applied — set final metadata and clear progress state.
	totalEntities := baseEntities + appliedThisRun

	rdbCtx := ctx
	pipe := c.rdb.Pipeline()
	pipe.HSet(rdbCtx, "meta", "last_replicated_id", snapshotID)
	pipe.HSet(rdbCtx, "meta", "state", "streaming")
	pipe.HSet(rdbCtx, "meta", "schema_version", store.SchemaVersion())
	pipe.HSet(rdbCtx, "meta", "entity_count", strconv.FormatInt(totalEntities, 10))
	// Clear snapshot progress keys.
	pipe.HDel(rdbCtx, "meta", "snapshot_etag", "snapshot_resume_offset", "snapshot_resume_entities", "snapshot_entities_applied", "snapshot_entities_per_frame")
	if _, err := pipe.Exec(rdbCtx); err != nil {
		return "", fmt.Errorf("set metadata: %w", err)
	}

	slog.Info("replica: snapshot applied", "total_entities", totalEntities, "new_entities", appliedThisRun, "stream_id", snapshotID)
	return snapshotID, nil
}

// applySnapshotEntity writes a single entity from the snapshot.
func (c *Client) applySnapshotEntity(ctx context.Context, qid int64, rawMappings string) error {
	return c.writer.UpsertRawEntitiesBatchContext(ctx, []store.RawEntityRecord{{
		WikidataID:  fmt.Sprintf("Q%d", qid),
		RawMappings: rawMappings,
	}})
}

// connectStream connects to the upstream SSE stream and applies changes.
func (c *Client) connectStream(ctx context.Context, since string) error {
	url := fmt.Sprintf("%s/replicate/stream?since=%s", c.upstreamURL, since)
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect stream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("unexpected status: %d", resp.StatusCode)
	}

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 64*1024), 1024*1024)

	var eventType string
	for scanner.Scan() {
		line := scanner.Text()

		if strings.HasPrefix(line, "event: ") {
			eventType = strings.TrimPrefix(line, "event: ")
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			data := strings.TrimPrefix(line, "data: ")

			switch eventType {
			case "reset":
				// Parse reset data for upstream state info.
				var resetInfo map[string]string
				_ = json.Unmarshal([]byte(data), &resetInfo)
				upstreamState := resetInfo["state"]
				reason := resetInfo["reason"]
				slog.Info("replica: received reset", "reason", reason, "upstream_state", upstreamState)

				// Flush and clear state so next iteration fetches snapshot.
				if err := c.rdb.FlushDB(ctx).Err(); err != nil {
					return fmt.Errorf("flush local DB: %w", err)
				}
				if err := c.writer.SetSyncStateContext(ctx, "schema_version", store.SchemaVersion()); err != nil {
					return fmt.Errorf("set schema version: %w", err)
				}
				return nil

			case "change":
				id, qid, rawMappings, err := replicate.ParseStreamChangeData(data)
				if err != nil {
					slog.Warn("replica: skip malformed event", "error", err)
					continue
				}

				if err := c.applyChange(ctx, qid, rawMappings); err != nil {
					return fmt.Errorf("apply change %s: %w", id, err)
				}

				if err := c.writer.SetSyncState("last_replicated_id", id); err != nil {
					return fmt.Errorf("update last_replicated_id: %w", err)
				}
			}

			eventType = ""
			continue
		}
		// Ignore keepalive comments and empty lines.
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("scan stream: %w", err)
	}
	return fmt.Errorf("stream ended unexpectedly")
}

// applyChange applies a single change event using atomic Lua scripts.
// If rawMappings is non-empty the event is an upsert; otherwise it is a delete.
func (c *Client) applyChange(ctx context.Context, qid int64, rawMappings string) error {
	qidStr := fmt.Sprintf("Q%d", qid)
	if rawMappings != "" {
		return c.writer.UpsertRawEntitiesBatchContext(ctx, []store.RawEntityRecord{{
			WikidataID:  qidStr,
			RawMappings: rawMappings,
		}})
	}
	return c.writer.DeleteEntitiesBatchContext(ctx, []string{qidStr})
}

func sleepWithContext(ctx context.Context, d time.Duration) {
	select {
	case <-time.After(d):
	case <-ctx.Done():
	}
}
