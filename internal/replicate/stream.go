package replicate

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// ServeStream handles GET /replicate/stream?since={id} as an SSE endpoint.
// It replays changelog entries after `since` and then blocks for live changes.
// If coverage for `since` cannot be proven (no retained entries, or `since`
// older than the oldest retained entry), it sends an "event: reset" and
// closes the connection.
func ServeStream(reader *store.Reader, w http.ResponseWriter, r *http.Request) {
	since := r.URL.Query().Get("since")
	if since == "" {
		since = "0"
	}

	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "streaming not supported", http.StatusInternalServerError)
		return
	}

	// Check if since is too old (before oldest retained entry).
	if err := checkStreamGap(reader, since); err != nil {
		// If the backing store is unreachable, return 503 so the replica
		// retries without purging its data. Only send a reset for genuine
		// coverage gaps.
		if errors.Is(err, errStoreUnavailable) {
			slog.Warn("stream: store unavailable, returning 503", "error", err)
			http.Error(w, "store unavailable", http.StatusServiceUnavailable)
			return
		}

		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")
		w.WriteHeader(http.StatusOK)

		resetData := map[string]string{"reason": err.Error()}
		if stats, sErr := reader.GetStats(); sErr == nil {
			resetData["state"] = stats.State
		}
		resetJSON, _ := json.Marshal(resetData)
		fmt.Fprintf(w, "event: reset\ndata: %s\n\n", resetJSON)
		flusher.Flush()
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.WriteHeader(http.StatusOK)
	flusher.Flush()

	ctx := r.Context()
	cursor := since
	deadline := time.After(MaxConnectionDuration)

	for {
		select {
		case <-ctx.Done():
			return
		case <-deadline:
			// Force reconnect, like Wikimedia EventStreams.
			return
		default:
		}

		events, err := reader.StreamRead(ctx, cursor, StreamReadCount, KeepAliveInterval)
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			slog.Error("stream: read error", "error", err)
			return
		}

		if len(events) == 0 {
			// Timeout with no events — send keepalive.
			fmt.Fprint(w, ": keepalive\n\n")
			flusher.Flush()
			continue
		}

		for _, ev := range events {
			fmt.Fprintf(w, "event: change\ndata: %s\n\n", FormatStreamChangeData(ev.ID, ev.QID, ev.RawMappings))
			cursor = ev.ID
		}
		flusher.Flush()
	}
}

// errStoreUnavailable is returned by checkStreamGap when the backing store
// cannot be reached (e.g. kvrocks is restarting). ServeStream uses this to
// respond with 503 instead of sending a destructive reset event.
var errStoreUnavailable = fmt.Errorf("store unavailable")

// checkStreamGap returns an error unless the changelog retention can prove
// coverage for `since`. This forces reset when the stream is empty/missing,
// and when `since` is older than the oldest retained entry.
// It returns errStoreUnavailable when the backing store is unreachable, which
// callers should treat differently from a genuine coverage gap.
func checkStreamGap(reader *store.Reader, since string) error {
	if since == "0" || since == "0-0" {
		// Always reset for since=0 (initial connection).
		return fmt.Errorf("since=0 requires snapshot")
	}

	firstID, _, _, err := reader.StreamInfo()
	if err != nil {
		return fmt.Errorf("cannot verify stream coverage for since %s: %w", since, errStoreUnavailable)
	}
	if firstID == "" {
		return fmt.Errorf("no retained entries available for since %s", since)
	}

	// Compare stream IDs lexicographically (Redis stream IDs are comparable).
	if compareStreamIDs(since, firstID) < 0 {
		return fmt.Errorf("since %s is older than oldest entry %s", since, firstID)
	}
	return nil
}

// compareStreamIDs compares two Redis stream IDs numerically. Returns -1, 0, or 1.
// Stream IDs have format "{ms}-{seq}".
func compareStreamIDs(a, b string) int {
	aParts := strings.SplitN(a, "-", 2)
	bParts := strings.SplitN(b, "-", 2)

	aMS, _ := strconv.ParseUint(aParts[0], 10, 64)
	bMS, _ := strconv.ParseUint(bParts[0], 10, 64)

	if aMS < bMS {
		return -1
	}
	if aMS > bMS {
		return 1
	}

	// Same millisecond — compare sequence.
	var aSeq, bSeq uint64
	if len(aParts) > 1 {
		aSeq, _ = strconv.ParseUint(aParts[1], 10, 64)
	}
	if len(bParts) > 1 {
		bSeq, _ = strconv.ParseUint(bParts[1], 10, 64)
	}

	if aSeq < bSeq {
		return -1
	}
	if aSeq > bSeq {
		return 1
	}
	return 0
}

// trimBatchSize is the number of entries removed per trim tick.
const trimBatchSize = 1000

// trimInterval is the base interval between trim ticks when the trimmer
// is caught up. When a full batch is trimmed (indicating a backlog), the
// next tick fires after trimBacklogDelay instead.
const trimInterval = 1 * time.Minute
const trimBacklogDelay = 1 * time.Second
const trimErrorDelay = 5 * time.Minute

// TrimChangelog trims changelog entries older than the retention period,
// removing up to trimBatchSize entries per call.
func TrimChangelog(ctx context.Context, writer *store.Writer, retention time.Duration) (int64, error) {
	cutoff := time.Now().Add(-retention)
	minID := fmt.Sprintf("%d-0", cutoff.UnixMilli())
	return writer.StreamTrimOlderThan(ctx, minID, trimBatchSize)
}

// RunChangelogTrimmer periodically trims the changelog stream. It adapts
// its tick rate: when a full batch is removed (backlog), the next tick
// fires after trimBacklogDelay; otherwise it waits trimInterval. On errors
// it waits trimErrorDelay before retrying.
func RunChangelogTrimmer(ctx context.Context, writer *store.Writer, retention time.Duration) {
	timer := time.NewTimer(trimInterval)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
			n, err := TrimChangelog(ctx, writer, retention)
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
