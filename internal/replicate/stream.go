package replicate

import (
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

// ClientReconnectDelay is sent to clients via the SSE `retry:` field as a
// hint for how long to wait before reconnecting after the connection drops.
const ClientReconnectDelay = 3 * time.Second

// writeSSEHeaders writes the response headers expected by EventSource clients
// and SSE-aware proxies. It must be called before WriteHeader.
func writeSSEHeaders(w http.ResponseWriter) {
	h := w.Header()
	h.Set("Content-Type", "text/event-stream; charset=utf-8")
	h.Set("Cache-Control", "no-cache, no-transform")
	h.Set("Connection", "keep-alive")
	// Hint to nginx and other reverse proxies to disable response
	// buffering so events are flushed to the client immediately.
	h.Set("X-Accel-Buffering", "no")
	// Permit cross-origin EventSource consumers (mirrors Wikimedia
	// EventStreams behavior).
	h.Set("Access-Control-Allow-Origin", "*")
	h.Set("Vary", "Accept-Encoding")
}

// resolveStreamCursor returns the stream cursor for this request, honoring
// the `since` query parameter first and falling back to the standard
// `Last-Event-ID` request header (used by browser EventSource on reconnect).
// When neither is provided, "0" is returned, which forces a reset.
func resolveStreamCursor(r *http.Request) string {
	if since := r.URL.Query().Get("since"); since != "" {
		return since
	}
	if last := r.Header.Get("Last-Event-ID"); last != "" {
		return last
	}
	return "0"
}

// ServeStream handles GET /replicate/stream as an SSE endpoint compatible
// with the W3C EventSource specification. The client identifies its current
// position via either the `since` query parameter or the standard
// `Last-Event-ID` request header (set automatically by EventSource on
// reconnect). It replays changelog entries after that position and then
// blocks for live changes. If coverage cannot be proven (no retained
// entries, or position older than the oldest retained entry), it sends an
// "event: reset" and closes the connection.
func ServeStream(reader *store.Reader, w http.ResponseWriter, r *http.Request) {
	since := resolveStreamCursor(r)

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

		writeSSEHeaders(w)
		w.WriteHeader(http.StatusOK)

		resetData := map[string]string{"reason": err.Error()}
		if stats, sErr := reader.GetStats(); sErr == nil {
			resetData["state"] = stats.State
		}
		resetJSON, _ := json.Marshal(resetData)
		fmt.Fprintf(w, "retry: %d\n", ClientReconnectDelay.Milliseconds())
		fmt.Fprintf(w, "event: reset\ndata: %s\n\n", resetJSON)
		flusher.Flush()
		return
	}

	writeSSEHeaders(w)
	w.WriteHeader(http.StatusOK)

	// Initial preamble: a comment to flush headers immediately so clients
	// know the connection is live, plus the reconnect-delay hint.
	fmt.Fprintf(w, ": connected\nretry: %d\n\n", ClientReconnectDelay.Milliseconds())
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
			// Emit the standard SSE `id:` field so EventSource clients
			// resume automatically via Last-Event-ID after a disconnect.
			fmt.Fprintf(w, "id: %s\nevent: change\ndata: %s\n\n", ev.ID, FormatStreamChangeData(ev.QID, ev.RawMappings))
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
