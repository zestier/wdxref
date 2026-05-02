package replicate

import (
	"fmt"
	"net/http"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/httpencoding"
	"github.com/ekeid/ekeid/internal/store"
)

// healthResponse is returned by /replicate/health.
type healthResponse struct {
	State       string `json:"state"`
	StreamLen   int64  `json:"stream_length"`
	FirstID     string `json:"first_id,omitempty"`
	LastID      string `json:"last_id,omitempty"`
	SnapshotID  string `json:"snapshot_id,omitempty"`
	SnapshotAge string `json:"snapshot_age,omitempty"`
	EntityCount int64  `json:"entity_count"`
}

// Handler returns an http.Handler that serves the replication endpoints:
//
//	GET /replicate/snapshot  — pre-generated entity snapshot (zstd line format)
//	GET /replicate/stream    — SSE changelog stream
//	GET /replicate/health    — replication status
func Handler(reader *store.Reader, snapshot *SnapshotGenerator, encodings []string) http.Handler {
	mux := http.NewServeMux()

	mux.HandleFunc("GET /replicate/snapshot", snapshot.ServeSnapshot)

	mux.Handle("GET /replicate/stream", httpencoding.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ServeStream(reader, w, r)
	}), encodings))

	mux.Handle("GET /replicate/health", httpencoding.Middleware(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		serveHealth(reader, snapshot, w)
	}), encodings))

	return mux
}

func serveHealth(reader *store.Reader, snapshot *SnapshotGenerator, w http.ResponseWriter) {
	resp := healthResponse{}

	stats, err := reader.GetStats()
	if err == nil {
		resp.State = stats.State
		resp.EntityCount = stats.EntityCount
	}

	firstID, lastID, length, err := reader.StreamInfo()
	if err == nil {
		resp.StreamLen = length
		resp.FirstID = firstID
		resp.LastID = lastID
	}

	if meta := snapshot.readMeta(); meta != nil {
		resp.SnapshotID = meta.ID
		if ms := parseStreamIDMs(meta.ID); ms > 0 {
			age := time.Since(time.UnixMilli(ms))
			resp.SnapshotAge = age.Truncate(time.Second).String()
		}
	}

	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-cache")
	_ = json.NewEncoder(w).Encode(resp)
}

// parseStreamIDMs extracts the millisecond timestamp from a Redis stream ID
// (format "ms-seq"). Returns 0 if parsing fails.
func parseStreamIDMs(id string) int64 {
	var ms int64
	_, _ = fmt.Sscanf(id, "%d-", &ms)
	return ms
}
