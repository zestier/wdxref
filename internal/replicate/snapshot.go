package replicate

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"time"

	"github.com/goccy/go-json"
	"github.com/klauspost/compress/zstd"

	"github.com/ekeid/ekeid/internal/store"
)

// snapshotMeta is the contents of meta.json. It points at the currently
// published snapshot file and records the changelog stream ID represented by
// that snapshot for operator visibility.
type snapshotMeta struct {
	File      string    `json:"file"`
	ID        string    `json:"id"`
	CreatedAt time.Time `json:"created_at"`
}

// countingWriter wraps an io.Writer and counts bytes written.
type countingWriter struct {
	w io.Writer
	n int64
}

func (cw *countingWriter) Write(p []byte) (int, error) {
	n, err := cw.w.Write(p)
	cw.n += int64(n)
	return n, err
}

// SnapshotGenerator periodically generates a snapshot of all entities and
// writes it as a zstd-compressed line file with a companion meta.json.
type SnapshotGenerator struct {
	reader   *store.Reader
	dir      string
	interval time.Duration
}

// NewSnapshotGenerator creates a new generator that writes snapshots to dir
// at the given interval.
func NewSnapshotGenerator(reader *store.Reader, dir string, interval time.Duration) *SnapshotGenerator {
	if interval <= 0 {
		interval = DefaultSnapshotInterval
	}
	return &SnapshotGenerator{
		reader:   reader,
		dir:      dir,
		interval: interval,
	}
}

// Run generates snapshots in a loop until ctx is cancelled.
func (g *SnapshotGenerator) Run(ctx context.Context) {
	// Clean up any leftover snapshot files from previous runs.
	g.cleanSnapshots()

	for {
		if err := g.generate(ctx); err != nil {
			slog.Error("snapshot: generation failed", "error", err)
		}

		// Use a shorter retry interval until the first snapshot exists so we
		// don't wait a full snapshot interval (potentially 24h) if the first
		// attempt skips due to an empty changelog or transient error.
		wait := g.interval
		if g.readMeta() == nil {
			wait = 30 * time.Second
		}

		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

// readMeta reads and parses the current publication pointer, returning nil if
// it doesn't exist or can't be parsed.
func (g *SnapshotGenerator) readMeta() *snapshotMeta {
	data, err := os.ReadFile(filepath.Join(g.dir, "meta.json"))
	if err != nil {
		return nil
	}
	var meta snapshotMeta
	if json.Unmarshal(data, &meta) != nil {
		return nil
	}
	return &meta
}

// cleanSnapshots removes all snapshot-*.zst files in the directory except the
// one currently referenced by meta.json. This handles leftover files from
// crashes, races, or incomplete writes.
func (g *SnapshotGenerator) cleanSnapshots() {
	meta := g.readMeta()
	keep := ""
	if meta != nil {
		keep = meta.File
	}

	matches, err := filepath.Glob(filepath.Join(g.dir, "snapshot-*.zst"))
	if err != nil {
		return
	}
	for _, path := range matches {
		name := filepath.Base(path)
		if name != keep {
			os.Remove(path)
		}
	}
	// Also clean up old .gz snapshots from before the format switch.
	oldMatches, _ := filepath.Glob(filepath.Join(g.dir, "snapshot-*.gz"))
	for _, path := range oldMatches {
		os.Remove(path)
	}
	// Also remove any leftover temp files.
	os.Remove(filepath.Join(g.dir, "meta.tmp"))
}

// generate captures the current state of all entities and writes a zstd line
// file where most entries are "<qid> <raw-mappings-json>". Each zstd frame
// begins with a checkpoint control line, and the final line in the stream is a
// done control line that carries the stream position represented by the
// snapshot. The companion meta.json is reduced to a publication pointer for the
// current file.
//
// Flow: write new snapshot → update meta.json → delete old snapshots.
func (g *SnapshotGenerator) generate(ctx context.Context) error {
	// Skip if a recent snapshot already exists (within the interval).
	if meta := g.readMeta(); meta != nil && !meta.CreatedAt.IsZero() {
		age := time.Since(meta.CreatedAt)
		if age < g.interval {
			slog.Info("snapshot: skipping, recent snapshot exists", "file", meta.File, "age", age.Round(time.Second))
			return nil
		}
	}

	// Record the changelog position before scanning.
	_, lastID, _, err := g.reader.StreamInfo()
	if err != nil {
		return fmt.Errorf("stream info: %w", err)
	}
	if lastID == "" {
		// The changelog is empty — skip this generation cycle. A snapshot
		// with ID "0-0" would cause the replica to loop: the server always
		// rejects since=0-0 with a reset, which clears replica state and
		// triggers another snapshot fetch indefinitely.
		slog.Info("snapshot: skipping, changelog is empty")
		return nil
	}

	// Each snapshot gets a unique filename based on the current timestamp.
	snapshotName := fmt.Sprintf("snapshot-%d.zst", time.Now().UnixNano())
	snapshotPath := filepath.Join(g.dir, snapshotName)
	metaPath := filepath.Join(g.dir, "meta.json")

	f, err := os.Create(snapshotPath)
	if err != nil {
		return fmt.Errorf("create snapshot file: %w", err)
	}
	defer f.Close()

	cw := &countingWriter{w: f}
	zw, err := zstd.NewWriter(cw)
	if err != nil {
		return fmt.Errorf("create zstd writer: %w", err)
	}
	var lineBuf []byte

	var count int
	frameCount := 0
	entitiesInFrame := 0
	lineBuf, err = AppendSnapshotCheckpointLine(lineBuf[:0], 0, 0)
	if err != nil {
		return fmt.Errorf("encode snapshot checkpoint: %w", err)
	}
	if _, err := zw.Write(lineBuf); err != nil {
		return fmt.Errorf("write snapshot checkpoint: %w", err)
	}
	frameCount++

	var cursor uint64
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}

		entities, next, err := g.reader.ScanEntities(ctx, cursor, 1000)
		if err != nil {
			return fmt.Errorf("scan: %w", err)
		}

		for _, ent := range entities {
			if entitiesInFrame == SnapshotFrameSize {
				if err := zw.Close(); err != nil {
					return fmt.Errorf("close zstd frame: %w", err)
				}
				checkpointOffset := cw.n
				lineBuf, err = AppendSnapshotCheckpointLine(lineBuf[:0], checkpointOffset, int64(count))
				if err != nil {
					return fmt.Errorf("encode snapshot checkpoint: %w", err)
				}
				zw.Reset(cw)
				entitiesInFrame = 0
				if _, err := zw.Write(lineBuf); err != nil {
					return fmt.Errorf("write snapshot checkpoint: %w", err)
				}
				frameCount++
			}

			lineBuf = AppendSnapshotEntityLine(lineBuf[:0], ent.QID, ent.RawMappings)
			if _, err := zw.Write(lineBuf); err != nil {
				return fmt.Errorf("write entity: %w", err)
			}
			count++
			entitiesInFrame++
		}

		cursor = next
		if cursor == 0 {
			break
		}
	}

	lineBuf, err = AppendSnapshotDoneLine(lineBuf[:0], lastID, int64(count))
	if err != nil {
		return fmt.Errorf("encode snapshot done: %w", err)
	}
	if _, err := zw.Write(lineBuf); err != nil {
		return fmt.Errorf("write snapshot done: %w", err)
	}

	if err := zw.Close(); err != nil {
		return fmt.Errorf("close zstd: %w", err)
	}
	if err := f.Close(); err != nil {
		return fmt.Errorf("close file: %w", err)
	}

	// Write meta.json with frame map.
	metaData, err := json.Marshal(snapshotMeta{
		File:      snapshotName,
		ID:        lastID,
		CreatedAt: time.Now(),
	})
	if err != nil {
		return fmt.Errorf("marshal meta: %w", err)
	}
	tmpPath := filepath.Join(g.dir, "meta.tmp")
	if err := os.WriteFile(tmpPath, metaData, 0644); err != nil {
		return fmt.Errorf("write meta tmp: %w", err)
	}
	if err := os.Rename(tmpPath, metaPath); err != nil {
		return fmt.Errorf("rename meta: %w", err)
	}

	// Delete old snapshot files. Any in-progress downloads keep the old
	// inode alive via their open file descriptor.
	g.cleanSnapshots()

	slog.Info("snapshot: generated", "entities", count, "frames", frameCount, "stream_id", lastID, "file", snapshotName)
	return nil
}

// ServeSnapshot handles GET /replicate/snapshot requests.
// Readiness and publication metadata are read from disk each time — no
// in-memory state. Resume is handled with standard HTTP Range and If-Range
// over the raw compressed bytes.
func (g *SnapshotGenerator) ServeSnapshot(w http.ResponseWriter, r *http.Request) {
	// Try up to twice: if the referenced file was deleted between reading
	// meta and opening the file, re-read meta to pick up the new snapshot.
	for range 2 {
		meta := g.readMeta()
		if meta == nil {
			http.Error(w, "snapshot not yet available", http.StatusServiceUnavailable)
			return
		}

		snapshotPath := filepath.Join(g.dir, meta.File)
		f, err := os.Open(snapshotPath)
		if err != nil {
			// The old file was deleted between reading meta and opening.
			// Retry to pick up the new meta.
			continue
		}
		defer f.Close()

		stat, err := f.Stat()
		if err != nil {
			http.Error(w, "snapshot stat failed", http.StatusInternalServerError)
			return
		}

		etag := `"` + meta.File + `"`
		w.Header().Set("Content-Type", "application/zstd")
		w.Header().Set("Content-Encoding", "identity")
		w.Header().Set("ETag", etag)

		http.ServeContent(w, r, meta.File, stat.ModTime(), f)
		return
	}

	http.Error(w, "snapshot temporarily unavailable", http.StatusServiceUnavailable)
}
