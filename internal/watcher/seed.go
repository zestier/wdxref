package watcher

import (
	"bufio"
	"bytes"
	"compress/bzip2"
	"compress/gzip"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ekeid/ekeid/internal/store"
)

// DumpFormat selects the compression format for Wikidata dump downloads.
type DumpFormat string

const (
	// DumpFormatGZ uses gzip compression (~130 GB). Faster decompression.
	DumpFormatGZ DumpFormat = "gz"
	// DumpFormatBZ2 uses bzip2 compression (~100 GB). Smaller download, slower decompression.
	DumpFormatBZ2 DumpFormat = "bz2"
)

// ErrDumpChanged is returned when the remote dump file changes (ETag mismatch)
// during a resumable download. Callers should reset any partial state and restart.
var ErrDumpChanged = errors.New("dump file changed during download")

const (
	dumpBaseURL          = "https://dumps.wikimedia.org/wikidatawiki/entities/latest-all.json."
	dumpScannerBufSize   = 16 * 1024 * 1024 // 16 MB max line
	dumpMaxLineSize      = 4 * 1024 * 1024  // 4 MB skip threshold
	dumpDecompChunkSize  = 256 * 1024       // 256 KB per decompression chunk
	dumpDecompChanCap    = 64               // up to 16 MB buffered ahead
	dumpProgressInterval = 30 * time.Second // log progress every 30s
	dumpBatchSize        = 2000             // entities per DB transaction

	dumpResumeMaxRetries    = 10              // max consecutive resume attempts
	dumpResumeBaseDelay     = 5 * time.Second // initial backoff delay
	dumpResumeMaxDelay      = 2 * time.Minute // maximum backoff delay
	dumpResumeBackoffFactor = 2               // exponential backoff multiplier

	dumpWriteMaxRetries = 5               // retries per batch flush to Kvrocks
	dumpWriteBaseDelay  = 2 * time.Second // initial retry backoff for writes

	// seedVersion is hashed for configFingerprint. Bump when seed-time
	// configuration changes (e.g. property filters) to trigger a reseed
	// on next startup without dropping all tables.
	seedVersion = "v2-property-based"

	dumpDateLookbackDays = 14                                                                           // how far back to look for dated dumps
	dumpStaleThreshold   = 7 * 24 * time.Hour                                                           // warn if dump is older than this
	dumpDatePathPattern  = "https://dumps.wikimedia.org/wikidatawiki/entities/%s/wikidata-%s-all.json." // YYYYMMDD date pattern
)

var (
	dumpResumeRetryDelay = func(retries int) time.Duration {
		delay := dumpResumeBaseDelay
		for i := 1; i < retries; i++ {
			delay *= time.Duration(dumpResumeBackoffFactor)
			if delay > dumpResumeMaxDelay {
				return dumpResumeMaxDelay
			}
		}
		return delay
	}

	dumpWriteRetryDelay = func(attempt int) time.Duration {
		if attempt <= 0 {
			return 0
		}
		return dumpWriteBaseDelay << (attempt - 1)
	}
)

// countingReader wraps an io.Reader and counts the number of bytes read.
// The counter is safe for concurrent access.
type countingReader struct {
	r     io.Reader
	bytes atomic.Int64
}

func (cr *countingReader) Read(p []byte) (int, error) {
	n, err := cr.r.Read(p)
	cr.bytes.Add(int64(n))
	return n, err
}

// chanReader implements io.Reader over a channel of byte slices, allowing
// a producer goroutine to run ahead of the consumer with buffering.
// Consumed buffers are returned to pool for reuse.
type chanReader struct {
	ch    <-chan []byte
	errCh <-chan error
	pool  *sync.Pool
	cur   []byte
	full  *[]byte // pointer to the original full-capacity slice for pool return
}

func (r *chanReader) Read(p []byte) (int, error) {
	for len(r.cur) == 0 {
		// Return the previous buffer to the pool
		if r.full != nil {
			r.pool.Put(r.full)
			r.full = nil
		}
		chunk, ok := <-r.ch
		if !ok {
			select {
			case err := <-r.errCh:
				if err != nil {
					return 0, err
				}
			default:
			}
			return 0, io.EOF
		}
		r.cur = chunk
		// Save pointer to the underlying array for pool return
		tmp := chunk[:cap(chunk)]
		r.full = &tmp
	}
	n := copy(p, r.cur)
	r.cur = r.cur[n:]
	return n, nil
}

// resumableBody wraps an HTTP response body and transparently reconnects
// using Range requests when the underlying connection fails. It validates
// the ETag on each reconnect to detect file changes. This enables resuming
// a 100 GB+ bz2/gzip download across transient network failures without
// restarting the decompressor, since the compressed byte stream is identical.
type resumableBody struct {
	body       io.ReadCloser
	url        string
	etag       string
	offset     int64
	ctx        context.Context
	httpClient *http.Client
	retries    int
}

// newResumableBody creates a resumableBody. The etag is the value from the
// initial response. If etag is empty (server doesn't support it), resumption
// is disabled and it behaves like a normal body.
func newResumableBody(ctx context.Context, body io.ReadCloser, url, etag string, httpClient *http.Client) *resumableBody {
	if ctx == nil {
		ctx = context.Background()
	}
	return &resumableBody{
		body:       body,
		url:        url,
		etag:       etag,
		ctx:        ctx,
		httpClient: httpClient,
	}
}

func (rb *resumableBody) Read(p []byte) (int, error) {
	n, err := rb.body.Read(p)
	rb.offset += int64(n)
	if n > 0 {
		rb.retries = 0 // successful read resets retry counter
	}
	if err != nil && err != io.EOF && rb.etag != "" {
		// Connection dropped — try to resume
		if resumeErr := rb.reconnect(); resumeErr != nil {
			return n, resumeErr
		}
		// Reconnected successfully; report what we got, next Read will use new body
		return n, nil
	}
	return n, err
}

func (rb *resumableBody) reconnect() error {
	rb.retries++
	if rb.retries > dumpResumeMaxRetries {
		return fmt.Errorf("dump download failed after %d resume attempts", dumpResumeMaxRetries)
	}

	delay := dumpResumeRetryDelay(rb.retries)
	slog.Warn("connection lost, retrying", "offset", rb.offset, "delay", delay, "attempt", rb.retries, "max_attempts", dumpResumeMaxRetries)
	if err := waitForContext(rb.ctx, delay); err != nil {
		return err
	}

	rb.body.Close()

	req, err := newRequestWithContext(rb.ctx, "GET", rb.url)
	if err != nil {
		return fmt.Errorf("create resume request: %w", err)
	}
	req.Header.Set("Range", fmt.Sprintf("bytes=%d-", rb.offset))
	req.Header.Set("If-Match", rb.etag)

	resp, err := rb.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("resume request: %w", err)
	}

	switch resp.StatusCode {
	case http.StatusPartialContent:
		rb.body = resp.Body
		slog.Info("resumed download", "offset", rb.offset)
		return nil
	case http.StatusPreconditionFailed:
		resp.Body.Close()
		return ErrDumpChanged
	case http.StatusRequestedRangeNotSatisfiable:
		// Offset past end of file — file may have changed
		resp.Body.Close()
		return ErrDumpChanged
	case http.StatusOK:
		// Server doesn't support Range for this resource; can't resume
		resp.Body.Close()
		return fmt.Errorf("server returned 200 instead of 206, Range requests not supported")
	default:
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
		resp.Body.Close()
		return fmt.Errorf("resume request returned status %d: %s", resp.StatusCode, string(body))
	}
}

func (rb *resumableBody) Close() error {
	return rb.body.Close()
}

// configFingerprint returns a stable hash of the seed configuration.
// When the seed version changes, this hash changes, triggering a reseed.
func configFingerprint() string {
	h := sha256.Sum256([]byte(seedVersion))
	return fmt.Sprintf("%x", h[:16])
}

// findDatedDumpURL attempts to find a dated dump URL by rolling back from today.
// It tries up to dumpDateLookback days in the past and returns the most recent
// available dump. It returns the URL, extracted date, and whether it's stale
// (older than dumpStaleThreshold). If no dated dump is found, it returns
// empty strings.
func (s *Seeder) findDatedDumpURL(ctx context.Context) (url, dateStr string, stale bool) {
	now := time.Now().UTC()
	// Try going back up to dumpDateLookbackDays days, one day at a time
	for daysBack := 0; daysBack <= dumpDateLookbackDays; daysBack++ {
		date := now.AddDate(0, 0, -daysBack)
		dateStr := date.Format("20060102")
		url := fmt.Sprintf(dumpDatePathPattern, dateStr, dateStr) + string(s.dumpFormat)

		// Try a HEAD request to check if this date exists
		req, err := http.NewRequestWithContext(ctx, "HEAD", url, nil)
		if err != nil {
			continue
		}

		resp, err := s.httpClient.Do(req)
		if err != nil {
			slog.Debug("dated dump check failed", "date", dateStr, "error", err)
			continue
		}
		resp.Body.Close()

		if resp.StatusCode == http.StatusOK {
			// Found a valid dated dump
			age := now.Sub(date)
			ageDays := int(age.Hours() / 24)
			if age > dumpStaleThreshold {
				slog.Warn("dated dump is older than threshold", "age_days", ageDays, "threshold", dumpStaleThreshold, "date", dateStr)
				return url, dateStr, true
			}
			slog.Info("found recent dated dump", "date", dateStr, "age_days", ageDays)
			return url, dateStr, false
		}

		slog.Debug("dated dump not found", "date", dateStr, "status", resp.StatusCode)
	}

	slog.Error("no dated dump found within lookback window", "lookback_days", dumpDateLookbackDays)
	return "", "", false
}

// Seeder handles bulk import by streaming a Wikidata JSON dump.
type Seeder struct {
	writer      *store.Writer
	reader      *store.Reader
	httpClient  *http.Client
	dumpFormat  DumpFormat
	dumpLocator func(ctx context.Context) (url, dateStr string, stale bool)
}

// NewSeeder creates a new Seeder. The format parameter selects the
// compression format (DumpFormatGZ or DumpFormatBZ2). An empty value
// defaults to gz.
func NewSeeder(writer *store.Writer, reader *store.Reader, httpClient *http.Client, format DumpFormat) *Seeder {
	if httpClient == nil {
		httpClient = &http.Client{Timeout: 0}
	}
	if format == "" {
		format = DumpFormatGZ
	}
	seeder := &Seeder{
		writer:     writer,
		reader:     reader,
		httpClient: httpClient,
		dumpFormat: format,
	}
	seeder.dumpLocator = seeder.findDatedDumpURL
	return seeder
}

// newDecompressor returns a reader that decompresses data from r
// using the Seeder's configured format.
func (s *Seeder) newDecompressor(r io.Reader) (io.Reader, error) {
	switch s.dumpFormat {
	case DumpFormatGZ:
		return gzip.NewReader(r)
	case DumpFormatBZ2:
		return bzip2.NewReader(r), nil
	default:
		return nil, fmt.Errorf("unsupported dump format: %q", s.dumpFormat)
	}
}

// NeedsSeed determines whether the database needs seeding.
func (s *Seeder) NeedsSeed() (bool, error) {
	dumpTime, err := s.reader.GetSyncState("dump_time")
	if err != nil {
		return false, err
	}
	if dumpTime == "" {
		return true, nil
	}

	storedHash, err := s.reader.GetSyncState("config_hash")
	if err != nil {
		return false, err
	}
	if storedHash != configFingerprint() {
		slog.Info("seed configuration changed, reseed required")
		return true, nil
	}

	return false, nil
}

// Seed performs a bulk import by streaming the Wikidata JSON dump.
// It flushes the database first and imports into a clean state.
// It respects ctx cancellation so the process can shut down promptly.
func (s *Seeder) Seed(ctx context.Context) error {
	locator := s.dumpLocator
	if locator == nil {
		locator = s.findDatedDumpURL
	}

	// Find a dated dump URL with an actual generation date in the path.
	// This is required—no fallback to latest or Last-Modified headers.
	dumpURL, dateStr, stale := locator(ctx)
	if dumpURL == "" {
		return fmt.Errorf("no dated dump found within %d day lookback", dumpDateLookbackDays)
	}
	if stale {
		slog.Warn("using stale dated dump, will continue anyway", "date", dateStr)
	}

	slog.Info("starting seed from wikidata dump", "url", dumpURL, "date", dateStr)

	seedStart := time.Now()

	resp, err := s.openDumpStream(ctx, dumpURL)
	if err != nil {
		return err
	}
	defer resp.Body.Close()

	slog.Info("flushing database before seed")
	if err := s.writer.FlushDataContext(ctx); err != nil {
		return fmt.Errorf("flush data: %w", err)
	}

	p := s.writer.NewPipe(ctx)
	p.SetSyncState("config_hash", configFingerprint())
	p.SetSyncState("state", "seeding")
	if err := p.Exec(); err != nil {
		return fmt.Errorf("set seed state: %w", err)
	}

	// Convert YYYYMMDD to RFC3339
	var syncTime string
	if t, err := time.Parse("20060102", dateStr); err == nil {
		syncTime = t.UTC().Format(time.RFC3339)
	} else {
		return fmt.Errorf("parse dump date %q: %w", dateStr, err)
	}
	slog.Info("dump date from URL", "dump_time", syncTime)

	counter := &countingReader{r: resp.Body}

	// Run decompression in a background goroutine with a buffered channel
	// so it can run ahead of line parsing (important for CPU-heavy bzip2).
	chunks := make(chan []byte, dumpDecompChanCap)
	errCh := make(chan error, 1)
	chunkPool := &sync.Pool{
		New: func() any {
			b := make([]byte, dumpDecompChunkSize)
			return &b
		},
	}

	go func() {
		defer close(chunks)
		decompressed, err := s.newDecompressor(counter)
		if err != nil {
			errCh <- fmt.Errorf("create decompressor: %w", err)
			return
		}
		if closer, ok := decompressed.(io.Closer); ok {
			defer closer.Close()
		}
		for {
			select {
			case <-ctx.Done():
				errCh <- ctx.Err()
				return
			default:
			}
			bufp := chunkPool.Get().(*[]byte)
			buf := (*bufp)[:dumpDecompChunkSize]
			n, err := decompressed.Read(buf)
			if n > 0 {
				select {
				case chunks <- buf[:n]:
				case <-ctx.Done():
					chunkPool.Put(bufp)
					errCh <- ctx.Err()
					return
				}
			} else {
				chunkPool.Put(bufp)
			}
			if err != nil {
				if err != io.EOF {
					errCh <- err
				}
				return
			}
		}
	}()

	decompReader := &chanReader{ch: chunks, errCh: errCh, pool: chunkPool}
	imported, lines, err := s.processDumpStream(ctx, decompReader, resp.ContentLength, counter)
	if err != nil {
		return err
	}

	p = s.writer.NewPipe(ctx)
	p.SetSyncState("dump_time", syncTime)
	if err := p.Exec(); err != nil {
		return fmt.Errorf("set dump_time: %w", err)
	}

	slog.Info("dump seed complete", "imported", imported, "lines", lines, "elapsed", time.Since(seedStart).Truncate(time.Second))
	return nil
}

func (s *Seeder) openDumpStream(ctx context.Context, dumpURL string) (*http.Response, error) {
	req, err := newRequestWithContext(ctx, "GET", dumpURL)
	if err != nil {
		return nil, fmt.Errorf("create request: %w", err)
	}

	resp, err := s.httpClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("download dump: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(io.LimitReader(resp.Body, 1024))
		resp.Body.Close()
		return nil, fmt.Errorf("dump download returned status %d: %s", resp.StatusCode, string(body))
	}

	// Wrap body in resumableBody if the server provided an ETag.
	// This enables transparent reconnection via Range requests on network drops.
	etag := resp.Header.Get("ETag")
	if etag != "" {
		slog.Info("dump etag, resumable download enabled", "etag", strings.Trim(etag, "\""))
		resp.Body = newResumableBody(ctx, resp.Body, dumpURL, etag, s.httpClient)
	} else {
		slog.Info("no etag in response, resumable download disabled")
	}

	return resp, nil
}

// processDumpStream reads decompressed JSON lines from r and upserts
// relevant entities. It uses a background goroutine to write to the DB
// while parsing continues, overlapping I/O with CPU work.
func (s *Seeder) processDumpStream(ctx context.Context, r io.Reader, totalCompressed int64, counter *countingReader) (int, int, error) {
	writeCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	scanner := bufio.NewScanner(r)
	scanner.Buffer(make([]byte, 0, dumpScannerBufSize), dumpScannerBufSize)

	lines := 0
	started := time.Now()
	lastLog := started

	// Channel for sending entities to background writer
	entityCh := make(chan store.EntityRecord, 10000)

	var wg sync.WaitGroup
	wg.Add(1)

	// Background writer: receives entities, batches them, writes to DB.
	// On persistent failure (after retries), it cancels the pipeline with the
	// underlying error so the producer stops cleanly.
	go func() {
		defer wg.Done()

		batch := make([]store.EntityRecord, 0, dumpBatchSize)
		flush := func() error {
			if len(batch) == 0 {
				return nil
			}
			var err error
			for attempt := 0; attempt < dumpWriteMaxRetries; attempt++ {
				if attempt > 0 {
					delay := dumpWriteRetryDelay(attempt)
					slog.Warn("retrying seed batch write", "attempt", attempt+1, "delay", delay, "error", err)
					if err := waitForContext(writeCtx, delay); err != nil {
						return err
					}
				}
				p := s.writer.NewPipe(writeCtx)
				for _, rec := range batch {
					p.UpsertEntity(rec)
				}
				err = p.Exec()
				if err == nil {
					batch = batch[:0]
					return nil
				}
				if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
					return err
				}
			}
			return fmt.Errorf("after %d attempts: %w", dumpWriteMaxRetries, err)
		}

		for item := range entityCh {
			batch = append(batch, item)
			if len(batch) >= dumpBatchSize {
				if err := flush(); err != nil {
					slog.Error("background writer error", "error", err)
					cancel(fmt.Errorf("background writer: %w", err))
					return
				}
			}
		}

		// Flush remaining
		if err := flush(); err != nil {
			slog.Error("background writer final flush error", "error", err)
			cancel(fmt.Errorf("background writer: %w", err))
		}
	}()

	imported := 0

	for scanner.Scan() {
		select {
		case <-ctx.Done():
			close(entityCh)
			wg.Wait()
			return imported, lines, ctx.Err()
		default:
		}
		if err := context.Cause(writeCtx); err != nil && ctx.Err() == nil {
			close(entityCh)
			wg.Wait()
			return imported, lines, err
		}
		line := scanner.Bytes()
		lines++

		line = bytes.TrimSpace(line)
		if len(line) == 0 || line[0] == '[' || line[0] == ']' {
			continue
		}
		if len(line) > dumpMaxLineSize {
			slog.Warn("skipping oversized line", "line", lines, "size", len(line), "max_size", dumpMaxLineSize)
			continue
		}
		if line[len(line)-1] == ',' {
			line = line[:len(line)-1]
		}

		entity, err := ParseEntityJSON(line)
		if err != nil {
			slog.Warn("failed to parse dump line", "line", lines, "error", err)
			continue
		}
		if entity == nil {
			continue
		}

		select {
		case entityCh <- store.EntityRecord{
			WikidataID: entity.ID,
			Mappings:   entity.Mappings,
		}:
		case <-writeCtx.Done():
			close(entityCh)
			wg.Wait()
			if err := ctx.Err(); err != nil {
				return imported, lines, err
			}
			return imported, lines, context.Cause(writeCtx)
		case <-ctx.Done():
			close(entityCh)
			wg.Wait()
			return imported, lines, ctx.Err()
		}
		imported++

		if now := time.Now(); now.Sub(lastLog) >= dumpProgressInterval {
			rate := float64(lines) / now.Sub(started).Seconds()
			if totalCompressed > 0 && counter != nil {
				pct := float64(counter.bytes.Load()) / float64(totalCompressed) * 100
				slog.Info("dump progress", "percent", fmt.Sprintf("%.2f", pct), "lines", lines, "imported", imported, "rate", fmt.Sprintf("%.2f", rate))
			} else {
				slog.Info("dump progress", "lines", lines, "imported", imported, "rate", fmt.Sprintf("%.2f", rate))
			}
			lastLog = now
		}
	}

	// Close channel and wait for writer to finish
	close(entityCh)
	wg.Wait()

	if err := ctx.Err(); err != nil {
		return imported, lines, err
	}

	if err := context.Cause(writeCtx); err != nil && ctx.Err() == nil {
		return imported, lines, err
	}

	if err := scanner.Err(); err != nil {
		return imported, lines, fmt.Errorf("scanner error at line %d: %w", lines, err)
	}

	return imported, lines, nil
}

func waitForContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
			return nil
		}
	}

	timer := time.NewTimer(d)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
