package watcher

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"strings"
	"sync"
	"time"

	"github.com/goccy/go-json"

	"github.com/ekeid/ekeid/internal/store"
)

// ErrStreamTooOld is returned when the EventStream does not have data going
// back far enough to cover the last sync point. The caller should reseed.
var ErrStreamTooOld = errors.New("event stream does not have data going back far enough")

const (
	eventStreamURL     = "https://stream.wikimedia.org/v2/stream/recentchange"
	reconnectDelay     = 5 * time.Second
	retryInterval      = 10 * time.Minute
	retryBatchSize     = 50
	queuePollInterval  = 30 * time.Second
	enqueueBatchSize   = 1000            // buffer up to N QIDs before flushing to DB
	enqueueFlushDelay  = 5 * time.Second // max delay before flushing a partial batch
	streamGapThreshold = 1 * time.Hour
)

// recentChangeEvent represents a Wikidata recent change event.
type recentChangeEvent struct {
	Meta struct {
		Domain string `json:"domain"`
	} `json:"meta"`
	Wiki      string `json:"wiki"`
	Namespace int    `json:"namespace"`
	Title     string `json:"title"`
	Timestamp int64  `json:"timestamp"`
}

// EventStreamWatcher watches Wikidata EventStreams for relevant changes.
type EventStreamWatcher struct {
	processor  *Processor
	writer     *store.Writer
	reader     *store.Reader
	httpClient *http.Client
	streamURL  string
}

// NewEventStreamWatcher creates a new EventStreams watcher.
func NewEventStreamWatcher(processor *Processor, writer *store.Writer, reader *store.Reader, httpClient *http.Client) *EventStreamWatcher {
	if httpClient == nil {
		httpClient = &http.Client{
			Timeout: 0, // No timeout for SSE
		}
	}
	return &EventStreamWatcher{
		processor:  processor,
		writer:     writer,
		reader:     reader,
		httpClient: httpClient,
		streamURL:  eventStreamURL,
	}
}

// Watch connects to EventStreams and processes changes until the context is cancelled.
// Events are enqueued into a pending_entity table in the database; a background
// goroutine drains the queue in batches via the wbgetentities API.
// A second background goroutine periodically retries entities from the failed_entity table.
func (w *EventStreamWatcher) Watch(ctx context.Context) error {
	// Recover any QIDs left in the processing set from a previous crash.
	if err := w.writer.RecoverProcessing(); err != nil {
		return fmt.Errorf("recover processing set: %w", err)
	}

	ctx, cancel := context.WithCancel(ctx)

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); w.processQueue(ctx) }()
	go func() { defer wg.Done(); w.retryFailedEntities(ctx) }()
	defer func() {
		cancel()
		wg.Wait()
	}()

	for {
		if err := ctx.Err(); err != nil {
			return err
		}

		err := w.connectAndProcess(ctx)
		if err != nil {
			if errors.Is(err, ErrStreamTooOld) {
				return err
			}
			if ctx.Err() != nil {
				return ctx.Err()
			}
			slog.Error("eventstream connection error, reconnecting", "error", err, "delay", reconnectDelay)
			select {
			case <-ctx.Done():
				return ctx.Err()
			case <-time.After(reconnectDelay):
			}
		}
	}
}

func (w *EventStreamWatcher) connectAndProcess(ctx context.Context) (retErr error) {
	streamCtx, cancel := context.WithCancelCause(ctx)
	defer cancel(nil)

	url := w.streamURL

	// sinceTime is used for the gap check: did the stream return data
	// going back far enough? We reconnect with ?since= using the earliest
	// timestamp embedded in the stored SSE event ID rather than sending the
	// raw Last-Event-ID back to EventStreams. This intentionally trades exact
	// cursor replay for a time-based lower bound that we can validate with the
	// gap check below.
	var sinceTime time.Time

	lastEventID, err := w.reader.GetSyncState("last_event_id")
	if err != nil {
		return fmt.Errorf("get last_event_id: %w", err)
	}

	if lastEventID != "" {
		sinceTime = extractEventIDTime(lastEventID)
	} else {
		dumpTime, err := w.reader.GetSyncState("dump_time")
		if err != nil {
			return fmt.Errorf("get dump_time: %w", err)
		}
		if dumpTime == "" {
			return fmt.Errorf("no dump_time; database must be seeded first")
		}
		t, err := time.Parse(time.RFC3339, dumpTime)
		if err != nil {
			return fmt.Errorf("parse dump_time: %w", err)
		}
		sinceTime = t
	}

	if !sinceTime.IsZero() {
		since := sinceTime.UTC().Format(time.RFC3339)
		url = fmt.Sprintf("%s?since=%s", url, since)
	}

	req, err := newRequestWithContext(streamCtx, "GET", url)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Accept", "text/event-stream")

	resp, err := w.httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("connect to eventstream: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("eventstream returned status %d", resp.StatusCode)
	}

	slog.Info("connected to EventStreams")

	// Channel for async DB writes.
	enqueueCh := make(chan struct {
		qid     string
		eventID string
	}, enqueueBatchSize)

	var wg sync.WaitGroup
	wg.Add(1)

	// Background writer: receives QIDs, batches them, writes to DB.
	// Any enqueue failure is treated as fatal for this stream connection so the
	// caller reconnects from the last persisted event ID.
	go func() {
		defer wg.Done()

		batchSet := make(map[string]struct{}, enqueueBatchSize)
		batch := make([]string, 0, enqueueBatchSize)
		var batchLastEventID string
		reportErr := func(err error) {
			cancel(fmt.Errorf("enqueue writer: %w", err))
			_ = resp.Body.Close()
		}
		flushOnce := func() error {
			if len(batchSet) == 0 {
				return nil
			}
			batch = batch[:0]
			for qid := range batchSet {
				batch = append(batch, qid)
			}
			p := w.writer.NewPipe(context.Background())
			p.EnqueueEntities(batch)
			if batchLastEventID != "" {
				p.SetSyncState("last_event_id", batchLastEventID)
			}
			if err := p.Exec(); err != nil {
				return fmt.Errorf("enqueue entities: %w", err)
			}
			slog.Info("enqueued entities", "count", len(batch))
			clear(batchSet)
			batchLastEventID = ""
			return nil
		}
		flush := flushOnce

		// Flush periodically even if batch not full
		ticker := time.NewTicker(enqueueFlushDelay)
		defer ticker.Stop()

		for {
			select {
			case item, ok := <-enqueueCh:
				if !ok {
					// Channel closed, flush remaining
					if err := flush(); err != nil {
						reportErr(fmt.Errorf("flush remaining enqueue buffer: %w", err))
					}
					return
				}
				batchSet[item.qid] = struct{}{}
				batchLastEventID = item.eventID
				if len(batchSet) >= enqueueBatchSize {
					if err := flush(); err != nil {
						slog.Error("error flushing enqueue buffer", "error", err)
						reportErr(err)
						return
					}
				}
			case <-ticker.C:
				if err := flush(); err != nil {
					slog.Error("error flushing enqueue buffer", "error", err)
					reportErr(err)
					return
				}
			}
		}
	}()

	// Ensure channel is closed and writer finishes before returning
	defer func() {
		close(enqueueCh)
		wg.Wait()
		if cause := context.Cause(streamCtx); cause != nil && ctx.Err() == nil && !errors.Is(retErr, ErrStreamTooOld) {
			retErr = cause
		}
	}()

	scanner := bufio.NewScanner(resp.Body)
	scanner.Buffer(make([]byte, 0, 256*1024), 1024*1024) // 1MB max line

	gapChecked := sinceTime.IsZero() // skip gap check if no since param
	var dataLines []string
	for scanner.Scan() {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if err := context.Cause(streamCtx); err != nil && ctx.Err() == nil {
			return err
		}

		line := scanner.Text()

		if strings.HasPrefix(line, "id: ") {
			lastEventID = strings.TrimPrefix(line, "id: ")
			continue
		}

		if strings.HasPrefix(line, "data: ") {
			dataLines = append(dataLines, strings.TrimPrefix(line, "data: "))
			continue
		}

		// Empty line = end of event
		if line == "" && len(dataLines) > 0 {
			data := strings.Join(dataLines, "")
			dataLines = nil

			// Check that the stream goes back far enough on the first event.
			if !gapChecked {
				eventTime := extractEventIDTime(lastEventID)
				if !eventTime.IsZero() {
					gap := eventTime.Sub(sinceTime)
					if gap > streamGapThreshold {
						slog.Warn("stream gap detected", "since", sinceTime.Format(time.RFC3339), "first_event", eventTime.Format(time.RFC3339), "gap", gap)
						p := w.writer.NewPipe(context.Background())
						p.DelSyncStates("dump_time", "last_event_id")
						if err := p.Exec(); err != nil {
							slog.Error("failed to clear sync cursors", "error", err)
						}
						return ErrStreamTooOld
					}
					gapChecked = true
				}
			}

			qid := w.extractEvent(data)
			if qid == "" {
				continue
			}

			select {
			case enqueueCh <- struct {
				qid     string
				eventID string
			}{qid: qid, eventID: lastEventID}:
			case <-streamCtx.Done():
				if err := ctx.Err(); err != nil {
					return err
				}
				return context.Cause(streamCtx)
			case <-ctx.Done():
				return ctx.Err()
			}
		}
	}

	if err := scanner.Err(); err != nil {
		if cause := context.Cause(streamCtx); cause != nil && ctx.Err() == nil {
			return cause
		}
		return fmt.Errorf("scanner error: %w", err)
	}

	if cause := context.Cause(streamCtx); cause != nil && ctx.Err() == nil {
		return cause
	}

	retErr = fmt.Errorf("stream ended")
	return
}

// extractEvent parses an SSE data payload and returns the QID if the event
// is a relevant Wikidata entity change. Returns "" if the event should be
// ignored.
func (w *EventStreamWatcher) extractEvent(data string) string {
	var event recentChangeEvent
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return ""
	}

	if event.Meta.Domain == "canary" {
		return ""
	}
	if event.Wiki != "wikidatawiki" || event.Namespace != 0 {
		return ""
	}
	if !strings.HasPrefix(event.Title, "Q") {
		return ""
	}

	return event.Title
}

// extractEventIDTime extracts the earliest Kafka timestamp from an SSE event ID.
// Event IDs are JSON arrays like:
//
//	[{"topic":"...","partition":0,"offset":-1},{"topic":"...","partition":0,"timestamp":1773879470875}]
//
// Entries without a timestamp (offset-only, for inactive DCs) are ignored.
// Timestamps are milliseconds since epoch. Returns zero time if no entry
// contains a timestamp.
func extractEventIDTime(eventID string) time.Time {
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

// processQueue drains the pending_entity table in batches, fetching entities
// via the wbgetentities API and processing them. It runs until the context
// is cancelled.
func (w *EventStreamWatcher) processQueue(ctx context.Context) {
	ticker := time.NewTicker(queuePollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			w.drainQueue(ctx)
		}
	}
}

func (w *EventStreamWatcher) drainQueue(ctx context.Context) {
	// Pipeline: fetch the next batch from Wikidata while the previous batch's
	// DB writes are still in progress. The processing set guarantees crash
	// safety — any QIDs left there after a crash are recovered at startup.

	type batchResult struct {
		qids            []string
		perEntityErrors map[string]error
		batchErr        error
	}

	// pending holds the in-flight fetch result for the next batch.
	var pending <-chan batchResult

	fetchAsync := func(qids []string) <-chan batchResult {
		ch := make(chan batchResult, 1)
		go func() {
			perEntityErrors, batchErr := w.processor.ProcessEntities(qids)
			ch <- batchResult{qids: qids, perEntityErrors: perEntityErrors, batchErr: batchErr}
		}()
		return ch
	}

	for {
		if ctx.Err() != nil {
			// Wait for in-flight fetch to finish before returning.
			if pending != nil {
				<-pending
			}
			return
		}

		// Claim the next batch while we might still be writing the previous one.
		qids, err := w.writer.ClaimPendingBatch(retryBatchSize)
		if err != nil {
			slog.Error("error claiming pending batch", "error", err)
			if pending != nil {
				<-pending
			}
			return
		}

		// Start fetching the new batch asynchronously (if non-empty).
		var nextPending <-chan batchResult
		if len(qids) > 0 {
			nextPending = fetchAsync(qids)
		}

		// Process the previously fetched batch (DB writes + ack).
		if pending != nil {
			res := <-pending
			if !w.handleBatchResult(ctx, res.qids, res.perEntityErrors, res.batchErr) {
				// Batch-level error (e.g. rate limit). Wait for in-flight too.
				if nextPending != nil {
					res2 := <-nextPending
					// Re-enqueue the second batch since we're bailing out.
					w.requeueOnFailure(res2.qids)
				}
				return
			}
		}

		pending = nextPending

		// Nothing left in the queue?
		if len(qids) == 0 {
			return
		}

		// If the batch wasn't full, drain the last one and stop.
		if len(qids) < retryBatchSize {
			if pending != nil {
				res := <-pending
				w.handleBatchResult(ctx, res.qids, res.perEntityErrors, res.batchErr)
			}
			return
		}
	}
}

// handleBatchResult processes the result of a ProcessEntities call: records
// per-entity failures and acks all QIDs from the processing set. Returns false
// if the batch failed entirely (caller should stop draining).
func (w *EventStreamWatcher) handleBatchResult(ctx context.Context, qids []string, perEntityErrors map[string]error, batchErr error) bool {
	if batchErr != nil {
		var rlErr *RateLimitedError
		if errors.As(batchErr, &rlErr) {
			if !rlErr.Maxlag {
				slog.Warn("rate limited by wikidata", "delay", rlErr.RetryAfter)
			}
			// Re-enqueue so they aren't lost in the processing set.
			w.requeueOnFailure(qids)
			select {
			case <-ctx.Done():
			case <-time.After(rlErr.RetryAfter):
			}
		} else {
			slog.Error("batch fetch failed", "error", batchErr)
			w.requeueOnFailure(qids)
		}
		return false
	}

	p := w.writer.NewPipe(context.Background())
	for _, qid := range qids {
		if perr := perEntityErrors[qid]; perr != nil {
			slog.Error("error processing entity", "qid", qid, "error", perr)
			p.RecordFailedEntity(qid, perr.Error())
		}
	}
	p.AckProcessedEntities(qids)
	if err := p.Exec(); err != nil {
		slog.Error("failed to record failures / ack processed entities", "error", err)
	}

	return true
}

// requeueOnFailure moves QIDs from the processing set back to pending so
// they will be retried on the next drain cycle.
func (w *EventStreamWatcher) requeueOnFailure(qids []string) {
	if len(qids) == 0 {
		return
	}
	p := w.writer.NewPipe(context.Background())
	p.EnqueueEntities(qids)
	p.AckProcessedEntities(qids)
	if err := p.Exec(); err != nil {
		slog.Error("failed to re-enqueue entities after batch failure", "error", err)
	}
}

// retryFailedEntities periodically retries entities from the failed_entity table.
func (w *EventStreamWatcher) retryFailedEntities(ctx context.Context) {
	ticker := time.NewTicker(retryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			qids, err := w.reader.LastFailedEntities(retryBatchSize)
			if err != nil {
				slog.Error("failed to read failed entities", "error", err)
				continue
			}
			if len(qids) == 0 {
				continue
			}

			slog.Info("retrying failed entities", "count", len(qids))

			perEntityErrors, batchErr := w.processor.ProcessEntities(qids)
			if batchErr != nil {
				slog.Error("retry batch failed", "error", batchErr)
				continue
			}

			for _, qid := range qids {
				p := w.writer.NewPipe(context.Background())
				if perr := perEntityErrors[qid]; perr != nil {
					slog.Error("retry failed", "qid", qid, "error", perr)
					p.RecordFailedEntity(qid, perr.Error())
				} else {
					slog.Info("retry succeeded", "qid", qid)
					p.DeleteFailedEntity(qid)
				}
				if err := p.Exec(); err != nil {
					slog.Error("failed to update retry record", "qid", qid, "error", err)
				}
			}
		}
	}
}
