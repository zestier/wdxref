// Package replicate implements the replication protocol for serving
// snapshots and streaming changelog events to downstream replicas.
package replicate

import (
	"bytes"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/goccy/go-json"
)

const (
	// KeepAliveInterval is how often the stream endpoint sends a keepalive
	// comment when there are no new events.
	KeepAliveInterval = 15 * time.Second

	// StreamReadCount is the maximum number of changelog entries read per
	// XREAD call.
	StreamReadCount = 100

	// DefaultSnapshotInterval is how often to re-generate the snapshot file.
	DefaultSnapshotInterval = 24 * time.Hour

	// SnapshotRetryInterval is the delay between snapshot generation
	// attempts when there is no valid snapshot available.
	SnapshotRetryInterval = 30 * time.Second

	// MaxConnectionDuration is how long a single SSE connection is allowed
	// to stay open before the server closes it, forcing a reconnect.
	MaxConnectionDuration = 15 * time.Minute

	// DefaultMaxStreamEvents is the maximum number of events returned by one
	// stream request when the server configuration is not overridden. It is
	// effectively unbounded; such a request is instead bounded by its timeout.
	DefaultMaxStreamEvents int64 = math.MaxInt64

	// SnapshotFrameSize is the target number of entity lines per zstd frame in
	// the snapshot. Each frame begins with a checkpoint control line and is
	// independently decompressible.
	SnapshotFrameSize = 10000

	// SnapshotControlVersion is the current version for control JSON lines
	// embedded in snapshot streams.
	SnapshotControlVersion = 1

	// SnapshotControlTypeCheckpoint marks the first line in each zstd frame.
	SnapshotControlTypeCheckpoint = "checkpoint"

	// SnapshotControlTypeDone marks the final line in the snapshot stream.
	SnapshotControlTypeDone = "done"
)

// StreamLimits bounds one replication stream request. The event and timeout
// limits are independent; a zero timeout means do not block for new events.
type StreamLimits struct {
	MaxEvents  int64
	MaxTimeout time.Duration
}

// DefaultStreamLimits returns the default server-side bounds for stream
// requests.
func DefaultStreamLimits() StreamLimits {
	return StreamLimits{
		MaxEvents:  DefaultMaxStreamEvents,
		MaxTimeout: MaxConnectionDuration,
	}
}

// SnapshotLineType classifies snapshot lines before parsing.
type SnapshotLineType int

const (
	SnapshotLineTypeInvalid SnapshotLineType = iota
	SnapshotLineTypeEntity
	SnapshotLineTypeControl
)

// SnapshotControl is the shared header for all control JSON lines.
type SnapshotControl struct {
	Type    string `json:"type"`
	Version int    `json:"v"`
}

// SnapshotCheckpoint is emitted as the first line in each zstd frame.
type SnapshotCheckpoint struct {
	Type           string `json:"type"`
	Version        int    `json:"v"`
	Offset         int64  `json:"offset"`
	EntitiesBefore int64  `json:"entities_before"`
}

// SnapshotDone is emitted as the final line in the snapshot stream.
type SnapshotDone struct {
	Type     string `json:"type"`
	Version  int    `json:"v"`
	StreamID string `json:"stream_id"`
	Entities int64  `json:"entities"`
}

// ClassifySnapshotLine distinguishes entity lines from control JSON lines.
func ClassifySnapshotLine(line []byte) SnapshotLineType {
	if len(line) == 0 {
		return SnapshotLineTypeInvalid
	}

	switch first := line[0]; {
	case first == '{':
		return SnapshotLineTypeControl
	case first >= '0' && first <= '9':
		return SnapshotLineTypeEntity
	default:
		return SnapshotLineTypeInvalid
	}
}

// AppendSnapshotCheckpointLine appends a checkpoint control line.
func AppendSnapshotCheckpointLine(dst []byte, offset, entitiesBefore int64) ([]byte, error) {
	return appendSnapshotJSONLine(dst, SnapshotCheckpoint{
		Type:           SnapshotControlTypeCheckpoint,
		Version:        SnapshotControlVersion,
		Offset:         offset,
		EntitiesBefore: entitiesBefore,
	})
}

// AppendSnapshotDoneLine appends the final snapshot completion control line.
func AppendSnapshotDoneLine(dst []byte, streamID string, entities int64) ([]byte, error) {
	return appendSnapshotJSONLine(dst, SnapshotDone{
		Type:     SnapshotControlTypeDone,
		Version:  SnapshotControlVersion,
		StreamID: streamID,
		Entities: entities,
	})
}

func appendSnapshotJSONLine(dst []byte, value any) ([]byte, error) {
	data, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	dst = append(dst[:0], data...)
	dst = append(dst, '\n')
	return dst, nil
}

// AppendSnapshotEntityLine appends a single snapshot line in the form
// "<qid> <raw-mappings-json>\n".
func AppendSnapshotEntityLine(dst []byte, qid int64, rawMappings string) []byte {
	dst = strconv.AppendInt(dst[:0], qid, 10)
	dst = append(dst, ' ')
	dst = append(dst, rawMappings...)
	dst = append(dst, '\n')
	return dst
}

// ParseSnapshotEntityLine parses a snapshot line in the form
// "<qid> <raw-mappings-json>".
func ParseSnapshotEntityLine(line []byte) (int64, string, error) {
	sep := bytes.IndexByte(line, ' ')
	if sep <= 0 || sep == len(line)-1 {
		return 0, "", fmt.Errorf("invalid snapshot line")
	}

	qid, err := strconv.ParseInt(string(line[:sep]), 10, 64)
	if err != nil {
		return 0, "", fmt.Errorf("parse snapshot qid: %w", err)
	}
	return qid, string(line[sep+1:]), nil
}

// ParseSnapshotControl parses and validates a control JSON line.
func ParseSnapshotControl(line []byte) (SnapshotControl, error) {
	if ClassifySnapshotLine(line) != SnapshotLineTypeControl {
		return SnapshotControl{}, fmt.Errorf("invalid snapshot control line")
	}

	var control SnapshotControl
	if err := json.Unmarshal(line, &control); err != nil {
		return SnapshotControl{}, fmt.Errorf("decode snapshot control: %w", err)
	}
	if control.Type == "" {
		return SnapshotControl{}, fmt.Errorf("missing snapshot control type")
	}
	if control.Version != SnapshotControlVersion {
		return SnapshotControl{}, fmt.Errorf("unsupported snapshot control version %d", control.Version)
	}
	return control, nil
}

// ParseSnapshotCheckpoint parses a checkpoint control line.
func ParseSnapshotCheckpoint(line []byte) (SnapshotCheckpoint, error) {
	control, err := ParseSnapshotControl(line)
	if err != nil {
		return SnapshotCheckpoint{}, err
	}
	if control.Type != SnapshotControlTypeCheckpoint {
		return SnapshotCheckpoint{}, fmt.Errorf("unexpected snapshot control type %q", control.Type)
	}

	var checkpoint SnapshotCheckpoint
	if err := json.Unmarshal(line, &checkpoint); err != nil {
		return SnapshotCheckpoint{}, fmt.Errorf("decode snapshot checkpoint: %w", err)
	}
	if checkpoint.Offset < 0 {
		return SnapshotCheckpoint{}, fmt.Errorf("invalid snapshot checkpoint offset %d", checkpoint.Offset)
	}
	if checkpoint.EntitiesBefore < 0 {
		return SnapshotCheckpoint{}, fmt.Errorf("invalid snapshot checkpoint entities_before %d", checkpoint.EntitiesBefore)
	}
	return checkpoint, nil
}

// ParseSnapshotDone parses the final snapshot completion control line.
func ParseSnapshotDone(line []byte) (SnapshotDone, error) {
	control, err := ParseSnapshotControl(line)
	if err != nil {
		return SnapshotDone{}, err
	}
	if control.Type != SnapshotControlTypeDone {
		return SnapshotDone{}, fmt.Errorf("unexpected snapshot control type %q", control.Type)
	}

	var done SnapshotDone
	if err := json.Unmarshal(line, &done); err != nil {
		return SnapshotDone{}, fmt.Errorf("decode snapshot done: %w", err)
	}
	if done.StreamID == "" {
		return SnapshotDone{}, fmt.Errorf("missing snapshot done stream_id")
	}
	if done.Entities < 0 {
		return SnapshotDone{}, fmt.Errorf("invalid snapshot done entities %d", done.Entities)
	}
	return done, nil
}

// FormatStreamChangeData formats a compact change payload for SSE data lines.
// The wire format is "<qid> [<raw-mappings-json>]". The stream ID is carried
// separately in the SSE `id:` field (per the EventSource spec).
// Presence of mappings means upsert; absence means delete.
func FormatStreamChangeData(qid int64, rawMappings string) string {
	qidStr := strconv.FormatInt(qid, 10)
	var builder strings.Builder
	builder.Grow(len(qidStr) + len(rawMappings) + 1)
	builder.WriteString(qidStr)
	if rawMappings != "" {
		builder.WriteByte(' ')
		builder.WriteString(rawMappings)
	}
	return builder.String()
}

// ParseStreamChangeData parses a compact change payload from an SSE data line.
// The wire format is "<qid> [<raw-mappings-json>]". Presence of mappings means
// upsert; absence means delete.
func ParseStreamChangeData(data string) (qid int64, rawMappings string, err error) {
	if data == "" {
		err = fmt.Errorf("invalid change event")
		return
	}

	qidPart := data
	if sep := strings.IndexByte(data, ' '); sep >= 0 {
		if sep == 0 || sep == len(data)-1 {
			err = fmt.Errorf("invalid change event")
			return
		}
		qidPart = data[:sep]
		rawMappings = data[sep+1:]
	}

	qid, err = strconv.ParseInt(qidPart, 10, 64)
	if err != nil {
		err = fmt.Errorf("parse change qid: %w", err)
		return
	}
	return
}
