# Bounded Replication Streams

**Status:** Proposed

## Summary

Extend the existing `GET /replicate/stream` endpoint so one SSE protocol can support both long-lived replication and request-oriented polling. Clients may request a smaller event limit or a shorter timeout, while the replicator enforces independent server-side maximums.

There will be no separate polling endpoint or polling-specific response format. Every response remains an SSE response with the existing cursor, reset, change-event, compression, and reconnect semantics.

## Motivation

A long-lived SSE connection is efficient for near-real-time replication, but it is not suitable for every deployment. Some clients and network environments prefer short-lived requests to reduce the number of held connections or to fit load-balancer and worker execution models.

A second JSON polling protocol would duplicate cursor handling, event encoding, reset behavior, compression, and replica application logic. It would also encourage buffering a complete response before returning it. The existing SSE protocol already supports incremental writes, so bounded SSE requests provide the same operational choice without creating a second replication contract.

## Goals

- Preserve the existing SSE wire format and cursor semantics.
- Allow a client to bound the number of events returned by one request.
- Allow a client to bound the total lifetime of one request.
- Flush events incrementally as they are read from the changelog.
- Let `timeout=0` act as an immediate, non-blocking poll.
- Enforce independent server-side maximums for events and request lifetime.
- Preserve reset and `503 Service Unavailable` behavior.
- Allow the replica client to use the same event application path for streaming and polling.

## Non-goals

- Adding a second polling endpoint or JSON event format.
- Guaranteeing that polling uses fewer Redis operations than SSE.
- Providing exactly-once delivery. Replication remains at-least-once with an atomic local entity update and cursor update.
- Changing snapshot generation or snapshot resume behavior.
- Allowing clients to bypass server-side limits.

## Proposed API

The endpoint remains:

```text
GET /replicate/stream?since=<stream-id>&limit=<n>&timeout=<duration>
```

The existing `since` query parameter and `Last-Event-ID` header retain their current precedence and meaning. `since` is an exclusive cursor: events after that ID are returned.

### Request fields

| Field | Meaning |
|---|---|
| `since` | Exclusive changelog cursor. If absent, `Last-Event-ID` is used. If both are absent, the request requires a snapshot and receives a reset event. |
| `limit` | Maximum number of change events in this response. If absent, use the configured server maximum. |
| `timeout` | Maximum total lifetime of this request, including time blocked waiting for new events. If absent, use the configured server maximum. Go duration syntax is used, such as `0s`, `5s`, or `15m`. |

`limit` must be a positive integer. `timeout` must be zero or greater. Malformed or negative values return `400 Bad Request`.

A request value greater than the configured server maximum is clamped to that maximum. This makes the endpoint tolerant of clients that use a generic large value while ensuring the server remains authoritative.

The effective values are therefore conceptually:

```text
effective limit   = min(request limit or configured max events,
                         configured max events)
effective timeout = min(request timeout or configured max timeout,
                         configured max timeout)
```

The implementation should represent omitted values explicitly rather than relying on integer or duration sentinel values.

### Examples

Normal bounded streaming with the defaults:

```text
GET /replicate/stream?since=1710000000000-0
```

Short long-poll request:

```text
GET /replicate/stream?since=1710000000000-0&limit=100&timeout=15s
```

Immediate poll for up to 1,000 available events:

```text
GET /replicate/stream?since=1710000000000-0&limit=1000&timeout=0s
```

## Server configuration

Add independent replicator settings:

| Variable | Default | Description |
|---|---:|---|
| `REPLICATION_MAX_EVENTS` | *(unbounded)* | Maximum number of change events sent by one stream request. |
| `REPLICATION_MAX_TIMEOUT` | `15m` | Maximum total lifetime of one stream request. |

The two settings are intentionally independent. A zero maximum timeout means non-blocking requests; it does not implicitly alter the event maximum. An administrator can set both values when they want both a short request lifetime and a small response.

The default event maximum is effectively unbounded (`math.MaxInt64`), so a default stream is bounded only by `REPLICATION_MAX_TIMEOUT`. This preserves the previous continuous-streaming behavior, where a connection is recycled on the max connection duration rather than an event count.

Setting `REPLICATION_MAX_TIMEOUT=0` is a supported, intentional configuration: it makes every stream request non-blocking, effectively turning off SSE. The endpoint then behaves as a request/response polling API that returns currently available events and closes, which makes it trivial to place behind a load balancer for polling-mode replication.

Configuration values must be positive for `REPLICATION_MAX_EVENTS` and non-negative for `REPLICATION_MAX_TIMEOUT`. Invalid configuration should fail startup with an actionable error. The configured timeout should also be subject to a hard implementation limit if the HTTP server or deployment has a known upper bound.

The existing 15-minute connection duration becomes the default value of `REPLICATION_MAX_TIMEOUT` or is replaced by it. There should be one effective request deadline rather than separate, competing timeout mechanisms.

## Response behavior

All responses use the current SSE headers and event format:

```text
: connected
retry: 3000

id: <stream-id>
event: change
data: <qid> [<raw-mappings-json>]

```

The server writes and flushes each batch as soon as it is read. It does not wait until the response reaches `limit` before sending data and does not accumulate all events in memory.

The handler should maintain a request deadline:

```text
request deadline = request start + effective timeout
```

For each Redis read, it passes the remaining time as the `XREAD BLOCK` duration. The read count is the smaller of the remaining event budget and `StreamReadCount`. After every batch, it updates the cursor and event count, flushes the response, and checks the deadline and event limit.

The request ends when any of these conditions occurs:

- `limit` events have been sent.
- The timeout expires.
- `timeout=0` finds no currently available events.
- The client cancels the request.
- The backing store returns an error.

A bounded request that reaches its limit or timeout closes cleanly after the last complete SSE event. A continuous client may reconnect using the last event ID. The existing client behavior for the current maximum connection duration can be treated as this same normal reconnect boundary.

For `timeout=0`, the handler performs non-blocking reads. It may drain multiple immediately available batches until the event limit is reached, but it must not block waiting for a new event.

An empty successful poll still receives the normal SSE preamble and then closes with no change events. This keeps the response format uniform and lets clients distinguish a valid empty poll from a transport failure.

## Error and reset semantics

The existing coverage check remains unchanged:

- A cursor that is older than the retained stream, or whose coverage cannot be proven, produces an SSE `reset` event and closes the response.
- A missing initial cursor continues to require a snapshot.
- A Kvrocks failure produces `503 Service Unavailable`, never an empty successful response and never a destructive reset.
- The reset payload continues to include the reason and, when available, the upstream state.

Parameter validation happens before the coverage check so malformed requests receive `400` consistently.

## Replica client changes

The replica client should retain one event parser and one event application function for all stream requests. In particular, applying a change and updating `last_replicated_id` must remain one local write transaction regardless of whether the upstream request was long-lived, long-polling, or immediate.

The client needs a transport policy for its request cycle:

- Continuous mode requests the stream with the configured maximums or omitted parameters.
- Polling mode requests `timeout=0` and reconnects after a configurable polling interval.
- Long-poll mode requests a finite `timeout` and reconnects after each clean response.

A clean EOF after a bounded response is normal completion, not an upstream error. An unexpected EOF in a request that was expected to remain open should retain the existing retry behavior. In all modes, a reset causes the existing snapshot recovery path, and a `503` causes retry without flushing local data.

The polling interval should be separate from the server timeout. It controls client-side request frequency and should be context-aware so shutdown is prompt. This setting can be added with the replica-client configuration work rather than encoded into the server endpoint.

## Implementation plan

1. Add a stream request-options parser in `internal/replicate` for `limit` and `timeout`, including validation, defaults, clamping, and request-deadline calculation.
2. Update `ServeStream` to use the effective event budget and total request deadline while preserving incremental SSE writes.
3. Replace or reconcile `MaxConnectionDuration` with the configured maximum timeout so there is one authoritative request lifetime.
4. Pass replication stream limits into `replicate.Handler` from `cmd/wdxref/main.go`, parsing `REPLICATION_MAX_EVENTS` and `REPLICATION_MAX_TIMEOUT` during replicator setup.
5. Refactor the replica event application logic so bounded clean EOFs share the same cursor and mutation path as the current SSE stream.
6. Add replica configuration for continuous, long-poll, or immediate-poll request behavior only if polling support is being delivered in the same change. The server-side endpoint remains usable by external clients independently.
7. Update the configuration table and replication documentation in `README.md`.
8. Add focused tests before broadening the implementation.

## Test plan

### Request parsing and configuration

- Missing `limit` and `timeout` resolve to configured maximums.
- A smaller request value is preserved.
- A larger request value is clamped.
- Zero timeout is accepted.
- Zero or negative limits, negative timeouts, malformed durations, and malformed integers are rejected as specified.
- Invalid server configuration prevents handler startup.

### Stream behavior

- Events are flushed before the event limit is reached.
- A response stops after exactly `limit` events.
- A zero-timeout request returns currently available events without blocking.
- An empty zero-timeout poll returns a valid SSE preamble and closes.
- A finite timeout closes after the total deadline, even when events arrive continuously.
- A client cancellation stops the read and response promptly.
- Existing keepalives are retained for requests that have remaining timeout budget.

### Recovery and protocol compatibility

- `since` remains exclusive and `Last-Event-ID` fallback remains intact.
- Reset responses remain unchanged.
- Store unavailability remains `503`.
- Compression works for bounded responses as it does for continuous responses.
- Existing stream clients continue to parse changes and reconnect.
- Replica cursor advancement remains atomic with entity application.
- Replica clean EOF handling does not lose or duplicate changes across reconnects.

## Operational considerations

Polling reduces the number of simultaneously held connections, but it can increase HTTP request and Redis command overhead if clients poll too frequently. Client documentation should recommend a reasonable interval for immediate polling and prefer finite long polling where the deployment permits it.

The event limit bounds response size and the timeout bounds connection lifetime independently. Operators can tune either dimension without changing the protocol or requiring a different endpoint. Metrics and logs should include the effective limit, effective timeout, events sent, and termination reason so polling and streaming behavior can be distinguished operationally.

## Open decisions

- Whether to expose a separate client-side mode setting in the first implementation or deliver the server capability first for external consumers.
- Whether to retain the name `MaxConnectionDuration` as an internal compatibility constant or rename it to reflect the request timeout semantics.
