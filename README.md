# wdxref

A fast, self-hosted cache of Wikidata external-identifier cross-references (`wd` + `xref`).

A self-hosted service that builds and maintains a local cache of external identifier mappings from [Wikidata](https://www.wikidata.org/). Look up any entity by one external ID (IMDb, TMDB, MusicBrainz, etc.) and get back its Wikidata QID along with all other known external IDs for that entity.

## Why

Wikidata stores millions of cross-references between external identifiers — for example, a single movie entity might link its IMDb ID, TMDB ID, Rotten Tomatoes slug, and dozens more. If your application needs to translate between these ID systems ("given this IMDb ID, what's the TMDB ID?"), the usual options are live SPARQL queries or Wikidata API calls — both of which can be slow, rate-limited, and unpredictable under load.

wdxref gives you a local, always-ready lookup service. It maintains a [Kvrocks](https://kvrocks.apache.org/) database (a Redis-compatible, disk-backed key-value store) that is seeded from a full Wikidata dump and kept up to date in real time via the Wikimedia EventStreams API. Lookups resolve in single-digit milliseconds with no external network calls, making it suitable for user-facing applications where responsiveness matters.

The typical use case is adding wdxref to a stack that needs fast, reliable ID remapping — for example, a media server that wants to cross-reference content across IMDb, TMDB, TVDB, and MusicBrainz without hammering upstream APIs or risking throttling during traffic spikes.

> **Planned feature:** Built-in property aliasing, so you can look up by friendly names like `imdb` or `tmdb` instead of needing to know the Wikidata property IDs (`P345`, `P4947`, etc.).

## Architecture

wdxref is a single binary (`cmd/wdxref`) that can run any combination of four roles, each connecting to a shared [Kvrocks](https://kvrocks.apache.org/) instance via TCP. Roles are selected via positional arguments (e.g. `wdxref primary api`) or, when none are given, the comma-separated `ROLES` environment variable. Every enabled role runs as a goroutine sharing one Kvrocks connection and a common shutdown context, so a single process can run everything or just a subset. The recommended image for new deployments is the combined `wdxref` Dockerfile target, which ships no default roles so you select them via arguments or the `ROLES` variable. The per-role Dockerfile targets (`api`, `primary`, `replicator`, `replica`) are kept for the one-container-per-role model.

- **Primary** (`primary`) — Seeds the database from the latest Wikidata JSON dump (gzip or bzip2), then subscribes to the [Wikimedia EventStreams](https://stream.wikimedia.org/) SSE feed to process entity changes in real time. Changed entities are enqueued and fetched from the Wikidata API in batches. Failed fetches are retried automatically with backoff. All changes are appended to a Redis Stream changelog.
- **Replicator** (`replicator`) — Reads from Kvrocks and serves replication endpoints over HTTP. Generates periodic zstd-compressed snapshots containing entity lines (`qid + raw mappings JSON`) plus a small number of JSON control lines for resumable range requests and completion handoff, and streams changelog events via SSE using the same raw payloads. Trims the changelog after a configurable retention period (default: 7 days).
- **Replica** (`replica`) — Syncs from an upstream replicator into a local Kvrocks instance. On first run, downloads a snapshot. Then connects to the SSE changelog stream for incremental updates. Writes to the local changelog for chaining (a replica's replicator can serve another replica). Mutually exclusive with the `primary` role, since both own writes to the store.
- **API** (`api`) — A read-only HTTP server that serves lookup queries against Kvrocks. Includes CORS support, health checks, and statistics.

The `replicator` and `api` roles both serve HTTP. When enabled together they share a single listen address: the API is served at the root and the replication endpoints are nested under a configurable prefix (default `/v1`, see `REPLICATE_BASE_PATH`) so their routes don't collide.

```
Primary machine                      Replica machine
┌──────────────────────────┐         ┌──────────────────────────┐
│  primary ──writes──→ kvrocks       │  replica ──writes──→ kvrocks
│  replicator ─reads─→ kvrocks       │  replicator ─reads─→ kvrocks
│  api ──reads──→ kvrocks            │  api ──reads──→ kvrocks
└──────────────────────────┘         └──────────────────────────┘
        │ HTTP (replication)                  │ HTTP (replication)
        └────────────────────────────────────┘
              Chainable: each replica's replicator
              can serve further downstream replicas
```

## API Endpoints

All endpoints are prefixed with `/v1`.

### Lookup by External ID

```
GET /v1/lookup/{property}/{value}
```

Look up an entity by a Wikidata property and value. The property must be in `P` format (e.g. `P345` for IMDb ID).

**Example** — Find the entity with IMDb ID `tt0111161`:

```bash
curl http://localhost:8080/v1/lookup/P345/tt0111161
```

```json
{
  "mappings": {
    "Q172241": [
      "P345:tt0111161",
      "P4947:278",
      "P4835:2095",
      "P8013:the-shawshank-redemption"
    ]
  }
}
```

The response is an object with a `mappings` field keyed by Wikidata QID. Each value is an array of `P<id>:<value>` strings for that entity. When multiple entities claim the same external ID, all are returned as separate keys. An empty `mappings` object is returned when no entity matches.

### Lookup by Wikidata ID

```
GET /v1/lookup/{qid}
```

Look up all external ID mappings for a Wikidata entity.

**Example:**

```bash
curl http://localhost:8080/v1/lookup/Q172241
```

### Health

```
GET /v1/health
```

Returns service status, database size, dump time, and last event sync time. Suitable for monitoring and liveness probes.

### Statistics

```
GET /v1/stats
```

Returns entity, pending, and failed entity counts along with database metadata. This endpoint executes `COUNT` queries and is more expensive than `/v1/health`.

> **Note:** If exposing the API publicly, consider restricting access to `/v1/stats` (and potentially `/v1/health`) to operators only — for example, via a reverse proxy. The stats endpoint is expensive and the operational details it exposes are generally not relevant to end users.

## Getting Started

### Prerequisites

- An OCI-compatible container runtime (e.g. [Podman](https://podman.io/) or [Docker](https://docs.docker.com/get-docker/)) with Compose support

### Configuration

Configuration is done through environment variables:

| Variable | Service | Default | Description |
|---|---|---|---|
| `CONTACT` | Primary | *(required)* | URL or email for the [Wikimedia User-Agent policy](https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy) |
| `ROLES` | All | *(none)* | Comma-separated roles to run when none are passed as arguments (`api`, `primary`, `replica`, `replicator`) |
| `KVROCKS_ADDR` | All | `localhost:6666` | Address of the Kvrocks instance |
| `DUMP_FORMAT` | Primary | `gz` | Dump compression format: `gz` (~150 GB) or `bz2` (~100 GB) |
| `LISTEN_ADDR` | API, Replicator | `:8080` | Address the shared HTTP server listens on |
| `REPLICATE_BASE_PATH` | Replicator | `/v1` | Path prefix the replication endpoints are nested under; `/` (or empty) serves them at the legacy root (`/replicate/*`) |
| `UPSTREAM_URL` | Replica | *(required)* | URL of the upstream replicator (e.g. `http://primary-replicator:8081`) |
| `SNAPSHOT_DIR` | Replicator | `/data/snapshots` | Directory for snapshot files |
| `SNAPSHOT_INTERVAL` | Replicator | `24h` | How often to regenerate the snapshot (Go duration format) |
| `CHANGELOG_RETENTION` | Primary, Replica | `168h` | How long to retain changelog entries; `0` trims entries on every tick (effectively disabled for leaf replicas); also accepts a plain integer as hours |
| `ENCODINGS` | API, Replica, Replicator | `zstd,gzip` | Comma-separated list of compression encodings to use; set to empty to disable compression |

### Running

There are three example Compose files under `examples/` for different deployment scenarios:

| Directory | Use case |
|---|---|
| `examples/primary/` | **Primary with replication** — full stack with replicator for serving downstream replicas |
| `examples/standalone/` | **Standalone** — primary + API only, no replication |
| `examples/replica/` | **Replica** — syncs from an upstream replicator, serves API, and can chain further |

#### Standalone (simplest)

```bash
export CONTACT="you@example.com"  # or a URL, per Wikimedia policy
docker compose -f examples/standalone/docker-compose.yml up -d
```

#### Primary with replication

```bash
export CONTACT="you@example.com"
docker compose -f examples/primary/docker-compose.yml up -d
```

The replicator will be available on port 8081 for downstream replicas.

#### Replica

```bash
export UPSTREAM_URL="http://primary-host:8081"
docker compose -f examples/replica/docker-compose.yml up -d
```

Replicas are chainable — each replica runs its own replicator, so further downstream replicas can sync from it.

---

On first start the primary will download and process the latest Wikidata dump. This is a large download (100+ GB compressed) and initial seeding will take a significant amount of time. Once seeded, the primary switches to real-time streaming and subsequent restarts will not re-download the dump unless it is deemed necessary.

The API will be available at `http://localhost:8080`.

### Building from Source

Requires Go 1.26+.

```bash
# Build the single wdxref binary
go build -o wdxref ./cmd/wdxref
```

Select one or more roles at runtime via arguments or the `ROLES` environment variable:

```bash
# Run a single role
./wdxref api

# Combine roles in one process (api served at /, replication under /v1)
./wdxref replicator api

# Equivalent, via the environment
ROLES=replicator,api ./wdxref
```

## Running Tests

```bash
go test ./...
```

## How It Works

1. **Seeding** — The primary downloads the latest `latest-all.json.gz` (or `.bz2`) dump from [Wikidata](https://dumps.wikimedia.org/wikidatawiki/entities/). It streams and decompresses the dump, extracting all external-id claims for every entity. These are inserted into Kvrocks in batches. The dump's ETag is tracked to support resumable downloads if the connection is interrupted.

2. **Streaming** — After seeding completes, the primary connects to the [Wikimedia EventStreams](https://stream.wikimedia.org/v2/stream/recentchange) SSE endpoint starting from the stored dump timestamp, or on reconnect from the earliest timestamp embedded in the last stored SSE event ID. Wikidata entity edits (namespace 0) are collected, deduplicated, and enqueued into a pending set. A background goroutine drains this set by fetching entities from the Wikidata `wbgetentities` API in batches of up to 50 and upserting their external ID mappings. All changes are appended to a Redis Stream (`changelog`).

3. **Replication** — The replicator periodically generates a zstd-compressed snapshot of all entities and serves it over HTTP. Most snapshot lines are `qid + raw mappings JSON`, so replicas can apply them without decoding and re-encoding identical mappings. The snapshot also embeds JSON control lines: frame-start checkpoints advertise legal byte-range resume offsets, and a final `done` line carries the stream ID to continue from once the snapshot is fully applied. Resume uses standard HTTP `Range` and `If-Range` over the compressed bytes. The SSE changelog stream uses the same raw payload strategy for incremental updates. Replicas connect to these endpoints to sync data. Each replica writes changes to its own local changelog, enabling chainable replication. Snapshots are eventually consistent — they are generated via a non-atomic scan, so the snapshot may reflect a mix of states. Replicas converge to a consistent state by replaying the changelog stream after applying the snapshot.

4. **Resilience** — If an entity fetch fails, it is recorded and retried periodically with backoff. If the EventStream connection drops, the primary reconnects automatically using a timestamp-derived lower bound from the last stored SSE event ID and verifies that the returned stream still covers that point. If the stream cannot go far enough back in time to cover the last sync point, a full reseed is triggered. Replicas retry on disconnect and fall back to a full snapshot if the changelog no longer covers their position.

5. **Serving** — The API server reads from Kvrocks and serves lookup queries. Responses are cached for 1 hour (`Cache-Control: public, max-age=3600`).

## License

This project is released into the public domain under [CC0 1.0 Universal](LICENSE).

## Disclosure

AI coding agents (GitHub Copilot) were used in the development of this project.
