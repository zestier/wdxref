// Command ekeid is a single binary capable of running any combination of the
// ekeid roles: primary, replica, replicator, and api.
//
// Roles are selected via positional arguments (e.g. "ekeid primary api") or,
// if none are given, via the ROLES environment variable (comma-separated).
// Every enabled role runs as a goroutine sharing a single Kvrocks connection
// and a common shutdown context, so one process can do everything or a subset.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/ekeid/ekeid/internal/api"
	"github.com/ekeid/ekeid/internal/httpencoding"
	"github.com/ekeid/ekeid/internal/replica"
	"github.com/ekeid/ekeid/internal/replicate"
	"github.com/ekeid/ekeid/internal/store"
	"github.com/ekeid/ekeid/internal/watcher"
)

const version = "0.1.0"

const (
	roleAPI        = "api"
	rolePrimary    = "primary"
	roleReplica    = "replica"
	roleReplicator = "replicator"
)

func main() {
	roles, err := selectRoles(os.Args[1:], os.Getenv("ROLES"))
	if err != nil {
		slog.Error("invalid roles", "error", err)
		os.Exit(1)
	}
	if len(roles) == 0 {
		slog.Error("no roles specified; pass roles as arguments or set ROLES (api, primary, replica, replicator)")
		os.Exit(1)
	}

	has := func(r string) bool {
		_, ok := roles[r]
		return ok
	}

	// primary and replica are both the sole writer to the store; running them
	// together would mean two independent sources of truth for the same data.
	if has(rolePrimary) && has(roleReplica) {
		slog.Error("primary and replica roles are mutually exclusive (both own writes to the store)")
		os.Exit(1)
	}

	kvrocksAddr := os.Getenv("KVROCKS_ADDR")
	if kvrocksAddr == "" {
		kvrocksAddr = "localhost:6666"
	}

	client, err := store.NewClient(kvrocksAddr)
	if err != nil {
		slog.Error("failed to connect to kvrocks", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	reader := store.NewReader(client)

	// Only the writer roles need a writer, and the schema migration only needs
	// to run once for whichever writer role is enabled.
	var writer *store.Writer
	if has(rolePrimary) || has(roleReplica) {
		writer = store.NewWriter(client)
		if err := writer.MigrateSchema(); err != nil {
			slog.Error("failed to migrate schema", "error", err)
			os.Exit(1)
		}
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("ekeid starting", "version", version, "roles", roleList(roles), "kvrocks", kvrocksAddr)

	var (
		wg       sync.WaitGroup
		errMu    sync.Mutex
		firstErr error
	)
	run := func(name string, fn func(context.Context) error) {
		wg.Add(1)
		go func() {
			defer wg.Done()
			err := fn(ctx)
			if err == nil || errors.Is(err, context.Canceled) {
				return
			}
			errMu.Lock()
			if firstErr == nil {
				firstErr = err
			}
			errMu.Unlock()
			slog.Error("role failed", "role", name, "error", err)
			stop() // cancel shared context so the other roles shut down too
		}()
	}

	if has(rolePrimary) {
		run(rolePrimary, func(ctx context.Context) error { return runPrimary(ctx, writer, reader) })
	}
	if has(roleReplica) {
		run(roleReplica, func(ctx context.Context) error { return runReplica(ctx, client, writer, reader) })
	}

	// The replicator and API are the two HTTP roles. When they resolve to the
	// same listen address they are served from a single http.Server with the
	// replication endpoints nested under the configured base path (default
	// /v1), so one port can serve both without the routes colliding.
	if err := startHTTPRoles(ctx, reader, has(roleReplicator), has(roleAPI), run); err != nil {
		slog.Error("failed to start HTTP roles", "error", err)
		os.Exit(1)
	}

	wg.Wait()

	if firstErr != nil {
		os.Exit(1)
	}
	slog.Info("ekeid stopped")
}

// selectRoles resolves the set of roles from positional arguments, falling back
// to a comma-separated environment value when no arguments are given.
func selectRoles(args []string, env string) (map[string]struct{}, error) {
	raw := args
	if len(raw) == 0 && env != "" {
		raw = strings.Split(env, ",")
	}

	roles := make(map[string]struct{}, len(raw))
	for _, r := range raw {
		r = strings.TrimSpace(r)
		if r == "" {
			continue
		}
		switch r {
		case roleAPI, rolePrimary, roleReplica, roleReplicator:
			roles[r] = struct{}{}
		default:
			return nil, fmt.Errorf("unknown role %q (valid: api, primary, replica, replicator)", r)
		}
	}
	return roles, nil
}

func roleList(roles map[string]struct{}) string {
	ordered := make([]string, 0, len(roles))
	for _, r := range []string{rolePrimary, roleReplica, roleReplicator, roleAPI} {
		if _, ok := roles[r]; ok {
			ordered = append(ordered, r)
		}
	}
	return strings.Join(ordered, ",")
}

// runPrimary ingests Wikidata via the Wikimedia EventStream, seeding from a dump
// first if the store is empty or stale.
func runPrimary(ctx context.Context, writer *store.Writer, reader *store.Reader) error {
	contact := os.Getenv("CONTACT")
	if contact == "" {
		return errors.New("CONTACT env var is required (URL or email for Wikimedia User-Agent policy)")
	}
	transport := watcher.NewThrottleTransport(
		watcher.NewUserAgentTransport(contact, http.DefaultTransport),
	)

	changelogRetention, err := parseChangelogRetention()
	if err != nil {
		return err
	}

	httpClient := &http.Client{Timeout: 5 * time.Minute, Transport: transport}
	wikidataClient := watcher.NewWikidataClient(httpClient)

	processor := watcher.NewProcessor(writer, wikidataClient)
	dumpClient := &http.Client{Timeout: 0, Transport: transport}
	seeder := watcher.NewSeeder(writer, reader, dumpClient, watcher.DumpFormat(os.Getenv("DUMP_FORMAT")))

	go store.RunChangelogTrimmer(ctx, writer, changelogRetention)

	sseClient := &http.Client{Timeout: 0, Transport: transport}
	esWatcher := watcher.NewEventStreamWatcher(processor, writer, reader, sseClient)

	for {
		needsSeed, err := seeder.NeedsSeed()
		if err != nil {
			return fmt.Errorf("failed to check seed state: %w", err)
		}

		if needsSeed {
			slog.Info("database needs seeding")
			if err := seeder.Seed(ctx); err != nil {
				if ctx.Err() != nil {
					return nil
				}
				return fmt.Errorf("seed failed: %w", err)
			}
		} else {
			slog.Info("database is up to date, skipping seed")
		}

		p := writer.NewPipe(context.Background())
		p.SetSyncState("state", "streaming")
		if err := p.Exec(); err != nil {
			return fmt.Errorf("failed to set state: %w", err)
		}

		slog.Info("starting EventStream watcher")
		err = esWatcher.Watch(ctx)
		if errors.Is(err, watcher.ErrStreamTooOld) {
			slog.Info("stream does not go back far enough, reseeding")
			continue
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			return fmt.Errorf("watcher error: %w", err)
		}
		return nil
	}
}

// runReplica pulls snapshots and the changelog stream from an upstream
// replicator and applies them to the local store.
func runReplica(ctx context.Context, client *store.Client, writer *store.Writer, reader *store.Reader) error {
	upstreamURL := os.Getenv("UPSTREAM_URL")
	if upstreamURL == "" {
		return errors.New("UPSTREAM_URL env var is required (e.g. http://primary-replicator:8081)")
	}

	changelogRetention, err := parseChangelogRetention()
	if err != nil {
		return err
	}

	encodings := httpencoding.ParseEncodings(os.Getenv("ENCODINGS"))
	replicaClient := replica.NewClient(writer, reader, client.Redis(), upstreamURL, encodings)

	go store.RunChangelogTrimmer(ctx, writer, changelogRetention)

	slog.Info("replica starting", "upstream", upstreamURL)
	if err := replicaClient.Run(ctx); err != nil && ctx.Err() == nil {
		return fmt.Errorf("replica error: %w", err)
	}
	return nil
}

// startHTTPRoles serves the replicator and/or API roles on a single
// http.Server bound to LISTEN_ADDR (default :8080). When both are enabled they
// share the one address: the API is served at / and the replication endpoints
// are nested under their prefix (default /v1) so the routes don't collide.
// Running the two roles on separate ports is intentionally out of scope — use
// separate processes for that.
func startHTTPRoles(ctx context.Context, reader *store.Reader, replicator, api bool, run func(string, func(context.Context) error)) error {
	if !replicator && !api {
		return nil
	}

	handler, err := buildHTTPHandler(ctx, reader, replicator, api)
	if err != nil {
		return err
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8080"
	}
	name := httpRoleName(replicator, api)

	slog.Info("http server listening", "addr", addr, "role", name)
	run(name, func(ctx context.Context) error {
		return serveHTTP(ctx, newHTTPServer(ctx, addr, handler), name, 30*time.Second)
	})
	return nil
}

// buildHTTPHandler assembles the combined handler for the enabled HTTP roles:
// the replication endpoints (nested under replicatePrefix) and/or the API at
// the root.
func buildHTTPHandler(ctx context.Context, reader *store.Reader, replicator, api bool) (http.Handler, error) {
	mux := http.NewServeMux()

	if replicator {
		replHandler, err := setupReplicateHandler(ctx, reader)
		if err != nil {
			return nil, err
		}
		prefix := replicatePrefix()
		mountReplicate(mux, prefix, replHandler)
		slog.Info("replicator enabled", "replicate_path", prefix+"/replicate")
	}

	if api {
		// The shared server disables the connection-wide WriteTimeout for the
		// replicator's streams and large snapshot transfers, so the API bounds
		// its own responses with a per-request write deadline instead.
		mux.Handle("/", withWriteDeadline(apiWriteTimeout, setupAPIHandler(reader)))
		slog.Info("api enabled", "version", version)
	}

	return mux, nil
}

// mountReplicate mounts a replication handler (which serves /replicate/*) under
// prefix. An empty prefix keeps the legacy root paths; a prefix like /v1 nests
// them and strips it back off before dispatching to the handler.
func mountReplicate(mux *http.ServeMux, prefix string, replHandler http.Handler) {
	if prefix == "" {
		mux.Handle("/replicate/", replHandler)
		return
	}
	mux.Handle(prefix+"/replicate/", http.StripPrefix(prefix, replHandler))
}

// replicatePrefix resolves the path prefix the replication endpoints are nested
// under. It defaults to the API namespace (/v1) so the API and replicator can
// share a port out of the box. REPLICATE_BASE_PATH overrides it, with "/" (or
// empty) selecting the legacy root paths that the dedicated replicator image
// uses for compatibility with existing replicas.
func replicatePrefix() string {
	v, ok := os.LookupEnv("REPLICATE_BASE_PATH")
	if !ok {
		return "/v1"
	}
	v = strings.Trim(strings.TrimSpace(v), "/")
	if v == "" {
		return ""
	}
	return "/" + v
}

// httpRoleName labels the HTTP server for logging based on which roles it hosts.
func httpRoleName(replicator, api bool) string {
	switch {
	case replicator && api:
		return "http"
	case replicator:
		return roleReplicator
	default:
		return roleAPI
	}
}

// setupAPIHandler builds the read-only query and health/stats HTTP handler.
func setupAPIHandler(reader *store.Reader) http.Handler {
	encodings := httpencoding.ParseEncodings(os.Getenv("ENCODINGS"))
	return api.NewServer(reader, version, encodings).Handler()
}

// apiWriteTimeout bounds how long an API response may take to write. It mirrors
// the write timeout the standalone API server used before the roles shared a
// single http.Server, whose connection-wide WriteTimeout must stay disabled for
// the replicator's long-lived streams and large snapshot transfers.
const apiWriteTimeout = 10 * time.Second

// withWriteDeadline applies a per-request write deadline to h. It is used to
// give bounded routes a write timeout even though the shared server leaves the
// connection-wide WriteTimeout disabled. The deadline is set at this outermost
// layer, before any response wrapping (e.g. compression), so it lands on the
// underlying connection; if the ResponseWriter doesn't support deadlines the
// request simply proceeds without one.
func withWriteDeadline(d time.Duration, h http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rc := http.NewResponseController(w)
		_ = rc.SetWriteDeadline(time.Now().Add(d))
		h.ServeHTTP(w, r)
	})
}

// setupReplicateHandler prepares the snapshot generator (starting its
// background loop) and returns the replication HTTP handler.
func setupReplicateHandler(ctx context.Context, reader *store.Reader) (http.Handler, error) {
	snapshotDir := os.Getenv("SNAPSHOT_DIR")
	if snapshotDir == "" {
		snapshotDir = "/data/snapshots"
	}

	snapshotInterval := replicate.DefaultSnapshotInterval
	if v := os.Getenv("SNAPSHOT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			return nil, fmt.Errorf("invalid SNAPSHOT_INTERVAL %q: %w", v, err)
		}
		snapshotInterval = d
	}

	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		return nil, fmt.Errorf("failed to create snapshot dir: %w", err)
	}

	snapshot := replicate.NewSnapshotGenerator(reader, snapshotDir, snapshotInterval)
	go snapshot.Run(ctx)

	slog.Info("replicator snapshot generator started", "snapshot_interval", snapshotInterval)

	encodings := httpencoding.ParseEncodings(os.Getenv("ENCODINGS"))
	return replicate.Handler(reader, snapshot, encodings), nil
}

// newHTTPServer builds an http.Server whose per-request contexts derive from
// ctx so streaming handlers observe shutdown. The connection-wide WriteTimeout
// is disabled because the replicator's SSE streams are long-lived and snapshot
// transfers are large; bounded routes (e.g. the API) set their own per-request
// write deadline instead. The read and idle timeouts still bound slow and idle
// clients.
func newHTTPServer(ctx context.Context, addr string, handler http.Handler) *http.Server {
	return &http.Server{
		Addr:    addr,
		Handler: handler,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0,
		IdleTimeout:  120 * time.Second,
	}
}

// serveHTTP runs an HTTP server until it fails or the context is cancelled,
// then shuts it down gracefully within the given timeout.
func serveHTTP(ctx context.Context, srv *http.Server, name string, shutdownTimeout time.Duration) error {
	errCh := make(chan error, 1)
	go func() {
		if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
			errCh <- err
			return
		}
		errCh <- nil
	}()

	select {
	case err := <-errCh:
		if err != nil {
			return fmt.Errorf("%s server error: %w", name, err)
		}
		return nil
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), shutdownTimeout)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			return fmt.Errorf("%s shutdown error: %w", name, err)
		}
		slog.Info(name + " stopped")
		return nil
	}
}

// parseChangelogRetention reads CHANGELOG_RETENTION as a Go duration, falling
// back to a bare integer interpreted as hours for backward compatibility.
func parseChangelogRetention() (time.Duration, error) {
	v := os.Getenv("CHANGELOG_RETENTION")
	if v == "" {
		return store.DefaultChangelogRetention, nil
	}
	d, err := time.ParseDuration(v)
	if err != nil {
		h, herr := strconv.Atoi(v)
		if herr != nil {
			return 0, fmt.Errorf("invalid CHANGELOG_RETENTION %q: %w", v, err)
		}
		d = time.Duration(h) * time.Hour
	}
	return d, nil
}
