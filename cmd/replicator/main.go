package main

import (
	"context"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ekeid/ekeid/internal/httpencoding"
	"github.com/ekeid/ekeid/internal/replicate"
	"github.com/ekeid/ekeid/internal/store"
)

func main() {
	kvrocksAddr := os.Getenv("KVROCKS_ADDR")
	if kvrocksAddr == "" {
		kvrocksAddr = "localhost:6666"
	}

	addr := os.Getenv("LISTEN_ADDR")
	if addr == "" {
		addr = ":8081"
	}

	snapshotDir := os.Getenv("SNAPSHOT_DIR")
	if snapshotDir == "" {
		snapshotDir = "/data/snapshots"
	}

	snapshotInterval := replicate.DefaultSnapshotInterval
	if v := os.Getenv("SNAPSHOT_INTERVAL"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			slog.Error("invalid SNAPSHOT_INTERVAL", "value", v, "error", err)
			os.Exit(1)
		}
		snapshotInterval = d
	}

	changelogRetention := replicate.DefaultChangelogRetention
	if v := os.Getenv("CHANGELOG_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			// Try parsing as hours integer for convenience.
			h, herr := strconv.Atoi(v)
			if herr != nil {
				slog.Error("invalid CHANGELOG_RETENTION", "value", v, "error", err)
				os.Exit(1)
			}
			d = time.Duration(h) * time.Hour
		}
		changelogRetention = d
	}

	if changelogRetention < 2*snapshotInterval {
		slog.Warn("changelog retention is less than 2x snapshot interval; replicas may enter reset loops",
			"retention", changelogRetention, "snapshot_interval", snapshotInterval)
	}

	client, err := store.NewClient(kvrocksAddr)
	if err != nil {
		slog.Error("failed to connect to kvrocks", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	reader := store.NewReader(client)
	writer := store.NewWriter(client)

	// Ensure snapshot directory exists.
	if err := os.MkdirAll(snapshotDir, 0o755); err != nil {
		slog.Error("failed to create snapshot dir", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	snapshot := replicate.NewSnapshotGenerator(reader, snapshotDir, snapshotInterval)

	// Run snapshot generator in background.
	go snapshot.Run(ctx)

	// Run changelog trimmer in background.
	go replicate.RunChangelogTrimmer(ctx, writer, changelogRetention)

	encodings := httpencoding.ParseEncodings(os.Getenv("ENCODINGS"))

	handler := replicate.Handler(reader, snapshot, encodings)

	httpServer := &http.Server{
		Addr:    addr,
		Handler: handler,
		BaseContext: func(_ net.Listener) context.Context {
			return ctx
		},
		ReadTimeout:  5 * time.Second,
		WriteTimeout: 0, // SSE streams have no write deadline
		IdleTimeout:  120 * time.Second,
	}

	go func() {
		slog.Info("replicator starting", "addr", addr, "snapshot_interval", snapshotInterval, "changelog_retention", changelogRetention)
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			slog.Error("server error", "error", err)
			os.Exit(1)
		}
	}()

	<-ctx.Done()
	slog.Info("shutting down")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		slog.Error("shutdown error", "error", err)
		os.Exit(1)
	}

	slog.Info("replicator stopped")
}
