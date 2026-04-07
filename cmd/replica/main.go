package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"

	"github.com/ekeid/ekeid/internal/replica"
	"github.com/ekeid/ekeid/internal/store"
)

func main() {
	kvrocksAddr := os.Getenv("KVROCKS_ADDR")
	if kvrocksAddr == "" {
		kvrocksAddr = "localhost:6666"
	}

	upstreamURL := os.Getenv("UPSTREAM_URL")
	if upstreamURL == "" {
		slog.Error("UPSTREAM_URL env var is required (e.g. http://primary-replicator:8081)")
		os.Exit(1)
	}

	client, err := store.NewClient(kvrocksAddr)
	if err != nil {
		slog.Error("failed to connect to kvrocks", "error", err)
		os.Exit(1)
	}
	defer client.Close()

	writer := store.NewWriter(client)

	if v, _ := strconv.ParseBool(os.Getenv("DISABLE_CHANGELOG")); v {
		writer.SetNoChangelog(true)
		slog.Info("changelog disabled for this replica")
	}

	if err := writer.MigrateSchema(); err != nil {
		slog.Error("failed to migrate schema", "error", err)
		os.Exit(1)
	}

	replicaClient := replica.NewClient(writer, client.Redis(), upstreamURL)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	slog.Info("replica starting", "upstream", upstreamURL, "kvrocks", kvrocksAddr)
	if err := replicaClient.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("replica error", "error", err)
		os.Exit(1)
	}

	slog.Info("replica stopped")
}
