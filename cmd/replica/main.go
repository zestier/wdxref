package main

import (
	"context"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ekeid/ekeid/internal/httpencoding"
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

	changelogRetention := store.DefaultChangelogRetention
	if v := os.Getenv("CHANGELOG_RETENTION"); v != "" {
		d, err := time.ParseDuration(v)
		if err != nil {
			h, herr := strconv.Atoi(v)
			if herr != nil {
				slog.Error("invalid CHANGELOG_RETENTION", "value", v, "error", err)
				os.Exit(1)
			}
			d = time.Duration(h) * time.Hour
		}
		changelogRetention = d
	}

	if err := writer.MigrateSchema(); err != nil {
		slog.Error("failed to migrate schema", "error", err)
		os.Exit(1)
	}

	reader := store.NewReader(client)

	encodings := httpencoding.ParseEncodings(os.Getenv("ENCODINGS"))

	replicaClient := replica.NewClient(writer, reader, client.Redis(), upstreamURL, encodings)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	go store.RunChangelogTrimmer(ctx, writer, changelogRetention)

	slog.Info("replica starting", "upstream", upstreamURL, "kvrocks", kvrocksAddr)
	if err := replicaClient.Run(ctx); err != nil && ctx.Err() == nil {
		slog.Error("replica error", "error", err)
		os.Exit(1)
	}

	slog.Info("replica stopped")
}
