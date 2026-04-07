package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/ekeid/ekeid/internal/store"
	"github.com/ekeid/ekeid/internal/watcher"
)

func main() {
	contact := os.Getenv("CONTACT")
	if contact == "" {
		slog.Error("CONTACT env var is required (URL or email for Wikimedia User-Agent policy)")
		os.Exit(1)
	}
	transport := watcher.NewThrottleTransport(
		watcher.NewUserAgentTransport(contact, http.DefaultTransport),
	)

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

	writer := store.NewWriter(client)

	if err := writer.MigrateSchema(); err != nil {
		slog.Error("failed to migrate schema", "error", err)
		os.Exit(1)
	}

	httpClient := &http.Client{Timeout: 5 * time.Minute, Transport: transport}
	wikidataClient := watcher.NewWikidataClient(httpClient)

	processor := watcher.NewProcessor(writer, wikidataClient)
	dumpClient := &http.Client{Timeout: 0, Transport: transport}
	seeder := watcher.NewSeeder(writer, dumpClient, watcher.DumpFormat(os.Getenv("DUMP_FORMAT")))

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	sseClient := &http.Client{Timeout: 0, Transport: transport}
	esWatcher := watcher.NewEventStreamWatcher(processor, writer, sseClient)

	for {
		needsSeed, err := seeder.NeedsSeed()
		if err != nil {
			slog.Error("failed to check seed state", "error", err)
			os.Exit(1)
		}

		if needsSeed {
			slog.Info("database needs seeding")
			if err := seeder.Seed(ctx); err != nil {
				if ctx.Err() != nil {
					break
				}
				slog.Error("seed failed", "error", err)
				os.Exit(1)
			}
		} else {
			slog.Info("database is up to date, skipping seed")
		}

		if err := writer.SetSyncState("state", "streaming"); err != nil {
			slog.Error("failed to set state", "error", err)
			os.Exit(1)
		}

		slog.Info("starting EventStream watcher")
		err = esWatcher.Watch(ctx)
		if errors.Is(err, watcher.ErrStreamTooOld) {
			slog.Info("stream does not go back far enough, reseeding")
			continue
		}
		if err != nil && !errors.Is(err, context.Canceled) {
			slog.Error("watcher error", "error", err)
			os.Exit(1)
		}
		break
	}

	slog.Info("primary stopped")
}
