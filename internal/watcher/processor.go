package watcher

import (
	"context"
	"fmt"
	"log/slog"

	"github.com/ekeid/ekeid/internal/store"
)

// Processor handles processing of Wikidata entity changes.
type Processor struct {
	writer *store.Writer
	client *WikidataClient
}

// NewProcessor creates a new entity change processor.
func NewProcessor(writer *store.Writer, client *WikidataClient) *Processor {
	return &Processor{
		writer: writer,
		client: client,
	}
}

// ProcessEntity fetches and processes a single Wikidata entity.
// It upserts the entity if it has external ID claims, or deletes it if not.
func (p *Processor) ProcessEntity(qid string) error {
	results, err := p.ProcessEntities([]string{qid})
	if err != nil {
		return err
	}
	if perr := results[qid]; perr != nil {
		return perr
	}
	return nil
}

// ProcessEntities fetches and processes a batch of Wikidata entities in a
// single API call. Returns a map of QID → error for entities that failed
// individually; a nil map value means success. A non-nil top-level error
// means the entire batch failed (e.g. network error).
func (p *Processor) ProcessEntities(qids []string) (map[string]error, error) {
	if len(qids) == 0 {
		return nil, nil
	}

	results, err := p.client.FetchEntitiesRaw(qids)
	if err != nil {
		return nil, fmt.Errorf("fetch entities batch: %w", err)
	}

	perEntityErrors := make(map[string]error, len(qids))

	var upserts []store.EntityRecord
	var deletes []string
	var errored int

	for _, qid := range qids {
		result, ok := results[qid]
		if !ok {
			perEntityErrors[qid] = fmt.Errorf("entity %s absent from API response", qid)
			errored++
			continue
		}

		if result.Missing {
			deletes = append(deletes, qid)
			continue
		}

		entity, parseErr := ParseEntityJSON(result.Data)
		if parseErr != nil {
			perEntityErrors[qid] = fmt.Errorf("parse entity %s: %w", qid, parseErr)
			errored++
			continue
		}
		if entity == nil {
			// Non-Q entity (property, lexeme) — skip silently.
			continue
		}

		upserts = append(upserts, store.EntityRecord{
			WikidataID: qid,
			Mappings:   entity.Mappings,
		})
	}

	if len(upserts) > 0 || len(deletes) > 0 {
		pipe := p.writer.NewPipe(context.Background())
		for _, rec := range upserts {
			pipe.UpsertEntity(rec)
		}
		for _, qid := range deletes {
			pipe.DeleteEntity(qid)
		}
		if err := pipe.Exec(); err != nil {
			return nil, fmt.Errorf("batch write: %w", err)
		}
	}

	slog.Info("batch processed", "total", len(qids), "upserted", len(upserts), "deleted", len(deletes), "errors", errored)

	return perEntityErrors, nil
}
