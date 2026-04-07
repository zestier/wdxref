package store

import "errors"

// schemaVersion identifies the current data layout. Bump this value whenever
// the key structure, encoding format, or data model changes. On startup the
// Writer issues FLUSHDB when the stored version does not match, and the
// Reader reports a degraded state so the API can return 503.
const schemaVersion = "v11-entities-hash"

// ErrSchemaMismatch is returned by Reader methods when the database was
// created with a different schema version. The API server should map this
// to an HTTP 503 response.
var ErrSchemaMismatch = errors.New("database schema mismatch")

// SchemaVersion returns the current schema version string.
func SchemaVersion() string {
	return schemaVersion
}
