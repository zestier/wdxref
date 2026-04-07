package store

// ChangeEvent represents a single entity change in the changelog stream.
type ChangeEvent struct {
	ID          string // Redis stream ID (e.g. "1617000000000-0")
	QID         int64  // Wikidata entity numeric ID
	RawMappings string // canonical mappings JSON; empty for deletes
}

// SnapshotEntity represents a single entity in a replication snapshot.
type SnapshotEntity struct {
	QID         int64
	RawMappings string // canonical mappings JSON
}
