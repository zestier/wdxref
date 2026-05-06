package model

import "time"

// LookupResult is returned by API lookups.
type LookupResult struct {
	WikidataID  int64
	RawMappings string // canonical JSON array of "P<id>:<value>" entries
}

// HealthInfo holds lightweight health data (no expensive COUNT queries).
type HealthInfo struct {
	DatabaseSize  int64
	DumpTime      time.Time
	LastEventID   string
	LastEventSync time.Time
	State         string
	SchemaMatch   bool
}

// DBStats holds database statistics (expensive COUNT queries).
type DBStats struct {
	HealthInfo
	EntityCount     int64
	PendingCount    int64
	ProcessingCount int64
	FailedCount     int64
	StreamLength    int64
	OldestEvent     time.Time
	NewestEvent     time.Time
}
