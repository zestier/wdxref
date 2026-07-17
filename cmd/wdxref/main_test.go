package main

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

// deadlineRecorder is a ResponseWriter that records the write deadline set on
// it via http.ResponseController.
type deadlineRecorder struct {
	http.ResponseWriter
	deadline time.Time
}

func (d *deadlineRecorder) SetWriteDeadline(t time.Time) error {
	d.deadline = t
	return nil
}

func TestWithWriteDeadline(t *testing.T) {
	rec := &deadlineRecorder{ResponseWriter: httptest.NewRecorder()}

	served := false
	before := time.Now()
	h := withWriteDeadline(10*time.Second, http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
		served = true
	}))
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/health", nil))

	if !served {
		t.Fatal("wrapped handler was not called")
	}
	if rec.deadline.IsZero() {
		t.Fatal("write deadline was not set on the response writer")
	}
	if got := rec.deadline.Sub(before); got < 9*time.Second || got > 11*time.Second {
		t.Errorf("deadline set ~%v from now, want ~10s", got)
	}
}

// pathSpy records the request path its inner mux dispatched to, so we can
// assert that mountReplicate hands the replication handler the paths it expects
// (i.e. that any prefix has been stripped).
func pathSpy() (http.Handler, *string) {
	got := new(string)
	mux := http.NewServeMux()
	mux.HandleFunc("GET /replicate/health", func(w http.ResponseWriter, r *http.Request) {
		*got = r.URL.Path
		w.WriteHeader(http.StatusOK)
	})
	return mux, got
}

func TestMountReplicate(t *testing.T) {
	t.Run("fixed v1 path strips before dispatch", func(t *testing.T) {
		mux := http.NewServeMux()
		spy, seen := pathSpy()
		mountReplicate(mux, spy)

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/v1/replicate/health", nil))

		if rec.Code != http.StatusOK {
			t.Fatalf("status = %d, want 200", rec.Code)
		}
		if *seen != "/replicate/health" {
			t.Errorf("handler saw %q, want /replicate/health (prefix should be stripped)", *seen)
		}
	})

	t.Run("legacy path 404s", func(t *testing.T) {
		mux := http.NewServeMux()
		spy, _ := pathSpy()
		mountReplicate(mux, spy)

		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/replicate/health", nil))

		if rec.Code != http.StatusNotFound {
			t.Errorf("status = %d, want 404", rec.Code)
		}
	})
}

// TestCombinedRouting verifies that when both roles share a mux the way
// buildHTTPHandler wires them up — replication nested under a prefix and the API
// as the root catch-all — requests are dispatched to the right handler and the
// catch-all does not shadow the replication subtree.
func TestCombinedRouting(t *testing.T) {
	// apiSpy stands in for the API handler mounted at "/"; it records the path
	// it received and reports which handler served the request.
	apiSeen := new(string)
	apiSpy := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		*apiSeen = r.URL.Path
		w.Header().Set("X-Handler", "api")
		w.WriteHeader(http.StatusOK)
	})

	replSpy, replSeen := pathSpy()

	mux := http.NewServeMux()
	mountReplicate(mux, replSpy)
	mux.Handle("/", apiSpy)

	cases := []struct {
		name        string
		path        string
		wantHandler string // "api" or "replicate"
		wantSeen    string // path the handler should observe
	}{
		{name: "replication endpoint", path: "/v1/replicate/health", wantHandler: "replicate", wantSeen: "/replicate/health"},
		{name: "api health under same namespace", path: "/v1/health", wantHandler: "api", wantSeen: "/v1/health"},
		{name: "api lookup", path: "/v1/lookup/P345/tt0111161", wantHandler: "api", wantSeen: "/v1/lookup/P345/tt0111161"},
		{name: "legacy replicate root falls through to api", path: "/replicate/health", wantHandler: "api", wantSeen: "/replicate/health"},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			*apiSeen, *replSeen = "", ""
			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, tc.path, nil))

			if rec.Code != http.StatusOK {
				t.Fatalf("status = %d, want 200", rec.Code)
			}
			switch tc.wantHandler {
			case "replicate":
				if *replSeen != tc.wantSeen {
					t.Errorf("replicate handler saw %q, want %q", *replSeen, tc.wantSeen)
				}
				if *apiSeen != "" {
					t.Errorf("api handler also saw %q, expected replicate to handle it", *apiSeen)
				}
			case "api":
				if *apiSeen != tc.wantSeen {
					t.Errorf("api handler saw %q, want %q", *apiSeen, tc.wantSeen)
				}
				if *replSeen != "" {
					t.Errorf("replicate handler also saw %q, expected api to handle it", *replSeen)
				}
			}
		})
	}
}
