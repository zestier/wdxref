package watcher

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

func TestThrottleTransportRateLimit(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	// Use a rate of 20/sec so the test stays fast while still observing waits.
	tt := &ThrottleTransport{
		limiter: rate.NewLimiter(rate.Limit(20), 1),
		base:    server.Client().Transport,
	}
	client := &http.Client{Transport: tt}

	const n = 5
	start := time.Now()
	for range n {
		req, err := http.NewRequest("GET", server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}
		resp, err := client.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
	}
	elapsed := time.Since(start)

	// 5 requests at 20/sec with burst=1: first fires immediately, then 4 waits
	// of 50ms each = ~200ms minimum.
	if elapsed < 150*time.Millisecond {
		t.Errorf("5 requests at rate=20/sec completed in %v, expected ≥150ms", elapsed)
	}
}

func TestThrottleTransportExpensiveEndpointCooldown(t *testing.T) {
	// Server that takes longer than the custom threshold to respond.
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(20 * time.Millisecond)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tt := &ThrottleTransport{
		limiter:            rate.NewLimiter(rate.Limit(100), 1), // high rate so limiter doesn't add delay
		base:               server.Client().Transport,
		expensiveThreshold: 10 * time.Millisecond,
		expensiveCooldown:  30 * time.Millisecond,
	}
	client := &http.Client{Transport: tt}

	// First request: triggers expensive endpoint detection.
	start := time.Now()
	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	firstDuration := time.Since(start)

	// The first request should take at least ~20ms (response) + 30ms (cooldown).
	if firstDuration < 45*time.Millisecond {
		t.Errorf("expensive request completed in %v, expected ≥45ms (20ms response + 30ms cooldown)", firstDuration)
	}
}

func TestThrottleTransportConcurrency(t *testing.T) {
	// Track max concurrent requests seen by the server.
	var (
		mu         = make(chan struct{}, 1)
		concurrent int
		maxSeen    int
	)
	mu <- struct{}{}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-mu
		concurrent++
		if concurrent > maxSeen {
			maxSeen = concurrent
		}
		mu <- struct{}{}

		time.Sleep(5 * time.Millisecond)

		<-mu
		concurrent--
		mu <- struct{}{}

		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tt := &ThrottleTransport{
		limiter: rate.NewLimiter(rate.Limit(100), 1), // high rate so limiter doesn't add delay
		base:    server.Client().Transport,
	}
	client := &http.Client{Transport: tt}

	// Launch multiple goroutines trying to make requests concurrently.
	done := make(chan struct{}, 5)
	for range 5 {
		go func() {
			req, _ := http.NewRequest("GET", server.URL, nil)
			resp, err := client.Do(req)
			if err == nil {
				resp.Body.Close()
			}
			done <- struct{}{}
		}()
	}

	for range 5 {
		<-done
	}

	if maxSeen > 1 {
		t.Errorf("max concurrent requests = %d, want 1 (throttle should serialise)", maxSeen)
	}
}

func TestThrottleTransportRespectsContextCancellation(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	tt := &ThrottleTransport{
		limiter: rate.NewLimiter(rate.Limit(1), 1),
		base:    server.Client().Transport,
	}
	client := &http.Client{Transport: tt}

	// Consume the one available token.
	req, _ := http.NewRequest("GET", server.URL, nil)
	resp, _ := client.Do(req)
	resp.Body.Close()

	// Next request with an already-cancelled context should fail fast.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	req, _ = http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	_, err := client.Do(req)
	if err == nil {
		t.Error("expected error from cancelled context, got nil")
	}
}
