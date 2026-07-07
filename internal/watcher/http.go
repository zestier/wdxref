package watcher

import (
	"context"
	"log/slog"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// UserAgentTransport is an http.RoundTripper that injects a User-Agent
// header into every outgoing request. Wrapping the transport ensures the
// header is set regardless of how requests are created.
//
// The User-Agent follows the format required by the Wikimedia Foundation
// User-Agent Policy:
// https://foundation.wikimedia.org/wiki/Policy:Wikimedia_Foundation_User-Agent_Policy
type UserAgentTransport struct {
	userAgent string
	base      http.RoundTripper
}

// NewUserAgentTransport creates a transport that sets User-Agent on every
// request. contact must be a URL or email so Wikimedia can reach the
// operator. base is the underlying transport (use http.DefaultTransport
// if nil).
func NewUserAgentTransport(contact string, base http.RoundTripper) *UserAgentTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &UserAgentTransport{
		userAgent: "WdxrefBot/0.1.0 (" + contact + ")",
		base:      base,
	}
}

// RoundTrip implements http.RoundTripper.
func (t *UserAgentTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	req = req.Clone(req.Context())
	req.Header.Set("User-Agent", t.userAgent)
	return t.base.RoundTrip(req)
}

// Wikimedia robot policy limits for unauthenticated bots.
// https://wikitech.wikimedia.org/wiki/Robot_policy
const (
	wikimediaRateLimit          = 5               // max requests per second (unauthenticated)
	wikimediaBurst              = 1               // max concurrent requests (unauthenticated)
	wikimediaExpensiveThreshold = 1 * time.Second // response time that triggers cooldown
	wikimediaExpensiveCooldown  = 5 * time.Second // wait after an expensive response
	wikimediaMaxlag             = 5.0             // seconds; lower = more generous to Wikimedia servers
)

// ThrottleTransport is an http.RoundTripper that enforces Wikimedia robot
// policy rate limits: serialised requests (concurrency=1), capped at
// 5 req/sec for unauthenticated bots, with a 5-second cooldown after any
// response that takes longer than 1 second (expensive endpoint).
type ThrottleTransport struct {
	mu      sync.Mutex
	limiter *rate.Limiter
	base    http.RoundTripper

	expensiveThreshold time.Duration
	expensiveCooldown  time.Duration
}

// NewThrottleTransport creates a ThrottleTransport configured for
// Wikimedia's unauthenticated robot policy. base is the underlying
// transport (typically a *UserAgentTransport).
func NewThrottleTransport(base http.RoundTripper) *ThrottleTransport {
	if base == nil {
		base = http.DefaultTransport
	}
	return &ThrottleTransport{
		limiter:            rate.NewLimiter(rate.Limit(wikimediaRateLimit), wikimediaBurst),
		base:               base,
		expensiveThreshold: wikimediaExpensiveThreshold,
		expensiveCooldown:  wikimediaExpensiveCooldown,
	}
}

// RoundTrip implements http.RoundTripper with rate limiting,
// concurrency=1, and expensive endpoint cooldown.
func (t *ThrottleTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	ctx := req.Context()

	t.mu.Lock()
	defer t.mu.Unlock()

	if err := t.limiter.Wait(ctx); err != nil {
		return nil, err
	}

	start := time.Now()
	resp, err := t.base.RoundTrip(req)
	elapsed := time.Since(start)

	if err != nil {
		return nil, err
	}

	threshold := t.expensiveThreshold
	if threshold <= 0 {
		threshold = wikimediaExpensiveThreshold
	}
	cooldown := t.expensiveCooldown
	if cooldown <= 0 {
		cooldown = wikimediaExpensiveCooldown
	}

	if elapsed > threshold {
		slog.Warn("wikimedia request slow, applying cooldown", "elapsed", elapsed.Truncate(time.Millisecond), "threshold", threshold, "cooldown", cooldown)
		select {
		case <-ctx.Done():
		case <-time.After(cooldown):
		}
	}

	return resp, nil
}

// newRequest creates an HTTP request. User-Agent is set by
// UserAgentTransport on the *http.Client, not here.
func newRequest(method, url string) (*http.Request, error) {
	return http.NewRequest(method, url, nil)
}

// newRequestWithContext creates an HTTP request with context.
func newRequestWithContext(ctx context.Context, method, url string) (*http.Request, error) {
	return http.NewRequestWithContext(ctx, method, url, nil)
}
