package store

import (
	"context"
	"fmt"
	"time"

	"github.com/redis/go-redis/v9"
)

// Client wraps a Redis-compatible client for connecting to Kvrocks.
type Client struct {
	rdb *redis.Client
}

// NewClient connects to a Kvrocks (or Redis-compatible) server at the given address.
func NewClient(addr string) (*Client, error) {
	return newClient(addr, &redis.Options{
		Addr:         addr,
		Protocol:     2, // RESP2 for Kvrocks compatibility
		DialTimeout:  5 * time.Second,
		ReadTimeout:  60 * time.Second,
		WriteTimeout: 60 * time.Second,
		PoolSize:     10,
	})
}

// NewTestClient connects to a Redis-compatible server with minimal timeouts
// and no retries, suitable for unit tests that intentionally close the server.
func NewTestClient(addr string) (*Client, error) {
	return newClient(addr, &redis.Options{
		Addr:            addr,
		Protocol:        2,
		DialTimeout:     50 * time.Millisecond,
		ReadTimeout:     100 * time.Millisecond,
		WriteTimeout:    100 * time.Millisecond,
		MaxRetries:      0,
		PoolSize:        2,
		MinRetryBackoff: -1, // disable retry backoff
	})
}

func newClient(addr string, opts *redis.Options) (*Client, error) {
	rdb := redis.NewClient(opts)
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		rdb.Close()
		return nil, fmt.Errorf("connect to kvrocks at %s: %w", addr, err)
	}
	return &Client{rdb: rdb}, nil
}

// Redis returns the underlying redis.Client for direct access.
func (c *Client) Redis() *redis.Client {
	return c.rdb
}

// Close closes the connection.
func (c *Client) Close() error {
	return c.rdb.Close()
}
