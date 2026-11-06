// Package client talks to a faultline cluster. It finds the leader by following redirects,
// retries through timeouts, crashes and elections, and gives every write a (client, seq) pair so
// a retried write is applied exactly once.
package client

import (
	"context"
	"fmt"
	"io"
	"math/rand/v2"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

// Client is one session. It is not safe for concurrent use: a session has one request in flight.
type Client struct {
	addrs []string
	id    uint64
	seq   uint64
	cur   int
	hc    *http.Client
	// Attempts counts every request sent, retries included.
	Attempts int
}

// New returns a client for the cluster at addrs (host:port).
func New(addrs []string, attemptTimeout time.Duration) *Client {
	return &Client{
		addrs: addrs,
		id:    rand.Uint64()>>1 | 1, // never 0, which means anonymous
		cur:   rand.IntN(len(addrs)),
		hc: &http.Client{
			Timeout:       attemptTimeout,
			CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
		},
	}
}

// Get returns a key's value ("" if unset). It reflects every write that completed before it.
func (c *Client) Get(ctx context.Context, key string) (string, error) {
	return c.do(ctx, http.MethodGet, "/kv/"+url.PathEscape(key), "", false)
}

// Put sets a key and returns the new value.
func (c *Client) Put(ctx context.Context, key, value string) (string, error) {
	return c.do(ctx, http.MethodPut, "/kv/"+url.PathEscape(key), value, true)
}

// Append appends to a key and returns the value after.
func (c *Client) Append(ctx context.Context, key, value string) (string, error) {
	return c.do(ctx, http.MethodPost, "/kv/"+url.PathEscape(key)+"?op=append", value, true)
}

func (c *Client) do(ctx context.Context, method, path, body string, write bool) (string, error) {
	if write {
		c.seq++
	}
	backoff := 10 * time.Millisecond
	for {
		if err := ctx.Err(); err != nil {
			return "", err
		}
		req, err := http.NewRequestWithContext(ctx, method, "http://"+c.addrs[c.cur]+path, strings.NewReader(body))
		if err != nil {
			return "", err
		}
		if write {
			req.Header.Set("Faultline-Client", strconv.FormatUint(c.id, 10))
			req.Header.Set("Faultline-Seq", strconv.FormatUint(c.seq, 10))
		}
		c.Attempts++
		resp, err := c.hc.Do(req)
		if err != nil {
			c.next() // down, paused or partitioned: try another node
			sleep(ctx, backoff)
			continue
		}
		b, _ := io.ReadAll(resp.Body)
		resp.Body.Close()
		switch resp.StatusCode {
		case http.StatusOK:
			return string(b), nil
		case http.StatusTemporaryRedirect:
			if !c.follow(resp.Header.Get("Location")) {
				c.next()
			}
		case http.StatusServiceUnavailable:
			c.next()
			sleep(ctx, backoff)
			backoff = min(2*backoff, 200*time.Millisecond)
		default:
			return "", fmt.Errorf("%s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(b)))
		}
	}
}

func (c *Client) follow(location string) bool {
	u, err := url.Parse(location)
	if err != nil {
		return false
	}
	for i, a := range c.addrs {
		if a == u.Host {
			c.cur = i
			return true
		}
	}
	return false
}

func (c *Client) next() { c.cur = (c.cur + 1) % len(c.addrs) }

func sleep(ctx context.Context, d time.Duration) {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-ctx.Done():
	case <-t.C:
	}
}
