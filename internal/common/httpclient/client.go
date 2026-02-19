package httpclient

import (
	"context"
	"io"
	"net"
	"net/http"
	"time"
)

// Client is a pooled HTTP client with connection reuse
// This addresses EM-141: HTTP client connection pooling
type Client struct {
	client  *http.Client
	timeout time.Duration
}

// Config holds HTTP client configuration
type Config struct {
	// Timeout is the request timeout
	Timeout time.Duration

	// MaxIdleConns controls the maximum number of idle connections
	MaxIdleConns int

	// MaxConnsPerHost limits connections per host
	MaxConnsPerHost int

	// MaxIdleConnsPerHost limits idle connections per host
	MaxIdleConnsPerHost int

	// IdleConnTimeout is how long idle connections stay open
	IdleConnTimeout time.Duration

	// TLSHandshakeTimeout is the TLS handshake timeout
	TLSHandshakeTimeout time.Duration

	// ResponseHeaderTimeout is the timeout for response headers
	ResponseHeaderTimeout time.Duration

	// DialTimeout is the TCP dial timeout
	DialTimeout time.Duration

	// KeepAlive is the TCP keep-alive period
	KeepAlive time.Duration
}

// DefaultConfig returns the default HTTP client configuration
// Optimized for high-throughput SMS sending
func DefaultConfig() *Config {
	return &Config{
		Timeout:               30 * time.Second,
		MaxIdleConns:          100,
		MaxConnsPerHost:       20,
		MaxIdleConnsPerHost:   20,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ResponseHeaderTimeout: 10 * time.Second,
		DialTimeout:           10 * time.Second,
		KeepAlive:             30 * time.Second,
	}
}

// New creates a new pooled HTTP client
func New(cfg *Config) *Client {
	if cfg == nil {
		cfg = DefaultConfig()
	}

	transport := &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   cfg.DialTimeout,
			KeepAlive: cfg.KeepAlive,
		}).DialContext,
		MaxIdleConns:          cfg.MaxIdleConns,
		MaxConnsPerHost:       cfg.MaxConnsPerHost,
		MaxIdleConnsPerHost:   cfg.MaxIdleConnsPerHost,
		IdleConnTimeout:       cfg.IdleConnTimeout,
		TLSHandshakeTimeout:   cfg.TLSHandshakeTimeout,
		ResponseHeaderTimeout: cfg.ResponseHeaderTimeout,
		ForceAttemptHTTP2:     true,
	}

	return &Client{
		client: &http.Client{
			Transport: transport,
			Timeout:   cfg.Timeout,
		},
		timeout: cfg.Timeout,
	}
}

// Do executes an HTTP request
func (c *Client) Do(req *http.Request) (*http.Response, error) {
	return c.client.Do(req)
}

// DoWithContext executes an HTTP request with context
func (c *Client) DoWithContext(ctx context.Context, req *http.Request) (*http.Response, error) {
	return c.client.Do(req.WithContext(ctx))
}

// Get performs a GET request
func (c *Client) Get(ctx context.Context, url string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	return c.client.Do(req)
}

// Post performs a POST request
func (c *Client) Post(ctx context.Context, url string, contentType string, body io.Reader) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", contentType)
	return c.client.Do(req)
}

// PostJSON performs a POST request with JSON content type
func (c *Client) PostJSON(ctx context.Context, url string, body io.Reader) (*http.Response, error) {
	return c.Post(ctx, url, "application/json", body)
}

// SetTimeout sets the request timeout
func (c *Client) SetTimeout(timeout time.Duration) {
	c.timeout = timeout
	c.client.Timeout = timeout
}

// Close closes idle connections
// Should be called when the client is no longer needed
func (c *Client) Close() {
	if transport, ok := c.client.Transport.(*http.Transport); ok {
		transport.CloseIdleConnections()
	}
}

// DrainBody reads and closes the response body
// This is important for connection reuse - EM-139 fix
func DrainBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
	}
}

// CloseBody closes the response body
// Always use defer CloseBody(resp) after getting a response - EM-139 fix
func CloseBody(resp *http.Response) {
	if resp != nil && resp.Body != nil {
		resp.Body.Close()
	}
}
