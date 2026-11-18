package dalgo2cosmosdb

import (
	"context"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

type Client struct {
	opts                     Options
	origin                   string
	transport                *http.Transport
	http                     *http.Client
	base                     context.Context
	cancel                   context.CancelFunc
	sem                      chan struct{}
	closeOnce                sync.Once
	mu                       sync.Mutex
	cursors                  map[*Continuation]*cursorState
	cursorBytes, cursorNodes int
}
type budget struct {
	mu                                         sync.Mutex
	requests, requestBytes, bytes, rows, pages int
	failed                                     bool
}
type operation struct {
	c                  *Client
	parent, ctx        context.Context
	budget             *budget
	finalMu            sync.Mutex
	completed, settled bool
	final              func(bool) error
	finalErr           error
}

func fresh(ctx context.Context) error {
	if e := ctx.Err(); e != nil {
		return e
	}
	if deadline, ok := ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	return nil
}
func (op *operation) check() error {
	if e := fresh(op.parent); e != nil {
		return e
	}
	if deadline, ok := op.ctx.Deadline(); ok && !time.Now().Before(deadline) {
		return context.DeadlineExceeded
	}
	if op.c.base.Err() != nil {
		return ErrClosed
	}
	return fresh(op.ctx)
}
func (op *operation) finish(success bool) error {
	op.finalMu.Lock()
	defer op.finalMu.Unlock()
	if op.settled {
		return op.finalErr
	}
	if e := op.check(); e != nil {
		success = false
		op.finalErr = e
	}
	op.settled = true
	if op.completed && op.final != nil {
		if e := op.final(success); e != nil {
			op.finalErr = e
		}
		op.final = nil
	}
	return op.finalErr
}
func (op *operation) complete() {
	op.finalMu.Lock()
	defer op.finalMu.Unlock()
	op.completed = true
	if op.settled && op.final != nil {
		if e := op.final(false); e != nil {
			op.finalErr = e
		}
		op.final = nil
	}
}
func (op *operation) onDelivery(fn func(bool) error) {
	op.finalMu.Lock()
	defer op.finalMu.Unlock()
	op.final = fn
}
func (op *operation) requestBytes(n int) error {
	if e := op.check(); e != nil {
		return e
	}
	b := op.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || b.bytes >= op.c.opts.Limits.MaxResponseBytes || b.rows >= op.c.opts.Limits.MaxRows || b.requests >= op.c.opts.Limits.MaxRequests || n > op.c.opts.Limits.MaxRequestBytes-b.requestBytes {
		b.failed = true
		return ErrLimit
	}
	b.requests++
	b.requestBytes += n
	return nil
}
func (op *operation) responseBytes(n int) error {
	b := op.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || n > op.c.opts.Limits.MaxResponseBytes-b.bytes {
		b.failed = true
		return ErrLimit
	}
	b.bytes += n
	return nil
}
func (op *operation) row() error {
	b := op.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || b.rows >= op.c.opts.Limits.MaxRows {
		b.failed = true
		return ErrLimit
	}
	b.rows++
	return nil
}
func validText(s string, n int) bool {
	return s != "" && len(s) <= n && utf8.ValidString(s) && !strings.ContainsFunc(s, func(r rune) bool { return r < 32 || r == 127 })
}
func validID(s string) bool {
	return validText(s, 1024) && s != "." && s != ".." && !strings.ContainsAny(s, "/\\?#")
}
func validName(s string) bool {
	if len(s) == 0 || len(s) > 128 {
		return false
	}
	for i, b := range []byte(s) {
		if !((b >= 'a' && b <= 'z') || (b >= 'A' && b <= 'Z') || b == '_' || (i > 0 && b >= '0' && b <= '9')) {
			return false
		}
	}
	return true
}
func validField(s string) bool {
	if !validName(s) {
		return false
	}
	switch s {
	case "_etag", "_ts", "_rid", "_self", "_attachments":
		return false
	}
	return true
}
func defaults(l Limits) (Limits, error) {
	if l.Timeout == 0 {
		l.Timeout = 30 * time.Second
	}
	if l.MaxRows == 0 {
		l.MaxRows = 10000
	}
	if l.MaxKeys == 0 {
		l.MaxKeys = 100
	}
	if l.MaxPages == 0 {
		l.MaxPages = 100
	}
	if l.MaxRequests == 0 {
		l.MaxRequests = 100
	}
	if l.MaxResponseBytes == 0 {
		l.MaxResponseBytes = 4 * 1024 * 1024
	}
	if l.MaxRequestBytes == 0 {
		l.MaxRequestBytes = 64 * 1024
	}
	if l.MaxConcurrent == 0 {
		l.MaxConcurrent = 4
	}
	if l.PageSize == 0 {
		l.PageSize = 100
	}
	if l.MaxCursorBytes == 0 {
		l.MaxCursorBytes = 16 * 1024 * 1024
	}
	if l.MaxCursorNodes == 0 {
		l.MaxCursorNodes = 65536
	}
	if l.Timeout < time.Millisecond || l.Timeout > 2*time.Minute || l.MaxRows < 1 || l.MaxRows > 10000 || l.MaxKeys < 1 || l.MaxKeys > 1000 || l.MaxPages < 1 || l.MaxPages > 1000 || l.MaxRequests < 1 || l.MaxRequests > 1000 || l.MaxResponseBytes < 1 || l.MaxResponseBytes > 16*1024*1024 || l.MaxRequestBytes < 1 || l.MaxRequestBytes > 1024*1024 || l.MaxConcurrent < 1 || l.MaxConcurrent > 16 || l.PageSize < 1 || l.PageSize > 1000 || l.MaxCursorBytes < 1 || l.MaxCursorBytes > 16*1024*1024 || l.MaxCursorNodes < 1 || l.MaxCursorNodes > 65536 {
		return Limits{}, ErrConfiguration
	}
	return l, nil
}

// New performs no I/O. It owns only a clone of the supplied transport; TLS
// verification stays enabled, ambient proxies/cookies/redirects are not used.
func New(endpoint string, opts Options) (*Client, error) {
	if len(endpoint) > 4096 {
		return nil, ErrConfiguration
	}
	u, e := url.Parse(endpoint)
	if e != nil || u.Scheme != "https" || u.Opaque != "" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || (u.Path != "" && u.Path != "/") {
		return nil, ErrConfiguration
	}
	if !validID(opts.Database) || !validID(opts.Container) || !validField(opts.PartitionField) || !validText(opts.AuthScopeID, 128) || opts.Authorization == nil || len(opts.Collections) > 128 {
		return nil, ErrConfiguration
	}
	opts.Limits, e = defaults(opts.Limits)
	if e != nil {
		return nil, e
	}
	collections := make(map[string]Collection, len(opts.Collections))
	for name, binding := range opts.Collections {
		if !validText(name, 128) || !validText(binding.Partition, 2048) {
			return nil, ErrConfiguration
		}
		collections[name] = binding
	}
	opts.Collections = collections
	var transport *http.Transport
	if opts.Transport == nil {
		transport = &http.Transport{DialContext: (&net.Dialer{Timeout: 10 * time.Second}).DialContext}
	} else {
		if opts.Transport.TLSClientConfig != nil && opts.Transport.TLSClientConfig.InsecureSkipVerify {
			return nil, ErrConfiguration
		}
		transport = opts.Transport.Clone()
	}
	transport.Proxy = nil
	transport.DisableCompression = true
	transport.DisableKeepAlives = true
	transport.MaxConnsPerHost = opts.Limits.MaxConcurrent
	transport.MaxResponseHeaderBytes = 64 * 1024
	transport.TLSHandshakeTimeout = min(10*time.Second, opts.Limits.Timeout)
	transport.ResponseHeaderTimeout = opts.Limits.Timeout
	base, cancel := context.WithCancel(context.Background())
	u.Path = ""
	u.RawPath = ""
	c := &Client{opts: opts, origin: u.String(), transport: transport, base: base, cancel: cancel, sem: make(chan struct{}, opts.Limits.MaxConcurrent), cursors: make(map[*Continuation]*cursorState)}
	c.http = &http.Client{Transport: transport, Timeout: opts.Limits.Timeout, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, nil
}
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.cancel()
		c.transport.CloseIdleConnections()
		c.mu.Lock()
		clear(c.cursors)
		c.cursorBytes = 0
		c.cursorNodes = 0
		c.mu.Unlock()
	})
	return nil
}
func (c *Client) ConfiguredMetadata() ConfiguredMetadata {
	bindings := make(map[string]Collection, len(c.opts.Collections))
	for k, v := range c.opts.Collections {
		bindings[k] = v
	}
	return ConfiguredMetadata{c.opts.Database, c.opts.Container, c.opts.PartitionField, true, c.opts.Limits, bindings}
}

// Physical worker admission is retained until ignored credential/transport work
// actually settles, even after a caller stops waiting. No caller record/codec
// is reachable from worker; such callbacks belong on the caller path only.
func run[T any](c *Client, parent context.Context, deadline time.Time, prepare func(*operation) error, fn func(*operation) (T, error)) (T, error) {
	return runMapped(c, parent, deadline, prepare, fn, nil)
}
func runMapped[T any](c *Client, parent context.Context, deadline time.Time, prepare func(*operation) error, fn func(*operation) (T, error), beforeAck func(T) error) (zero T, err error) {
	if e := fresh(parent); e != nil {
		return zero, e
	}
	if c.base.Err() != nil {
		return zero, ErrClosed
	}
	ctx, cancel := context.WithDeadline(parent, deadline)
	stopClose := context.AfterFunc(c.base, cancel)
	op := &operation{c: c, parent: parent, ctx: ctx, budget: &budget{}}
	stopFinish := context.AfterFunc(ctx, func() { _ = op.finish(false) })
	defer func() { stopClose(); cancel(); stopFinish(); _ = op.finish(false) }()
	if e := op.check(); e != nil {
		return zero, e
	}
	select {
	case c.sem <- struct{}{}:
	default:
		return zero, ErrBusy
	}
	if prepare != nil {
		if e := prepare(op); e != nil {
			if cancelled := op.check(); cancelled != nil {
				e = cancelled
			}
			op.complete()
			<-c.sem
			return zero, e
		}
	}
	if e := op.check(); e != nil {
		op.complete()
		<-c.sem
		return zero, e
	}
	type outcome struct {
		value T
		err   error
	}
	done := make(chan outcome, 1)
	go func() {
		defer func() { <-c.sem }()
		v, e := fn(op)
		if e != nil {
			_ = op.finish(false)
		}
		op.complete()
		done <- outcome{v, e}
	}()
	select {
	case <-ctx.Done():
		if e := op.check(); e != nil {
			return zero, e
		}
		return zero, ctx.Err()
	case out := <-done:
		if e := op.check(); e != nil {
			return zero, e
		}
		if out.err != nil {
			return zero, out.err
		}
		if beforeAck != nil {
			if e := beforeAck(out.value); e != nil {
				if cancelled := op.check(); cancelled != nil {
					return zero, cancelled
				}
				return zero, e
			}
			if e := op.check(); e != nil {
				return zero, e
			}
		}
		if e := op.finish(true); e != nil {
			return zero, e
		}
		return out.value, nil
	}
}
