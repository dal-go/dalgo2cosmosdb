package dalgo2cosmosdb

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"math"
	"mime"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

const apiVersion = "2020-11-05"

func (c *Client) path(id string) string {
	path := "/dbs/" + c.opts.Database + "/colls/" + c.opts.Container + "/docs"
	if id != "" {
		path += "/" + id
	}
	return path
}
func (c *Client) session(partition string, s *Session) (string, error) {
	if s == nil {
		return "", nil
	}
	if s.owner != c || s.partition != partition || !validHeader(s.token, 16384) {
		return "", ErrRequest
	}
	return s.token, nil
}
func validHeader(s string, max int) bool {
	if len(s) == 0 || len(s) > max {
		return false
	}
	for _, b := range []byte(s) {
		if b < 32 || b > 126 {
			return false
		}
	}
	return true
}
func oneHeader(h http.Header, name string, max int) (string, error) {
	values := h.Values(name)
	if len(values) > 1 {
		return "", ErrResponse
	}
	if len(values) == 0 || values[0] == "" {
		return "", nil
	}
	if !validHeader(values[0], max) {
		return "", ErrResponse
	}
	return values[0], nil
}
func (c *Client) metadata(h http.Header, partition string) (Metadata, error) {
	var m Metadata
	charge, e := oneHeader(h, "x-ms-request-charge", 64)
	if e != nil {
		return m, e
	}
	if charge != "" {
		if !json.Valid([]byte(charge)) || charge[0] == '-' || charge[0] < '0' || charge[0] > '9' {
			return m, ErrResponse
		}
		v, e := strconv.ParseFloat(charge, 64)
		if e != nil || math.IsInf(v, 0) || math.IsNaN(v) || v > 1e12 {
			return m, ErrResponse
		}
		m.RequestCharge = json.Number(charge)
		m.RequestChargePresent = true
	}
	m.ActivityID, e = oneHeader(h, "x-ms-activity-id", 128)
	if e != nil {
		return m, e
	}
	m.ETag, e = oneHeader(h, "etag", 1024)
	if e != nil {
		return m, e
	}
	token, e := oneHeader(h, "x-ms-session-token", 16384)
	if e != nil {
		return m, e
	}
	if token != "" {
		m.Session = &Session{owner: c, partition: partition, token: token}
	}
	return m, nil
}

type wireResponse struct {
	status       int
	body         any
	meta         Metadata
	continuation string
	itemCount    *int
}

func (c *Client) request(op *operation, method, id, partition string, body []byte, continuation, session string, pageSize int) (out wireResponse, err error) {
	if e := op.requestBytes(len(body)); e != nil {
		return out, e
	}
	resource := "dbs/" + c.opts.Database + "/colls/" + c.opts.Container
	if id != "" {
		resource += "/docs/" + id
	}
	credential, e := c.opts.Authorization(op.ctx, AuthRequest{method, resource, c.opts.AuthScopeID})
	if cancelled := op.check(); cancelled != nil {
		return out, cancelled
	}
	if e != nil || credential.header == "" {
		return out, ErrTransport
	}
	u, e := url.Parse(c.origin)
	if e != nil {
		return out, ErrConfiguration
	}
	u.Path = c.path(id)
	u.RawPath = ""
	req, e := http.NewRequestWithContext(op.ctx, method, u.String(), bytes.NewReader(body))
	if e != nil {
		return out, ErrRequest
	}
	req.Header.Set("Authorization", credential.header)
	req.Header.Set("x-ms-date", time.Now().UTC().Format(http.TimeFormat))
	req.Header.Set("x-ms-version", apiVersion)
	req.Header.Set("Accept", "application/json")
	req.Header.Set("Accept-Encoding", "identity")
	pk, _ := json.Marshal([]string{partition})
	req.Header.Set("x-ms-documentdb-partitionkey", string(pk))
	if session != "" {
		req.Header.Set("x-ms-consistency-level", "Session")
		req.Header.Set("x-ms-session-token", session)
	}
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/query+json")
		req.Header.Set("x-ms-documentdb-isquery", "true")
		req.Header.Set("x-ms-max-item-count", strconv.Itoa(pageSize))
		if continuation != "" {
			req.Header.Set("x-ms-continuation", continuation)
		}
	}
	if e := op.check(); e != nil {
		return out, e
	}
	resp, e := c.http.Do(req)
	if e != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		if cancelled := op.check(); cancelled != nil {
			return out, cancelled
		}
		return out, ErrTransport
	}
	if resp.Body == nil {
		return out, ErrResponse
	}
	var once sync.Once
	closeBody := func() { once.Do(func() { _ = resp.Body.Close() }) }
	stop := context.AfterFunc(op.ctx, closeBody)
	defer stop()
	defer closeBody()
	if e := op.check(); e != nil {
		return out, e
	}
	out.status = resp.StatusCode
	out.meta, e = c.metadata(resp.Header, partition)
	if e != nil {
		return out, e
	}
	if resp.StatusCode == http.StatusTooManyRequests {
		retry, e := oneHeader(resp.Header, "x-ms-retry-after-ms", 12)
		if e != nil {
			return out, e
		}
		var delay time.Duration
		if retry != "" {
			n, e := strconv.ParseUint(retry, 10, 32)
			if e != nil || n > 86400000 {
				return out, ErrResponse
			}
			delay = time.Duration(n) * time.Millisecond
		}
		return out, &RateLimitError{HTTPError: HTTPError{Status: 429, Metadata: out.meta}, RetryAfter: delay}
	}
	if resp.StatusCode == 404 && method == http.MethodGet {
		return out, nil
	}
	if resp.StatusCode != 200 {
		return out, &HTTPError{Status: resp.StatusCode, Metadata: out.meta}
	}
	media, _, e := mime.ParseMediaType(resp.Header.Get("Content-Type"))
	if e != nil || media != "application/json" || strings.TrimSpace(resp.Header.Get("Content-Encoding")) != "" {
		return out, ErrResponse
	}
	op.budget.mu.Lock()
	remaining := c.opts.Limits.MaxResponseBytes - op.budget.bytes
	op.budget.mu.Unlock()
	raw, e := io.ReadAll(io.LimitReader(resp.Body, int64(remaining)+1))
	if cancelled := op.check(); cancelled != nil {
		return out, cancelled
	}
	if e != nil {
		return out, ErrTransport
	}
	if e := op.responseBytes(len(raw)); e != nil {
		return out, e
	}
	out.body, e = decodeJSON(raw)
	if e != nil {
		return out, e
	}
	if e := op.check(); e != nil {
		return out, e
	}
	out.continuation, e = oneHeader(resp.Header, "x-ms-continuation", 16384)
	if e != nil {
		return out, e
	}
	if method == http.MethodPost {
		count, e := oneHeader(resp.Header, "x-ms-item-count", 10)
		if e != nil {
			return out, e
		}
		if count != "" {
			n, e := strconv.ParseUint(count, 10, 32)
			if e != nil || n > uint64(c.opts.Limits.MaxRows) {
				return out, ErrResponse
			}
			v := int(n)
			out.itemCount = &v
		}
	}
	return out, nil
}
func (c *Client) document(op *operation, value any, key NativeKey, meta Metadata) (*Document, error) {
	data, ok := value.(map[string]any)
	if !ok {
		return nil, ErrResponse
	}
	id, ok := data["id"].(string)
	if !ok || !validID(id) || (key.ID != "" && id != key.ID) {
		return nil, ErrResponse
	}
	partition, ok := data[c.opts.PartitionField].(string)
	if !ok || partition != key.Partition {
		return nil, ErrResponse
	}
	if etag, present := data["_etag"]; present {
		v, ok := etag.(string)
		if !ok || !validHeader(v, 1024) || (meta.ETag != "" && v != meta.ETag) {
			return nil, ErrResponse
		}
		if meta.ETag == "" {
			meta.ETag = v
		}
	}
	if e := op.row(); e != nil {
		return nil, e
	}
	return &Document{Key: NativeKey{id, key.Partition}, Data: data, Metadata: meta}, nil
}
