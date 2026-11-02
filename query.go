package dalgo2cosmosdb

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"encoding/json"
	"hash"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

type cursorState struct {
	binding                      [sha256.Size]byte
	token, session               string
	deadline                     time.Time
	budget                       *budget
	tokens, documents            map[string]struct{}
	busy                         bool
	retainedBytes, retainedNodes int
}
type queryPlan struct {
	body              []byte
	binding           [sha256.Size]byte
	session           string
	useSession        bool
	incoming          *cursorState
	tokens, documents map[string]struct{}
}

func copySet(source map[string]struct{}) map[string]struct{} {
	out := make(map[string]struct{}, len(source))
	for k := range source {
		out[k] = struct{}{}
	}
	return out
}

// queryScope hashes every prior equality component with a domain and explicit
// lengths. The admitted body is written directly, never converted to a JSON
// string and re-escaped or retained in each cursor. String chunks stay bounded.
func scopeBytes(h hash.Hash, b []byte) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(b)))
	_, _ = h.Write(size[:])
	_, _ = h.Write(b)
}
func scopeString(h hash.Hash, s string) {
	var size [8]byte
	binary.BigEndian.PutUint64(size[:], uint64(len(s)))
	_, _ = h.Write(size[:])
	var chunk [1024]byte
	for len(s) > 0 {
		n := copy(chunk[:], s)
		_, _ = h.Write(chunk[:n])
		s = s[n:]
	}
}
func queryScope(sql string, body []byte, partition string, limit, pageSize int, session, authScope string) [sha256.Size]byte {
	h := sha256.New()
	scopeString(h, "dalgo2cosmosdb/native-query-scope/v1")
	scopeString(h, sql)
	scopeBytes(h, body)
	scopeString(h, partition)
	scopeString(h, strconv.Itoa(limit))
	scopeString(h, strconv.Itoa(pageSize))
	scopeString(h, session)
	scopeString(h, authScope)
	var result [sha256.Size]byte
	copy(result[:], h.Sum(nil))
	return result
}

// Retention accounting is deliberately logical/conservative, not an exact Go
// heap estimate. It bounds all live registry strings/entries together, including
// repeated token occurrences and fixed per-map/handle/state charges.
func (c *Client) retention(state *cursorState) (int, int, error) {
	bytes, nodes := 256+sha256.Size+len(state.token)+len(state.session), 5
	for value := range state.tokens {
		bytes += 64 + len(value)
		nodes++
		if bytes > c.opts.Limits.MaxCursorBytes || nodes > c.opts.Limits.MaxCursorNodes {
			return 0, 0, ErrLimit
		}
	}
	for value := range state.documents {
		bytes += 64 + len(value)
		nodes++
		if bytes > c.opts.Limits.MaxCursorBytes || nodes > c.opts.Limits.MaxCursorNodes {
			return 0, 0, ErrLimit
		}
	}
	if bytes > c.opts.Limits.MaxCursorBytes || nodes > c.opts.Limits.MaxCursorNodes {
		return 0, 0, ErrLimit
	}
	return bytes, nodes, nil
}

// c.mu must be held. Checking membership makes expiry, Close and late cleanup
// idempotent; no operation can refund the same retained state twice.
func (c *Client) retireCursor(handle *Continuation) {
	if state, ok := c.cursors[handle]; ok {
		c.cursorBytes -= state.retainedBytes
		c.cursorNodes -= state.retainedNodes
		delete(c.cursors, handle)
	}
}
func (c *Client) pruneCursors() {
	now := time.Now()
	for handle, state := range c.cursors {
		if !state.busy && !now.Before(state.deadline) {
			c.retireCursor(handle)
		}
	}
}
func parameterName(s string) bool { return len(s) > 1 && s[0] == '@' && validName(s[1:]) }
func (c *Client) prepareQuery(op *operation, q Query) (queryPlan, error) {
	var p queryPlan
	if !validText(q.Partition, 2048) || len(q.SQL) > 32*1024 || !utf8.ValidString(q.SQL) || strings.ContainsRune(q.SQL, 0) || q.Limit < 1 || q.Limit > c.opts.Limits.MaxRows || len(q.Parameters) > 100 {
		return p, ErrRequest
	}
	fields := strings.Fields(q.SQL)
	if len(fields) < 2 || !strings.EqualFold(fields[0], "SELECT") {
		return p, ErrRequest
	}
	parameters := make([]Parameter, len(q.Parameters))
	seen := map[string]bool{}
	admission := &jsonAdmission{op: op, remaining: c.opts.Limits.MaxRequestBytes}
	if e := admission.charge(len(`{"query":`) + len(`,"parameters":[`) + len(`]}`)); e != nil {
		return p, e
	}
	if e := admission.string(q.SQL); e != nil {
		return p, e
	}
	for i, param := range q.Parameters {
		if !parameterName(param.Name) || seen[param.Name] {
			return p, ErrRequest
		}
		seen[param.Name] = true
		if i > 0 {
			if e := admission.charge(1); e != nil {
				return p, e
			}
		}
		if e := admission.charge(len(`{"name":`) + len(`,"value":`) + 1); e != nil {
			return p, e
		}
		if e := admission.string(param.Name); e != nil {
			return p, e
		}
		value, e := admission.value(param.Value, 0)
		if e != nil {
			return p, e
		}
		parameters[i] = Parameter{param.Name, value}
	}
	// Wire names are lowercase per the native API. No custom user Marshaler is
	// reachable: values were copied into a closed JSON tree on the caller path.
	type wireParameter struct {
		Name  string `json:"name"`
		Value any    `json:"value"`
	}
	wire := make([]wireParameter, len(parameters))
	for i, v := range parameters {
		wire[i] = wireParameter{v.Name, v.Value}
	}
	var e error
	p.body, e = json.Marshal(struct {
		Query      string          `json:"query"`
		Parameters []wireParameter `json:"parameters"`
	}{q.SQL, wire})
	if e != nil {
		return p, ErrRequest
	}
	if len(p.body) > c.opts.Limits.MaxRequestBytes || len(p.body) != admission.used {
		return p, ErrLimit
	}
	p.session, e = c.session(q.Partition, q.Session)
	if e != nil {
		return p, e
	}
	p.useSession = q.Session != nil
	p.binding = queryScope(q.SQL, p.body, q.Partition, q.Limit, c.opts.Limits.PageSize, p.session, c.opts.AuthScopeID)
	if e := op.check(); e != nil {
		return p, e
	}
	p.tokens = map[string]struct{}{}
	p.documents = map[string]struct{}{}
	if q.Continuation != nil {
		c.mu.Lock()
		state, ok := c.cursors[q.Continuation]
		if !ok || state.binding != p.binding {
			c.mu.Unlock()
			return p, ErrRequest
		}
		if state.busy {
			c.mu.Unlock()
			return p, ErrBusy
		}
		state.busy = true
		p.incoming = state
		p.session = state.session
		p.tokens = copySet(state.tokens)
		p.documents = copySet(state.documents)
		op.budget = state.budget
		c.mu.Unlock()
		op.onDelivery(func(success bool) error {
			c.mu.Lock()
			defer c.mu.Unlock()
			state.busy = false
			if c.cursors[q.Continuation] == state && !time.Now().Before(state.deadline) {
				c.retireCursor(q.Continuation)
			}
			return nil
		})
	}
	return p, nil
}
func (op *operation) page() error {
	b := op.budget
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.failed || b.pages >= op.c.opts.Limits.MaxPages {
		b.failed = true
		return ErrLimit
	}
	b.pages++
	return nil
}

// Query executes read-only parameterized native SQL. Results must be complete
// native documents (id and configured string partition field), not scalar or
// aggregate rows. Rows, empty pages and resumed journeys all retain budgets.
func (c *Client) Query(ctx context.Context, q Query) (Page, error) { return c.query(ctx, q, nil) }

// query stages owned data through the caller-side factory before cursor ACK.
func (c *Client) query(ctx context.Context, q Query, beforeAck func(Page) error) (Page, error) {
	deadline := time.Now().Add(c.opts.Limits.Timeout)
	if q.Continuation != nil {
		c.mu.Lock()
		state := c.cursors[q.Continuation]
		if state != nil && state.deadline.Before(deadline) {
			deadline = state.deadline
		}
		c.mu.Unlock()
	}
	var plan queryPlan
	return runMapped(c, ctx, deadline, func(op *operation) error { var e error; plan, e = c.prepareQuery(op, q); return e }, func(op *operation) (Page, error) {
		page := Page{Documents: make([]*Document, 0), Metadata: make([]Metadata, 0)}
		token := ""
		if plan.incoming != nil {
			token = plan.incoming.token
		}
		session := plan.session
		for {
			if e := op.check(); e != nil {
				return Page{}, e
			}
			if e := op.page(); e != nil {
				return Page{}, e
			}
			size := min(c.opts.Limits.PageSize, q.Limit-len(page.Documents))
			response, e := c.request(op, "POST", "", q.Partition, plan.body, token, session, size)
			if e != nil {
				return Page{}, e
			}
			page.Pages++
			page.Metadata = append(page.Metadata, response.meta)
			object, ok := response.body.(map[string]any)
			if !ok {
				return Page{}, ErrResponse
			}
			rows, ok := object["Documents"].([]any)
			if !ok || len(rows) > size {
				return Page{}, ErrResponse
			}
			count, ok := object["_count"].(json.Number)
			if !ok {
				return Page{}, ErrResponse
			}
			n, e := strconv.ParseInt(string(count), 10, 32)
			if e != nil || int(n) != len(rows) || (response.itemCount != nil && *response.itemCount != len(rows)) {
				return Page{}, ErrResponse
			}
			itemMeta := response.meta
			itemMeta.ETag = "" // Query response etag is not every document etag.
			for _, raw := range rows {
				document, e := c.document(op, raw, NativeKey{Partition: q.Partition}, itemMeta)
				if e != nil {
					return Page{}, e
				}
				if _, duplicate := plan.documents[document.Key.ID]; duplicate {
					return Page{}, ErrResponse
				}
				plan.documents[document.Key.ID] = struct{}{}
				page.Documents = append(page.Documents, document)
			}
			if plan.useSession && response.meta.Session != nil {
				session = response.meta.Session.token
			}
			token = response.continuation
			if token != "" {
				if _, repeated := plan.tokens[token]; repeated {
					return Page{}, ErrResponse
				}
				plan.tokens[token] = struct{}{}
			}
			if token == "" || len(page.Documents) == q.Limit {
				next := (*Continuation)(nil)
				var nextState *cursorState
				if token != "" {
					next = &Continuation{marker: 1}
					expires, _ := op.ctx.Deadline()
					nextState = &cursorState{binding: plan.binding, token: token, session: session, deadline: expires, budget: op.budget, tokens: plan.tokens, documents: plan.documents}
					var e error
					nextState.retainedBytes, nextState.retainedNodes, e = c.retention(nextState)
					if e != nil {
						return Page{}, e
					}
					page.Next = next
				}
				op.onDelivery(func(success bool) error {
					c.mu.Lock()
					defer c.mu.Unlock()
					if plan.incoming != nil {
						plan.incoming.busy = false
					}
					if !success {
						c.pruneCursors()
						return nil
					}
					if e := op.check(); e != nil {
						return e
					}
					c.pruneCursors()
					// Consumption and successor admission are one atomic delivery operation.
					oldBytes, oldNodes := 0, 0
					if plan.incoming != nil {
						if c.cursors[q.Continuation] != plan.incoming {
							return ErrRequest
						}
						oldBytes = plan.incoming.retainedBytes
						oldNodes = plan.incoming.retainedNodes
					}
					if next != nil && (c.cursorBytes-oldBytes+nextState.retainedBytes > c.opts.Limits.MaxCursorBytes || c.cursorNodes-oldNodes+nextState.retainedNodes > c.opts.Limits.MaxCursorNodes || (len(c.cursors) >= 1000 && plan.incoming == nil)) {
						return ErrLimit
					}
					if e := op.check(); e != nil {
						return e
					}
					if q.Continuation != nil {
						if c.cursors[q.Continuation] != plan.incoming {
							return ErrRequest
						}
						c.retireCursor(q.Continuation)
					}
					if next != nil {
						c.cursors[next] = nextState
						c.cursorBytes += nextState.retainedBytes
						c.cursorNodes += nextState.retainedNodes
					}
					return nil
				})
				return page, nil
			}
		}
	}, beforeAck)
}
