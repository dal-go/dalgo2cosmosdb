package dalgo2cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func nativeDocument(id, partition string) map[string]any {
	return map[string]any{"id": id, "tenant": partition, "exact": json.Number("9007199254740993"), "fraction": json.Number("12345678901234567890.0001"), "flag": false, "empty": nil, "nested": map[string]any{"array": []any{json.Number("-2"), "native"}}, "_etag": "\"owned-etag\"", "_rid": "owned-rid", "_ts": json.Number("123")}
}
func fixtureJSON(w http.ResponseWriter, status int, value any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(value)
}
func fixtureServer(t *testing.T, handler http.HandlerFunc) *httptest.Server {
	t.Helper()
	s := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("Authorization") == "" || r.Header.Get("x-ms-date") == "" || r.Header.Get("x-ms-version") != apiVersion || r.Header.Get("Accept") != "application/json" || r.Header.Get("Accept-Encoding") != "identity" || r.Header.Get("Cookie") != "" || r.Header.Get("x-ms-documentdb-query-enablecrosspartition") != "" || r.Method != "GET" && r.Method != "POST" {
			t.Error("owned native request contract failed")
			http.Error(w, "owned refusal", 400)
			return
		}
		handler(w, r)
	}))
	s.Config.ErrorLog = log.New(io.Discard, "", 0)
	s.StartTLS()
	t.Cleanup(s.Close)
	return s
}
func fixtureClient(t *testing.T, s *httptest.Server, edit func(*Options)) *Client {
	t.Helper()
	credential, e := ResourceToken("type=resource&ver=1.0&sig=owned+/=")
	if e != nil {
		t.Fatal(e)
	}
	opts := Options{Database: "existing", Container: "native", PartitionField: "tenant", AuthScopeID: "synthetic-reader", Authorization: func(context.Context, AuthRequest) (Credential, error) { return credential, nil }, Transport: s.Client().Transport.(*http.Transport), Limits: Limits{Timeout: 2 * time.Second}, Collections: map[string]Collection{"items": {Partition: "one"}, "other-partition": {Partition: "two"}}}
	if edit != nil {
		edit(&opts)
	}
	c, e := New(s.URL, opts)
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { _ = c.Close() })
	return c
}
func wait(t *testing.T, ch <-chan struct{}) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(3 * time.Second):
		t.Fatal("owned fixture acknowledgment missing")
	}
}
func TestCredentialEncodingAndClosedModes(t *testing.T) {
	token, e := ResourceToken("type=resource&ver=1.0&sig=owned+/=")
	if e != nil || token.header != "type%3Dresource%26ver%3D1.0%26sig%3Downed%2B%2F%3D" {
		t.Fatal("resource token percent encoding contract")
	}
	if strings.Contains(fmt.Sprintf("%v %#v", token, token), "owned") {
		t.Fatal("credential formatter exposed secret")
	}
	for _, raw := range []string{"", "type=master&ver=1.0&sig=owned", "type%3Dresource%26ver%3D1.0%26sig%3Downed", "type=resource&ver=1.0&sig=owned&extra=1", "type=resource&ver=1.0&sig=owned%2B", "type=resource&ver=1.0&sig=owned\n"} {
		if _, e := ResourceToken(raw); !errors.Is(e, ErrConfiguration) {
			t.Fatal("invalid resource token accepted")
		}
	}
	aad, e := AADToken("owned.jwt.signature")
	if e != nil || aad.header != "type=aad&ver=1.0&sig=owned.jwt.signature" {
		t.Fatal("official raw AAD format changed")
	}
	for _, raw := range []string{"", "Bearer owned", "type=aad&ver=1.0&sig=owned", "owned\r\nheader"} {
		if _, e := AADToken(raw); !errors.Is(e, ErrConfiguration) {
			t.Fatal("invalid AAD token accepted")
		}
	}
}
func TestNativePointIdentityValuesSessionsMissingAndTLS(t *testing.T) {
	var requests atomic.Int64
	var wantSession atomic.Bool
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.URL.Path != "/dbs/existing/colls/native/docs/a" && r.URL.Path != "/dbs/existing/colls/native/docs/missing" {
			t.Error("unexpected native path")
		}
		var partition []string
		if json.Unmarshal([]byte(r.Header.Get("x-ms-documentdb-partitionkey")), &partition) != nil || len(partition) != 1 {
			t.Error("explicit partition header missing")
			return
		}
		if wantSession.Load() {
			if r.Header.Get("x-ms-session-token") != "0:-1#owned-session" || r.Header.Get("x-ms-consistency-level") != "Session" {
				t.Error("scoped session echo missing")
			}
		} else if r.Header.Get("x-ms-session-token") != "" {
			t.Error("implicit global session cache")
		}
		w.Header().Set("x-ms-request-charge", "0.70")
		w.Header().Set("x-ms-session-token", "0:-1#owned-session")
		w.Header().Set("etag", "\"owned-etag\"")
		if strings.HasSuffix(r.URL.Path, "/missing") {
			fixtureJSON(w, 404, map[string]any{"private": "synthetic-secret"})
			return
		}
		fixtureJSON(w, 200, nativeDocument("a", partition[0]))
	})
	c := fixtureClient(t, s, nil)
	key := NativeKey{"a", "one"}
	result, e := c.Read(context.Background(), key)
	if e != nil {
		t.Fatal(e)
	}
	d := result.Document
	if d == nil || d.Key != key || d.Data["exact"] != json.Number("9007199254740993") || d.Data["fraction"] != json.Number("12345678901234567890.0001") || d.Data["flag"] != false || d.Data["empty"] != nil || d.Data["_rid"] != "owned-rid" || d.Metadata.ETag != "\"owned-etag\"" {
		t.Fatal("native document fidelity changed")
	}
	if !result.Metadata.RequestChargePresent || result.Metadata.RequestCharge != "0.70" {
		t.Fatal("RU wire text changed")
	}
	wantSession.Store(true)
	if _, e = c.Get(context.Background(), key, ReadOptions{Session: result.Metadata.Session}); e != nil {
		t.Fatal(e)
	}
	wantSession.Store(false)
	before := requests.Load()
	if _, e = c.Get(context.Background(), NativeKey{"a", "two"}, ReadOptions{Session: result.Metadata.Session}); !errors.Is(e, ErrRequest) || requests.Load() != before {
		t.Fatal("cross-partition session accepted")
	}
	foreign := fixtureClient(t, s, nil)
	if _, e = foreign.Get(context.Background(), key, ReadOptions{Session: result.Metadata.Session}); !errors.Is(e, ErrRequest) {
		t.Fatal("foreign session accepted")
	}
	other, e := c.Get(context.Background(), NativeKey{"a", "two"})
	if e != nil || other.Key.Partition != "two" {
		t.Fatal("same native id lost partition identity")
	}
	missing, e := c.Read(context.Background(), NativeKey{"missing", "one"})
	if e != nil || missing.Document != nil || !missing.Metadata.RequestChargePresent {
		t.Fatal("missing metadata was lost")
	}
	exists, e := c.Exists(context.Background(), NativeKey{"missing", "one"})
	if e != nil || exists {
		t.Fatal("point404 is not missing")
	}
	// The fixture CA is trusted only by its owned clone; no insecure TLS escape.
	untrusted, e := New(s.URL, Options{Database: "existing", Container: "native", PartitionField: "tenant", AuthScopeID: "owned", Authorization: c.opts.Authorization})
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = untrusted.Close() }()
	before = requests.Load()
	if _, e = untrusted.Get(context.Background(), key); !errors.Is(e, ErrTransport) || requests.Load() != before {
		t.Fatal("untrusted TLS accepted")
	}
	bad := c.opts
	bad.Transport = c.transport.Clone()
	bad.Transport.TLSClientConfig = bad.Transport.TLSClientConfig.Clone()
	bad.Transport.TLSClientConfig.InsecureSkipVerify = true
	if _, e = New(s.URL, bad); !errors.Is(e, ErrConfiguration) {
		t.Fatal("insecure TLS override accepted")
	}
	if _, e = New(strings.Replace(s.URL, "https:", "http:", 1), c.opts); !errors.Is(e, ErrConfiguration) {
		t.Fatal("HTTP downgrade accepted")
	}
}
func TestReadOnlyRouteRedirectAndRateLimitMetadata(t *testing.T) {
	var destinationCalls atomic.Int64
	destination := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		destinationCalls.Add(1)
		fixtureJSON(w, 200, nativeDocument("a", "one"))
	})
	var requests atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch {
		case strings.HasSuffix(r.URL.Path, "/redirect"):
			w.Header().Set("Location", destination.URL+"/dbs/existing/colls/native/docs/a")
			w.WriteHeader(302)
		case strings.HasSuffix(r.URL.Path, "/limited"):
			w.Header().Set("x-ms-request-charge", "1.250")
			w.Header().Set("x-ms-retry-after-ms", "17")
			w.WriteHeader(429)
		default:
			fixtureJSON(w, 403, map[string]any{"message": "synthetic-secret-auth-body"})
		}
	})
	c := fixtureClient(t, s, nil)
	_, e := c.Get(context.Background(), NativeKey{"redirect", "one"})
	var httpError *HTTPError
	if !errors.As(e, &httpError) || httpError.Status != 302 || destinationCalls.Load() != 0 {
		t.Fatal("redirect followed or authority forwarded")
	}
	before := requests.Load()
	_, e = c.Get(context.Background(), NativeKey{"limited", "one"})
	var limited *RateLimitError
	if !errors.As(e, &limited) || limited.RetryAfter != 17*time.Millisecond || limited.Metadata.RequestCharge != "1.250" || !limited.Metadata.RequestChargePresent || requests.Load() != before+1 {
		t.Fatal("429 retry or metadata contract changed")
	}
	_, e = c.Get(context.Background(), NativeKey{"denied", "one"})
	if !errors.As(e, &httpError) || httpError.Status != 403 || strings.Contains(e.Error(), "synthetic-secret") || errors.Unwrap(e) != nil {
		t.Fatal("unsafe HTTP error")
	}
}
func TestNativeQueryEmptyPagesScopedContinuationAndRecovery(t *testing.T) {
	var requests atomic.Int64
	var repeated atomic.Bool
	var seenMu sync.Mutex
	var observed []string
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		if r.Method != "POST" || r.URL.Path != "/dbs/existing/colls/native/docs" || r.Header.Get("Content-Type") != "application/query+json" || r.Header.Get("x-ms-documentdb-isquery") != "true" || r.Header.Get("x-ms-max-item-count") != "1" {
			t.Error("native query wire headers changed")
			return
		}
		raw, e := io.ReadAll(io.LimitReader(r.Body, 65537))
		if e != nil {
			t.Error("owned query body failed")
			return
		}
		var body struct {
			Query      string `json:"query"`
			Parameters []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"parameters"`
		}
		if json.Unmarshal(raw, &body) != nil || body.Query != "SELECT * FROM c WHERE c.kind = @kind" || len(body.Parameters) != 1 || body.Parameters[0].Name != "@kind" || body.Parameters[0].Value != "open' OR true" {
			t.Error("query parameter interpolation or shape changed")
			return
		}
		w.Header().Set("etag", "container-query-etag")
		token := r.Header.Get("x-ms-continuation")
		seenMu.Lock()
		observed = append(observed, token)
		seenMu.Unlock()
		switch token {
		case "":
			w.Header().Set("x-ms-continuation", "opaque-1")
			fixtureJSON(w, 200, map[string]any{"Documents": []any{}, "_count": 0})
		case "opaque-1":
			w.Header().Set("x-ms-continuation", "opaque-2")
			fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
		case "opaque-2":
			if repeated.Load() {
				w.Header().Set("x-ms-continuation", "opaque-2")
				fixtureJSON(w, 200, map[string]any{"Documents": []any{}, "_count": 0})
			} else {
				fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("b", "one")}, "_count": 1})
			}
		default:
			t.Error("unknown service continuation")
		}
	})
	c := fixtureClient(t, s, nil)
	q := Query{SQL: "SELECT * FROM c WHERE c.kind = @kind", Parameters: []Parameter{{"@kind", "open' OR true"}}, Partition: "one", Limit: 1}
	first, e := c.Query(context.Background(), q)
	if e != nil || len(first.Documents) != 1 || first.Pages != 2 || first.Next == nil {
		t.Fatal("empty page was treated as EOF", e)
	}
	if first.Documents[0].Metadata.ETag != "\"owned-etag\"" {
		t.Fatal("query response etag overwrote native document metadata")
	}
	before := requests.Load()
	wrong := q
	wrong.Partition = "two"
	wrong.Continuation = first.Next
	if _, e = c.Query(context.Background(), wrong); !errors.Is(e, ErrRequest) {
		t.Fatal("cross-partition cursor accepted")
	}
	foreign := fixtureClient(t, s, nil)
	resume := q
	resume.Continuation = first.Next
	if _, e = foreign.Query(context.Background(), resume); !errors.Is(e, ErrRequest) {
		t.Fatal("foreign cursor accepted")
	}
	if requests.Load() != before {
		t.Fatal("invalid cursor dispatched")
	}
	repeated.Store(true)
	if page, e := c.Query(context.Background(), resume); !errors.Is(e, ErrResponse) || len(page.Documents) != 0 || page.Next != nil {
		t.Fatal("nonprogressing continuation returned a prefix", e)
	}
	repeated.Store(false)
	next, e := c.Query(context.Background(), resume)
	if e != nil || len(next.Documents) != 1 || next.Documents[0].Key.ID != "b" || next.Next != nil {
		t.Fatal("failed resume consumed prior handle", e)
	}
	before = requests.Load()
	if _, e = c.Query(context.Background(), resume); !errors.Is(e, ErrRequest) || requests.Load() != before {
		t.Fatal("acknowledged handle replayed")
	}
	seenMu.Lock()
	defer seenMu.Unlock()
	if !reflect.DeepEqual(observed, []string{"", "opaque-1", "opaque-2", "opaque-2"}) {
		t.Fatal("unexpected cursor progression")
	}
}
func TestNativeWholeResultAndWorkLimits(t *testing.T) {
	for _, mode := range []string{"bytes", "count", "header-count", "partition", "duplicate", "missing-partition", "scalar", "json-duplicate", "surrogate", "pages"} {
		t.Run(mode, func(t *testing.T) {
			var calls atomic.Int64
			s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				n := calls.Add(1)
				switch mode {
				case "json-duplicate":
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"a","id":"b","tenant":"one"}`)
					return
				case "surrogate":
					w.Header().Set("Content-Type", "application/json")
					_, _ = io.WriteString(w, `{"id":"a","tenant":"one","bad":"\ud800"}`)
					return
				case "pages":
					w.Header().Set("x-ms-continuation", fmt.Sprintf("opaque-%d", n))
					fixtureJSON(w, 200, map[string]any{"Documents": []any{}, "_count": 0})
					return
				}
				if mode == "header-count" {
					w.Header().Set("x-ms-item-count", "2")
				}
				d := nativeDocument("a", "one")
				if mode == "partition" {
					d["tenant"] = "other"
				}
				if mode == "missing-partition" {
					delete(d, "tenant")
				}
				rows := []any{d}
				count := 1
				if mode == "duplicate" {
					rows = append(rows, d)
					count = 2
				}
				if mode == "count" {
					count = 2
				}
				if mode == "scalar" {
					rows = []any{"not-a-document"}
				}
				if r.Method == "GET" {
					fixtureJSON(w, 200, d)
				} else {
					fixtureJSON(w, 200, map[string]any{"Documents": rows, "_count": count})
				}
			})
			c := fixtureClient(t, s, func(o *Options) {
				if mode == "bytes" {
					o.Limits.MaxResponseBytes = 32
				}
				if mode == "pages" {
					o.Limits.MaxPages = 2
				}
			})
			if mode == "json-duplicate" || mode == "surrogate" {
				d, e := c.Get(context.Background(), NativeKey{"a", "one"})
				if !errors.Is(e, ErrResponse) || d != nil {
					t.Fatal("malformed native JSON delivered", e)
				}
				return
			}
			page, e := c.Query(context.Background(), Query{SQL: "SELECT * FROM c", Partition: "one", Limit: 2})
			want := ErrResponse
			if mode == "bytes" || mode == "pages" {
				want = ErrLimit
			}
			if !errors.Is(e, want) || len(page.Documents) != 0 || page.Next != nil {
				t.Fatal("whole-result refusal changed", e)
			}
			if mode == "pages" && calls.Load() != 2 {
				t.Fatal("empty page work was unbounded")
			}
		})
	}
}
func TestOrderedCopiedBatchAggregateBytesAndNoLaterDispatch(t *testing.T) {
	var mu sync.Mutex
	var ids []string
	var deny atomic.Bool
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/dbs/existing/colls/native/docs/")
		mu.Lock()
		ids = append(ids, id)
		mu.Unlock()
		if deny.Load() && id == "b" {
			w.WriteHeader(403)
			return
		}
		if id == "missing" {
			w.WriteHeader(404)
			return
		}
		fixtureJSON(w, 200, nativeDocument(id, "one"))
	})
	c := fixtureClient(t, s, nil)
	rows, e := c.GetMany(context.Background(), []NativeKey{{"b", "one"}, {"missing", "one"}, {"a", "one"}, {"a", "one"}})
	if e != nil || len(rows) != 4 || rows[0].Key.ID != "b" || rows[1] != nil || rows[2].Key.ID != "a" || rows[2] == rows[3] {
		t.Fatal("ordered native duplicates/misses changed", e)
	}
	raw, _ := json.Marshal(nativeDocument("a", "one"))
	limited := fixtureClient(t, s, func(o *Options) { o.Limits.MaxResponseBytes = 2*(len(raw)+1) - 1 })
	mu.Lock()
	ids = nil
	mu.Unlock()
	rows, e = limited.GetMany(context.Background(), []NativeKey{{"a", "one"}, {"a", "one"}, {"a", "one"}})
	if !errors.Is(e, ErrLimit) || rows != nil {
		t.Fatal("batch byte overflow returned prefix", e)
	}
	mu.Lock()
	if len(ids) != 2 {
		t.Fatal("later key dispatched after byte failure")
	}
	ids = nil
	mu.Unlock()
	deny.Store(true)
	rows, e = c.GetMany(context.Background(), []NativeKey{{"a", "one"}, {"b", "one"}, {"later", "one"}})
	var status *HTTPError
	if !errors.As(e, &status) || status.Status != 403 || rows != nil {
		t.Fatal("batch HTTP failure returned prefix", e)
	}
	mu.Lock()
	if !reflect.DeepEqual(ids, []string{"a", "b"}) {
		t.Fatal("later key dispatched after failure")
	}
	mu.Unlock()
	deny.Store(false)
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var authCalls atomic.Int64
	copied := fixtureClient(t, s, func(o *Options) {
		previous := o.Authorization
		o.Authorization = func(ctx context.Context, r AuthRequest) (Credential, error) {
			if authCalls.Add(1) == 1 {
				close(entered)
				<-release
			}
			return previous(ctx, r)
		}
	})
	keys := []NativeKey{{"a", "one"}, {"b", "one"}}
	done := make(chan error, 1)
	go func() { _, e := copied.GetMany(context.Background(), keys); done <- e }()
	wait(t, entered)
	keys[1].ID = "changed-after-admission"
	close(release)
	if e = <-done; e != nil {
		t.Fatal(e)
	}
	mu.Lock()
	defer mu.Unlock()
	if ids[len(ids)-1] != "b" {
		t.Fatal("borrowed input changed detached worker")
	}
}
func TestCallerDeadlineAfterDispatchDisconnectAndReopen(t *testing.T) {
	for _, deadline := range []bool{false, true} {
		t.Run(fmt.Sprint(deadline), func(t *testing.T) {
			started, disconnected := make(chan struct{}), make(chan struct{})
			escape := make(chan struct{})
			s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if strings.HasSuffix(r.URL.Path, "/held") {
					_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 65537))
					close(started)
					select {
					case <-r.Context().Done():
						close(disconnected)
					case <-escape:
					}
					return
				}
				fixtureJSON(w, 200, nativeDocument("a", "one"))
			})
			// Escape cleanup is registered after server cleanup, so a failed assertion
			// cannot make httptest.Close wait forever for the held synthetic request.
			t.Cleanup(func() {
				select {
				case <-escape:
				default:
					close(escape)
				}
			})
			c := fixtureClient(t, s, func(o *Options) { o.Limits.Timeout = time.Second })
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := c.Get(ctx, NativeKey{"held", "one"}); done <- e }()
			wait(t, started)
			if !deadline {
				cancel()
			}
			e := <-done
			want := context.Canceled
			if deadline {
				want = context.DeadlineExceeded
			}
			if !errors.Is(e, want) {
				t.Fatal("wrong scoped cancellation", e)
			}
			wait(t, disconnected)
			if _, e = c.Get(context.Background(), NativeKey{"a", "one"}); e != nil {
				t.Fatal("fresh read after cancellation failed", e)
			}
		})
	}
}
