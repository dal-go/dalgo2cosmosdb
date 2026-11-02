package dalgo2cosmosdb

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
)

func TestEscapedRequestBytesAtExactWireBoundary(t *testing.T) {
	cases := []struct {
		name, sql string
		value     any
	}{
		{"html-controls-unicode", "SELECT * FROM c WHERE c.x = @x /* <&>\n\u2028 */", "\"\\\b\f\n\r\t\x00<&>\u2028\u2029é🙂"},
		{"nested-keys-and-numbers", "SELECT * FROM c WHERE c.x = @x", map[string]any{"\"\\\n<&>\u2028é": []any{nil, true, false, json.Number("-123.4500e+2"), int64(-5), uint64(9007199254740993), float64(0.25), map[string]any{"empty": ""}}}},
		{"empty-containers", "SELECT * FROM c", map[string]any{"array": []any{}, "object": map[string]any{}}},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			type parameter struct {
				Name  string `json:"name"`
				Value any    `json:"value"`
			}
			wire, e := json.Marshal(struct {
				Query      string      `json:"query"`
				Parameters []parameter `json:"parameters"`
			}{test.sql, []parameter{{"@x", test.value}}})
			if e != nil {
				t.Fatal(e)
			}
			var calls, auth atomic.Int64
			s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				raw, e := io.ReadAll(io.LimitReader(r.Body, int64(len(wire))+1))
				if e != nil || !bytes.Equal(raw, wire) {
					t.Error("native escaped JSON wire bytes changed")
				}
				fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
			})
			edit := func(limit int) func(*Options) {
				return func(o *Options) {
					o.Limits.MaxRequestBytes = limit
					previous := o.Authorization
					o.Authorization = func(ctx context.Context, r AuthRequest) (Credential, error) { auth.Add(1); return previous(ctx, r) }
				}
			}
			query := Query{SQL: test.sql, Parameters: []Parameter{{"@x", test.value}}, Partition: "one", Limit: 1}
			under := fixtureClient(t, s, edit(len(wire)))
			if page, e := under.Query(context.Background(), query); e != nil || len(page.Documents) != 1 {
				t.Fatal("exact encoded request boundary refused", e)
			}
			beforeAuth, beforeCalls := auth.Load(), calls.Load()
			over := fixtureClient(t, s, edit(len(wire)-1))
			if page, e := over.Query(context.Background(), query); !errors.Is(e, ErrLimit) || len(page.Documents) != 0 {
				t.Fatal("over-bound encoded request accepted", e)
			}
			if auth.Load() != beforeAuth || calls.Load() != beforeCalls {
				t.Fatal("encoded request refused after authentication or I/O")
			}
		})
	}
}
func TestSharedParameterReferencesRefuseBeforeLargeMarshalAllocation(t *testing.T) {
	// Only24KiB of string input is allocated. Its repeated wire occurrences would
	// form24MiB before the old post-marshal check; no64GiB test allocation is used.
	leaf := strings.Repeat("r", 24*1024)
	shared := make([]any, 1024)
	for i := range shared {
		shared[i] = leaf
	}
	var calls, auth atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fixtureJSON(w, 200, map[string]any{"Documents": []any{}, "_count": 0})
	})
	c := fixtureClient(t, s, func(o *Options) {
		previous := o.Authorization
		o.Authorization = func(ctx context.Context, r AuthRequest) (Credential, error) { auth.Add(1); return previous(ctx, r) }
	})
	query := Query{SQL: "SELECT * FROM c WHERE c.x = @x", Parameters: []Parameter{{"@x", shared}}, Partition: "one", Limit: 1}
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	page, e := c.Query(context.Background(), query)
	runtime.ReadMemStats(&after)
	if !errors.Is(e, ErrLimit) || len(page.Documents) != 0 || auth.Load() != 0 || calls.Load() != 0 {
		t.Fatal("repeated parameter wire budget did not refuse before auth/I/O", e)
	}
	// A generous working-overhead allowance distinguishes admitted bounded copy
	// from materializing the24MiB wire payload, without asserting object counts or
	// claiming a production-process heap cap. Other tests in this package are serial.
	if after.TotalAlloc-before.TotalAlloc > 4*1024*1024 {
		t.Fatal("over-budget repeated strings were fully materialized before refusal")
	}
	caller, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e = c.Query(caller, query); !errors.Is(e, context.Canceled) || auth.Load() != 0 || calls.Load() != 0 {
		t.Fatal("preaborted large input crossed preparation boundary", e)
	}
}
func TestEscapedControlExpansionRefusesBeforeAuthentication(t *testing.T) {
	var auth atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		t.Error("over-budget control input dispatched HTTP")
		w.WriteHeader(400)
	})
	c := fixtureClient(t, s, func(o *Options) {
		previous := o.Authorization
		o.Authorization = func(ctx context.Context, r AuthRequest) (Credential, error) { auth.Add(1); return previous(ctx, r) }
	})
	if _, e := c.Query(context.Background(), Query{SQL: "SELECT * FROM c WHERE c.x = @x", Parameters: []Parameter{{"@x", strings.Repeat("\x00", 12000)}}, Partition: "one", Limit: 1}); !errors.Is(e, ErrLimit) || auth.Load() != 0 {
		t.Fatal("six-byte JSON escape expansion was not admitted", e)
	}
}
func TestCompleteScopeFingerprintAndBoundedRetainedBinding(t *testing.T) {
	base := queryScope("sql", []byte("body"), "partition", 1, 100, "session", "auth")
	variants := [][sha256.Size]byte{
		queryScope("other", []byte("body"), "partition", 1, 100, "session", "auth"), queryScope("sql", []byte("other"), "partition", 1, 100, "session", "auth"), queryScope("sql", []byte("body"), "other", 1, 100, "session", "auth"), queryScope("sql", []byte("body"), "partition", 2, 100, "session", "auth"), queryScope("sql", []byte("body"), "partition", 1, 101, "session", "auth"), queryScope("sql", []byte("body"), "partition", 1, 100, "other", "auth"), queryScope("sql", []byte("body"), "partition", 1, 100, "session", "other"),
	}
	for _, different := range variants {
		if different == base {
			t.Fatal("scope component was omitted from cursor binding")
		}
	}
	if queryScope("ab", []byte("c"), "partition", 1, 100, "session", "auth") == queryScope("a", []byte("bc"), "partition", 1, 100, "session", "auth") {
		t.Fatal("scope field delimiters are ambiguous")
	}
	var calls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		// Receive the complete owned POST before replying/closing: an early TLS
		// response can otherwise race the client's still-in-progress body write.
		raw, err := io.ReadAll(io.LimitReader(r.Body, 64*1024+1))
		if err != nil || len(raw) > 64*1024 {
			t.Error("owned large query body exceeded its bound or failed")
			http.Error(w, "owned request refused", 400)
			return
		}
		var request struct {
			Query      string `json:"query"`
			Parameters []struct {
				Name  string `json:"name"`
				Value string `json:"value"`
			} `json:"parameters"`
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		var extra any
		if r.Method != http.MethodPost || r.URL.Path != "/dbs/existing/colls/native/docs" || r.Header.Get("Content-Type") != "application/query+json" || r.Header.Get("x-ms-documentdb-isquery") != "true" || r.Header.Get("x-ms-max-item-count") != "1" || decoder.Decode(&request) != nil || decoder.Decode(&extra) != io.EOF || request.Query != "SELECT * FROM c WHERE c.x = @x" || len(request.Parameters) != 1 || request.Parameters[0].Name != "@x" || request.Parameters[0].Value != strings.Repeat("<", 8192) {
			t.Error("owned large native query request contract changed")
			http.Error(w, "owned request refused", 400)
			return
		}
		w.Header().Set("x-ms-continuation", "owned-next")
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
	})
	c := fixtureClient(t, s, nil)
	query := Query{SQL: "SELECT * FROM c WHERE c.x = @x", Parameters: []Parameter{{"@x", strings.Repeat("<", 8192)}}, Partition: "one", Limit: 1}
	page, e := c.Query(context.Background(), query)
	if e != nil || page.Next == nil {
		t.Fatal("large admitted query failed", e)
	}
	c.mu.Lock()
	state := c.cursors[page.Next]
	size := len(state.binding)
	c.mu.Unlock()
	if size > 64 {
		t.Fatal("cursor retained re-escaped raw query instead of fixed binding")
	}
	query.Continuation = page.Next
	query.Parameters = []Parameter{{"@x", strings.Repeat(">", 8192)}}
	before := calls.Load()
	if _, e = c.Query(context.Background(), query); !errors.Is(e, ErrRequest) || calls.Load() != before {
		t.Fatal("same-size changed parameter crossed cursor scope", e)
	}
}
