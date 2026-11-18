package dalgo2cosmosdb

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"

	"github.com/dal-go/record"
)

type callbackDecode struct{ callback func() }

func (v *callbackDecode) UnmarshalJSON([]byte) error { v.callback(); return nil }
func TestEveryRecordErrorReclassificationChecksRealCallerCancellation(t *testing.T) {
	for _, mode := range []string{"initial-success", "final-success", "transport-error", "missing", "data", "codec"} {
		t.Run(mode, func(t *testing.T) {
			s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
				if mode == "transport-error" {
					w.WriteHeader(403)
					return
				}
				if mode == "missing" {
					w.WriteHeader(404)
					return
				}
				fixtureJSON(w, 200, nativeDocument("a", "one"))
			})
			c := fixtureClient(t, s, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			var target map[string]any
			var dataCalls, notifications, nilNotifications int
			var destination any = &target
			if mode == "codec" {
				destination = &callbackDecode{callback: func() { _ = c.Close() }}
			}
			r := observedRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "a"), destination)}
			r.onData = func() {
				dataCalls++
				if mode == "data" {
					_ = c.Close()
				}
			}
			r.onError = func(e error) {
				notifications++
				if e == nil {
					nilNotifications++
					if mode == "initial-success" || (mode == "final-success" && nilNotifications == 2) {
						_ = c.Close()
					}
				}
				var status *HTTPError
				if mode == "transport-error" && errors.As(e, &status) {
					_ = c.Close()
				}
				if mode == "missing" && record.IsNotFound(e) {
					_ = c.Close()
				}
				if errors.Is(e, ErrClosed) {
					cancel()
				} // actual second callback event, not Err counting
			}
			e := c.DB().Get(ctx, r)
			if !errors.Is(e, context.Canceled) || !errors.Is(r.Record.Error(), context.Canceled) {
				t.Fatal("SetError reclassification returned stale Close/record state", e)
			}
			if notifications > 4 {
				t.Fatal("record classification failed to converge")
			}
			if mode != "data" && mode != "codec" && mode != "final-success" && dataCalls != 0 {
				t.Fatal("error notification invoked later caller Data")
			}
		})
	}
}
func TestReaderFactoryStagingCannotAcknowledgeFailedDelivery(t *testing.T) {
	var calls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 65537))
		if r.Header.Get("x-ms-continuation") == "" {
			w.Header().Set("x-ms-continuation", "owned-next")
			fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
		} else {
			fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("b", "one")}, "_count": 1})
		}
	})
	c := fixtureClient(t, s, nil)
	q := Query{SQL: "SELECT * FROM c", Partition: "one", Limit: 1}
	first, e := c.Query(context.Background(), q)
	if e != nil || first.Next == nil {
		t.Fatal(e)
	}
	q.Continuation = first.Next
	// This is the same internal caller-side factory boundary used by QueryRecords,
	// after actual transport/validation, before cursor ACK. It triggers a real
	// cancellation event instead of a fabricated context.Err invocation count.
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	staged := false
	page, e := c.query(ctx, q, func(page Page) error {
		staged = true
		if len(page.Documents) != 1 {
			t.Error("factory did not receive staged native page")
		}
		cancel()
		return nil
	})
	if !staged || !errors.Is(e, context.Canceled) || page.Next != nil || len(page.Documents) != 0 {
		t.Fatal("failed reader factory delivered/acknowledged page", e)
	}
	c.mu.Lock()
	_, retained := c.cursors[first.Next]
	count := len(c.cursors)
	c.mu.Unlock()
	if !retained || count != 1 {
		t.Fatal("reader factory lost prior handle or leaked successor")
	}
	retry, e := c.Query(context.Background(), q)
	if e != nil || len(retry.Documents) != 1 || retry.Documents[0].Key.ID != "b" {
		t.Fatal("failed factory consumed original cursor", e)
	}
	if calls.Load() != 3 {
		t.Fatal("factory performed hidden query retry")
	}
	// A real Close at the same factory boundary cannot register an undisclosed
	// successor. Close intentionally invalidates the entire client/registry.
	closed := fixtureClient(t, s, nil)
	page, e = closed.query(context.Background(), Query{SQL: "SELECT * FROM c", Partition: "one", Limit: 1}, func(Page) error { _ = closed.Close(); return nil })
	if !errors.Is(e, ErrClosed) || page.Next != nil || len(page.Documents) != 0 {
		t.Fatal("closed factory returned an acknowledged result", e)
	}
	closed.mu.Lock()
	count = len(closed.cursors)
	closed.mu.Unlock()
	if count != 0 {
		t.Fatal("Close factory leaked cursor")
	}
}
func TestQuerySessionRequiresExplicitOptInAcrossPagesAndResumes(t *testing.T) {
	for _, explicit := range []bool{false, true} {
		for _, separate := range []bool{false, true} {
			name := map[bool]string{false: "default", true: "explicit"}[explicit] + map[bool]string{false: "-same-call", true: "-resume"}[separate]
			t.Run(name, func(t *testing.T) {
				var queryCalls atomic.Int64
				s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
					if r.Method == "GET" {
						w.Header().Set("x-ms-session-token", "0:-1#owned-v0")
						fixtureJSON(w, 200, nativeDocument("seed", "one"))
						return
					}
					_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 65537))
					n := queryCalls.Add(1)
					if !explicit {
						if r.Header.Get("x-ms-session-token") != "" || r.Header.Get("x-ms-consistency-level") != "" {
							t.Error("observed response token silently upgraded consistency")
						}
					} else {
						want := "0:-1#owned-v0"
						if n == 2 {
							want = "0:-1#owned-v1"
						}
						if r.Header.Get("x-ms-session-token") != want || r.Header.Get("x-ms-consistency-level") != "Session" {
							t.Error("explicit latest session token was not echoed")
						}
					}
					if n == 1 {
						w.Header().Set("x-ms-session-token", "0:-1#owned-v1")
						w.Header().Set("x-ms-continuation", "owned-next")
						fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
					} else {
						w.Header().Set("x-ms-session-token", "0:-1#owned-v2")
						fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("b", "one")}, "_count": 1})
					}
				})
				c := fixtureClient(t, s, nil)
				seed, e := c.Read(context.Background(), NativeKey{"seed", "one"})
				if e != nil || seed.Metadata.Session == nil {
					t.Fatal(e)
				}
				limit := 2
				if separate {
					limit = 1
				}
				q := Query{SQL: "SELECT * FROM c", Partition: "one", Limit: limit}
				if explicit {
					q.Session = seed.Metadata.Session
				}
				page, e := c.Query(context.Background(), q)
				if e != nil {
					t.Fatal(e)
				}
				if page.Metadata[0].Session == nil {
					t.Fatal("observed session metadata was discarded")
				}
				if separate {
					if page.Next == nil {
						t.Fatal("expected native continuation")
					}
					q.Continuation = page.Next
					page, e = c.Query(context.Background(), q)
					if e != nil {
						t.Fatal(e)
					}
				}
				if queryCalls.Load() != 2 || page.Metadata[len(page.Metadata)-1].Session == nil {
					t.Fatal("native session traversal did not complete")
				}
			})
		}
	}
}
