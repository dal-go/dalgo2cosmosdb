package dalgo2cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type roundTripper func(*http.Request) (*http.Response, error)

func (f roundTripper) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

type closingBody struct {
	io.ReadCloser
	once sync.Once
	hook func()
}

func (b *closingBody) Close() error { e := b.ReadCloser.Close(); b.once.Do(b.hook); return e }
func TestCredentialWaitPreabortRedactionAndRetainedPhysicalAdmission(t *testing.T) {
	var httpCalls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		httpCalls.Add(1)
		fixtureJSON(w, 200, nativeDocument("a", "one"))
	})
	entered, release := make(chan struct{}), make(chan struct{})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	var callbacks atomic.Int64
	c := fixtureClient(t, s, func(o *Options) {
		o.Limits.MaxConcurrent = 1
		previous := o.Authorization
		o.Authorization = func(ctx context.Context, r AuthRequest) (Credential, error) {
			if callbacks.Add(1) == 1 {
				close(entered)
				<-release
				return Credential{}, errors.New("synthetic-secret-late-provider")
			}
			return previous(ctx, r)
		}
	})
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, e := c.Get(ctx, NativeKey{"a", "one"}); !errors.Is(e, context.Canceled) || callbacks.Load() != 0 || httpCalls.Load() != 0 {
		t.Fatal("preabort invoked credentials")
	}
	ctx, cancel = context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { _, e := c.Get(ctx, NativeKey{"a", "one"}); done <- e }()
	wait(t, entered)
	cancel()
	if e := <-done; !errors.Is(e, context.Canceled) {
		t.Fatal("credential wait did not cancel", e)
	}
	if _, e := c.Get(context.Background(), NativeKey{"a", "one"}); !errors.Is(e, ErrBusy) || callbacks.Load() != 1 || httpCalls.Load() != 0 {
		t.Fatal("abandoned provider freed physical admission", e)
	}
	close(release)
	// Poll the public admission/result behavior, not a private release counter.
	// Only ErrBusy is retried; those refusals cannot dispatch provider or HTTP work.
	deadline := time.NewTimer(3 * time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		d, e := c.Get(context.Background(), NativeKey{"a", "one"})
		if e == nil {
			if d == nil || httpCalls.Load() != 1 {
				t.Fatal("reopen result changed")
			}
			break
		}
		if !errors.Is(e, ErrBusy) {
			t.Fatal("late provider error escaped or prevented reopen", e)
		}
		select {
		case <-tick.C:
		case <-deadline.C:
			t.Fatal("physical lease was never released")
		}
	}
	failed := fixtureClient(t, s, func(o *Options) {
		o.Authorization = func(context.Context, AuthRequest) (Credential, error) {
			return Credential{}, errors.New("synthetic-secret-credential")
		}
	})
	if _, e := failed.Get(context.Background(), NativeKey{"a", "one"}); !errors.Is(e, ErrTransport) || strings.Contains(e.Error(), "synthetic-secret") || errors.Unwrap(e) != nil {
		t.Fatal("credential cause leaked")
	}
}
func TestOriginalBatchDeadlineAndBeforeNextDispatch(t *testing.T) {
	var calls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fixtureJSON(w, 200, nativeDocument("a", "one"))
	})
	var mu sync.Mutex
	var first time.Time
	var authCalls int
	c := fixtureClient(t, s, func(o *Options) {
		o.Limits.Timeout = time.Second
		previous := o.Authorization
		o.Authorization = func(ctx context.Context, r AuthRequest) (Credential, error) {
			deadline, ok := ctx.Deadline()
			if !ok {
				t.Error("credential scope has no deadline")
			}
			mu.Lock()
			authCalls++
			n := authCalls
			if n == 1 {
				first = deadline
			} else if !deadline.Equal(first) {
				t.Error("batch reset original deadline")
			}
			mu.Unlock()
			if n == 2 {
				<-ctx.Done()
			}
			return previous(ctx, r)
		}
	})
	rows, e := c.GetMany(context.Background(), []NativeKey{{"a", "one"}, {"a", "one"}})
	if !errors.Is(e, context.DeadlineExceeded) || rows != nil || calls.Load() != 1 {
		t.Fatal("batch deadline returned a prefix or dispatched next key", e)
	}
}
func TestBodyCloseCallerAbortAndClientCloseFreshDelivery(t *testing.T) {
	for _, closeClient := range []bool{false, true} {
		t.Run(map[bool]string{false: "caller", true: "client"}[closeClient], func(t *testing.T) {
			s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) { fixtureJSON(w, 200, nativeDocument("a", "one")) })
			c := fixtureClient(t, s, nil)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			native := c.http.Transport
			var closes atomic.Int64
			c.http.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
				response, e := native.RoundTrip(r)
				if e != nil {
					return response, e
				}
				response.Body = &closingBody{ReadCloser: response.Body, hook: func() {
					closes.Add(1)
					if closeClient {
						_ = c.Close()
					} else {
						cancel()
					}
				}}
				return response, nil
			})
			doc, e := c.Get(ctx, NativeKey{"a", "one"})
			want := context.Canceled
			if closeClient {
				want = ErrClosed
			}
			if !errors.Is(e, want) || doc != nil {
				t.Fatal("cleanup cancellation became late success", e)
			}
			if closes.Load() != 1 {
				t.Fatal("owned body cleanup did not occur exactly once")
			}
		})
	}
}
func TestClientCloseStopsOwnedWorkWithoutClosingCallerTransport(t *testing.T) {
	started, disconnected, escape := make(chan struct{}), make(chan struct{}), make(chan struct{})
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		if strings.HasSuffix(r.URL.Path, "/held") {
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
	t.Cleanup(func() { close(escape) })
	c := fixtureClient(t, s, nil)
	done := make(chan error, 1)
	go func() { _, e := c.Get(context.Background(), NativeKey{"held", "one"}); done <- e }()
	wait(t, started)
	_ = c.Close()
	if e := <-done; !errors.Is(e, ErrClosed) {
		t.Fatal("owned close did not cancel read", e)
	}
	wait(t, disconnected)
	if _, e := c.Get(context.Background(), NativeKey{"a", "one"}); !errors.Is(e, ErrClosed) {
		t.Fatal("closed client accepted read")
	}
	// The original independently owned caller transport still works with a fresh
	// clone/client. Closing this adapter never disables shared TLS trust or pools.
	other := fixtureClient(t, s, nil)
	if d, e := other.Get(context.Background(), NativeKey{"a", "one"}); e != nil || d == nil {
		t.Fatal("borrowed transport was closed or mutated", e)
	}
}
func TestAADWireAndUnknownChargeIsNotZero(t *testing.T) {
	var calls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		if r.Header.Get("Authorization") != "type=aad&ver=1.0&sig=owned.jwt.signature" {
			t.Error("AAD wire format changed")
		}
		fixtureJSON(w, 200, nativeDocument("a", "one"))
	})
	token, e := AADToken("owned.jwt.signature")
	if e != nil {
		t.Fatal(e)
	}
	c := fixtureClient(t, s, func(o *Options) {
		o.Authorization = func(context.Context, AuthRequest) (Credential, error) { return token, nil }
	})
	result, e := c.Read(context.Background(), NativeKey{"a", "one"})
	if e != nil || result.Metadata.RequestChargePresent || result.Metadata.RequestCharge != "" || calls.Load() != 1 {
		t.Fatal("unknown RU became zero", e)
	}
	encoded, err := json.Marshal(result.Metadata)
	if err != nil || strings.Contains(string(encoded), "\"requestCharge\":") {
		t.Fatal("unknown RU serialized as numeric zero")
	}
}
func TestNativeHeaderAndClosedInputRefusals(t *testing.T) {
	var calls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		w.Header().Set("x-ms-session-token", strings.Repeat("s", 16385))
		fixtureJSON(w, 200, nativeDocument("a", "one"))
	})
	c := fixtureClient(t, s, nil)
	if d, e := c.Get(context.Background(), NativeKey{"a", "one"}); !errors.Is(e, ErrResponse) || d != nil {
		t.Fatal("oversized service token accepted", e)
	}
	before := calls.Load()
	for _, key := range []NativeKey{{"../a", "one"}, {"a", ""}, {"a", strings.Repeat("p", 2049)}} {
		if _, e := c.Get(context.Background(), key); !errors.Is(e, ErrRequest) {
			t.Fatal("invalid native key accepted", e)
		}
	}
	cycle := map[string]any{}
	cycle["cycle"] = cycle
	for _, value := range []any{cycle, make(chan int), json.Number("NaN")} {
		if _, e := c.Query(context.Background(), Query{SQL: "SELECT * FROM c WHERE c.x = @x", Parameters: []Parameter{{"@x", value}}, Partition: "one", Limit: 1}); e == nil {
			t.Fatal("hostile parameter accepted")
		}
	}
	if calls.Load() != before {
		t.Fatal("invalid preflight performed HTTP")
	}
}

func TestLateHTTPResponseAndHostileCloseRetainOwnedCleanup(t *testing.T) {
	for _, late := range []bool{true, false} {
		t.Run(map[bool]string{true: "late-response", false: "hostile-close"}[late], func(t *testing.T) {
			s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) { fixtureJSON(w, 200, nativeDocument("a", "one")) })
			c := fixtureClient(t, s, func(o *Options) { o.Limits.MaxConcurrent = 1; o.Limits.Timeout = time.Second })
			entered, release, closed := make(chan struct{}), make(chan struct{}), make(chan struct{})
			var closes atomic.Int64
			t.Cleanup(func() {
				select {
				case <-release:
				default:
					close(release)
				}
			})
			native := c.http.Transport
			c.http.Transport = roundTripper(func(r *http.Request) (*http.Response, error) {
				selected := r
				if late {
					selected = r.Clone(context.Background())
				} // fixture ONLY: deliberately ignored cancellation
				response, e := native.RoundTrip(selected)
				if e != nil {
					return response, e
				}
				response.Body = &closingBody{ReadCloser: response.Body, hook: func() {
					closes.Add(1)
					if !late {
						close(entered)
						<-release
					}
					close(closed)
				}}
				if late {
					close(entered)
					<-release
				}
				return response, nil
			})
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { _, e := c.Get(ctx, NativeKey{"a", "one"}); done <- e }()
			wait(t, entered)
			cancel()
			if e := <-done; !errors.Is(e, context.Canceled) {
				t.Fatal("ignored owned work prevented bounded cancellation", e)
			}
			if _, e := c.Get(context.Background(), NativeKey{"a", "one"}); !errors.Is(e, ErrBusy) {
				t.Fatal("unfinished physical response cleanup freed admission", e)
			}
			close(release)
			wait(t, closed)
			if closes.Load() != 1 {
				t.Fatal("late owned body cleanup was not once")
			}
			// Probe reopening with a fresh client over the original still-valid transport.
			other := fixtureClient(t, s, nil)
			if d, e := other.Get(context.Background(), NativeKey{"a", "one"}); e != nil || d == nil {
				t.Fatal("fresh owned read failed after late cleanup", e)
			}
		})
	}
}

func TestQueryDispatchCancellationPreservesPriorContinuation(t *testing.T) {
	started, disconnected, escape := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var hold atomic.Bool
	hold.Store(true)
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		_, _ = io.Copy(io.Discard, io.LimitReader(r.Body, 65537)) // own POST EOF before disconnect wait
		if r.Header.Get("x-ms-continuation") == "" {
			w.Header().Set("x-ms-continuation", "owned-next")
			fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
			return
		}
		if hold.Swap(false) {
			close(started)
			select {
			case <-r.Context().Done():
				close(disconnected)
			case <-escape:
			}
			return
		}
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("b", "one")}, "_count": 1})
	})
	t.Cleanup(func() { close(escape) })
	c := fixtureClient(t, s, nil)
	q := Query{SQL: "SELECT * FROM c", Partition: "one", Limit: 1}
	first, e := c.Query(context.Background(), q)
	if e != nil || first.Next == nil {
		t.Fatal("initial page failed", e)
	}
	q.Continuation = first.Next
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		page, e := c.Query(ctx, q)
		if len(page.Documents) != 0 || page.Next != nil {
			done <- ErrResponse
			return
		}
		done <- e
	}()
	wait(t, started)
	if _, e = c.Query(context.Background(), q); !errors.Is(e, ErrBusy) {
		t.Fatal("inflight continuation was not reserved", e)
	}
	cancel()
	if e = <-done; !errors.Is(e, context.Canceled) {
		t.Fatal("query dispatch did not cancel whole", e)
	}
	wait(t, disconnected)
	timer := time.NewTimer(time.Second)
	defer timer.Stop()
	ticker := time.NewTicker(time.Millisecond)
	defer ticker.Stop()
	for {
		page, e := c.Query(context.Background(), q)
		if e == nil {
			if len(page.Documents) != 1 || page.Documents[0].Key.ID != "b" || page.Next != nil {
				t.Fatal("resumed native result changed")
			}
			break
		}
		if !errors.Is(e, ErrBusy) {
			t.Fatal("failed query consumed prior handle", e)
		}
		select {
		case <-ticker.C:
		case <-timer.C:
			t.Fatal("query reservation did not settle")
		}
	}
}
