package dalgo2cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

func registryUsage(c *Client) (int, int, int) {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.cursorBytes, c.cursorNodes, len(c.cursors)
}
func quotaQuery(label string) Query {
	return Query{SQL: "SELECT * FROM c WHERE c.kind = @kind", Parameters: []Parameter{{"@kind", label}}, Partition: "one", Limit: 1}
}
func queryLabel(t *testing.T, r *http.Request) string {
	t.Helper()
	raw, e := io.ReadAll(io.LimitReader(r.Body, 65537))
	if e != nil {
		t.Error("owned quota request read failed")
		return "invalid"
	}
	var body struct {
		Parameters []struct {
			Value string `json:"value"`
		} `json:"parameters"`
	}
	if json.Unmarshal(raw, &body) != nil || len(body.Parameters) != 1 {
		t.Error("owned quota query shape changed")
		return "invalid"
	}
	return body.Parameters[0].Value
}
func TestAggregateCursorRefusalPreservesPriorAndRefunds(t *testing.T) {
	var grow atomic.Bool
	grow.Store(true)
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		label := queryLabel(t, r)
		if r.Header.Get("x-ms-continuation") == "" {
			w.Header().Set("x-ms-continuation", "owned-next")
			fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument(label+"-a", "one")}, "_count": 1})
			return
		}
		if grow.Load() {
			w.Header().Set("x-ms-continuation", "owned-more")
		}
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument(label+"-b", "one")}, "_count": 1})
	})
	c := fixtureClient(t, s, func(o *Options) { o.Limits.MaxCursorBytes = 450 })
	first, e := c.Query(context.Background(), quotaQuery("alpha"))
	if e != nil || first.Next == nil {
		t.Fatal("first low-budget handle failed", e)
	}
	bytes, nodes, count := registryUsage(c)
	if bytes <= 0 || bytes > 450 || nodes <= 0 || count != 1 {
		t.Fatal("initial retained credits invalid")
	}
	if page, e := c.Query(context.Background(), quotaQuery("beta")); !errors.Is(e, ErrLimit) || len(page.Documents) != 0 || page.Next != nil {
		t.Fatal("aggregate cursor bytes returned a partial/new handle", e)
	}
	if b, n, k := registryUsage(c); b != bytes || n != nodes || k != count {
		t.Fatal("failed independent admission leaked credits")
	}
	resume := quotaQuery("alpha")
	resume.Continuation = first.Next
	c.mu.Lock()
	old := c.cursors[first.Next]
	c.mu.Unlock()
	old.budget.mu.Lock()
	beforeWork := old.budget.requests
	old.budget.mu.Unlock()
	if page, e := c.Query(context.Background(), resume); !errors.Is(e, ErrLimit) || len(page.Documents) != 0 || page.Next != nil {
		t.Fatal("oversized successor was acknowledged", e)
	}
	if b, n, k := registryUsage(c); b != bytes || n != nodes || k != 1 {
		t.Fatal("failed successor changed prior retained credits")
	}
	old.budget.mu.Lock()
	afterWork := old.budget.requests
	old.budget.mu.Unlock()
	if afterWork <= beforeWork {
		t.Fatal("failed admission refunded actually incurred work")
	}
	grow.Store(false)
	page, e := c.Query(context.Background(), resume)
	if e != nil || len(page.Documents) != 1 || page.Next != nil {
		t.Fatal("failed quota admission consumed prior position", e)
	}
	if b, n, k := registryUsage(c); b != 0 || n != 0 || k != 0 {
		t.Fatal("terminal resume failed to refund prior retention")
	}
	if _, e = c.Query(context.Background(), quotaQuery("beta")); e != nil {
		t.Fatal("released credits could not be reused", e)
	}
	_ = c.Close()
	_ = c.Close()
	if b, n, k := registryUsage(c); b != 0 || n != 0 || k != 0 {
		t.Fatal("Close retention refund was not idempotent")
	}
}
func TestCursorQuotaAdmitsNetReplacementCost(t *testing.T) {
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		label := queryLabel(t, r)
		token := r.Header.Get("x-ms-continuation")
		id := label + "-a"
		next := label + "-1"
		if token == label+"-1" {
			id = label + "-b"
			next = label + "-2"
		}
		if token == label+"-2" {
			id = label + "-c"
			next = ""
		}
		if next != "" {
			w.Header().Set("x-ms-continuation", next)
		}
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument(id, "one")}, "_count": 1})
	})
	c := fixtureClient(t, s, func(o *Options) { o.Limits.MaxCursorBytes = 1100 })
	first, e := c.Query(context.Background(), quotaQuery("q"))
	if e != nil {
		t.Fatal(e)
	}
	other, e := c.Query(context.Background(), quotaQuery("r"))
	if e != nil {
		t.Fatal(e)
	}
	before, _, count := registryUsage(c)
	if count != 2 || before > 1100 {
		t.Fatal("two small cursor admissions failed")
	}
	resume := quotaQuery("q")
	resume.Continuation = first.Next
	second, e := c.Query(context.Background(), resume)
	if e != nil || second.Next == nil {
		t.Fatal("net replacement wrongly charged old plus new retained state", e)
	}
	after, _, count := registryUsage(c)
	if count != 2 || after <= before || after > 1100 {
		t.Fatal("net replacement ledger invalid")
	}
	c.mu.Lock()
	_, oldPresent := c.cursors[first.Next]
	_, otherPresent := c.cursors[other.Next]
	c.mu.Unlock()
	if oldPresent || !otherPresent {
		t.Fatal("net replacement consumed wrong handle")
	}
	resume.Continuation = second.Next
	if page, e := c.Query(context.Background(), resume); e != nil || page.Next != nil {
		t.Fatal("terminal replacement failed", e)
	}
	if b, _, k := registryUsage(c); k != 1 || b <= 0 || b >= after {
		t.Fatal("terminal journey did not release net retention")
	}
}
func TestCursorNodeQuotaAndCancelledFactoryDoNotLeak(t *testing.T) {
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		label := queryLabel(t, r)
		id := label + "-a"
		if r.Header.Get("x-ms-continuation") == "" {
			w.Header().Set("x-ms-continuation", "owned-next")
		} else {
			id = label + "-b"
		}
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument(id, "one")}, "_count": 1})
	})
	c := fixtureClient(t, s, func(o *Options) { o.Limits.MaxCursorNodes = 7 })
	first, e := c.Query(context.Background(), quotaQuery("q"))
	if e != nil || first.Next == nil {
		t.Fatal(e)
	}
	bytes, nodes, count := registryUsage(c)
	if nodes != 7 || count != 1 {
		t.Fatal("initial logical node charge changed")
	}
	if _, e = c.Query(context.Background(), quotaQuery("r")); !errors.Is(e, ErrLimit) {
		t.Fatal("aggregate cursor nodes were unbounded", e)
	}
	resume := quotaQuery("q")
	resume.Continuation = first.Next
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	_, e = c.query(ctx, resume, func(Page) error { cancel(); return nil })
	if !errors.Is(e, context.Canceled) {
		t.Fatal("factory cancellation did not reject page", e)
	}
	if b, n, k := registryUsage(c); b != bytes || n != nodes || k != count {
		t.Fatal("cancelled delivery consumed or leaked retained credits")
	}
	if _, e = c.Query(context.Background(), resume); e != nil {
		t.Fatal("cancelled factory lost retryable prior", e)
	}
	if b, n, k := registryUsage(c); b != 0 || n != 0 || k != 0 {
		t.Fatal("successful terminal retry leaked credits")
	}
}
func TestExpiredCursorAndCloseRefundRetentionOnce(t *testing.T) {
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		label := queryLabel(t, r)
		w.Header().Set("x-ms-continuation", "owned-next")
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument(label+"-a", "one")}, "_count": 1})
	})
	c := fixtureClient(t, s, func(o *Options) { o.Limits.MaxCursorBytes = 450 })
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	first, e := c.Query(ctx, quotaQuery("old"))
	if e != nil || first.Next == nil {
		t.Fatal(e)
	}
	<-ctx.Done() // actual local journey expiry, no arbitrary sleep
	if _, e = c.Query(context.Background(), quotaQuery("new")); e != nil {
		t.Fatal("expired idle retention blocked admission", e)
	}
	if b, n, k := registryUsage(c); b <= 0 || b > 450 || n != 7 || k != 1 {
		t.Fatal("expiry/refund ledger changed")
	}
	c.mu.Lock()
	c.pruneCursors()
	c.pruneCursors()
	c.mu.Unlock()
	_ = c.Close()
	_ = c.Close()
	if b, n, k := registryUsage(c); b != 0 || n != 0 || k != 0 {
		t.Fatal("expiry/Close double-refunded retention")
	}
}
func TestConcurrentCursorACKAdmissionIsAtomic(t *testing.T) {
	arrived, release := make(chan struct{}, 2), make(chan struct{})
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		label := queryLabel(t, r)
		arrived <- struct{}{}
		<-release
		w.Header().Set("x-ms-continuation", "owned-next")
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument(label+"-a", "one")}, "_count": 1})
	})
	t.Cleanup(func() {
		select {
		case <-release:
		default:
			close(release)
		}
	})
	c := fixtureClient(t, s, func(o *Options) { o.Limits.MaxCursorBytes = 450 })
	type result struct {
		page Page
		err  error
	}
	done := make(chan result, 2)
	for _, label := range []string{"one", "two"} {
		go func(label string) {
			page, e := c.Query(context.Background(), quotaQuery(label))
			done <- result{page, e}
		}(label)
	}
	wait(t, arrived)
	wait(t, arrived)
	close(release)
	success, refused := 0, 0
	for i := 0; i < 2; i++ {
		result := <-done
		if result.err == nil {
			success++
			if result.page.Next == nil {
				t.Fatal("successful ACK omitted successor")
			}
		} else if errors.Is(result.err, ErrLimit) {
			refused++
			if len(result.page.Documents) != 0 || result.page.Next != nil {
				t.Fatal("failed concurrent ACK leaked result")
			}
		} else {
			t.Fatal(result.err)
		}
	}
	if success != 1 || refused != 1 {
		t.Fatal("concurrent ACK over-admitted retained state")
	}
	if b, n, k := registryUsage(c); b <= 0 || b > 450 || n != 7 || k != 1 {
		t.Fatal("atomic retained ledger invalid")
	}
	_ = c.Close()
	if b, n, k := registryUsage(c); b != 0 || n != 0 || k != 0 {
		t.Fatal("concurrent cleanup leaked credits")
	}
}
