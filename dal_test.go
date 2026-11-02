package dalgo2cosmosdb

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/record"
)

type observedRecord struct {
	record.Record
	onKey   func()
	onData  func()
	onError func(error)
}

func (r observedRecord) Key() *record.Key {
	if r.onKey != nil {
		r.onKey()
	}
	return r.Record.Key()
}
func (r observedRecord) Data() any {
	if r.onData != nil {
		r.onData()
	}
	return r.Record.Data()
}
func (r observedRecord) SetError(e error) record.Record {
	if r.onError != nil {
		r.onError(e)
	}
	return r.Record.SetError(e)
}
func TestDALgoNativeFixedPartitionIdentityMissingAndNumbers(t *testing.T) {
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		id := strings.TrimPrefix(r.URL.Path, "/dbs/existing/colls/native/docs/")
		if id == "missing" {
			w.WriteHeader(404)
			return
		}
		var pk []string
		_ = json.Unmarshal([]byte(r.Header.Get("x-ms-documentdb-partitionkey")), &pk)
		fixtureJSON(w, 200, nativeDocument(id, pk[0]))
	})
	c := fixtureClient(t, s, nil)
	db := c.DB()
	var one, two map[string]any
	r1 := record.NewRecordWithData(record.NewKeyWithID("items", "a"), &one)
	r2 := record.NewRecordWithData(record.NewKeyWithID("other-partition", "a"), &two)
	if e := db.Get(context.Background(), r1); e != nil {
		t.Fatal(e)
	}
	if e := db.Get(context.Background(), r2); e != nil {
		t.Fatal(e)
	}
	if one["tenant"] != "one" || two["tenant"] != "two" || one["exact"] != json.Number("9007199254740993") || one["fraction"] != json.Number("12345678901234567890.0001") || one["id"] != "a" || one["_etag"] != "\"owned-etag\"" {
		t.Fatal("DALgo bridge rewrote native identity or numbers")
	}
	var missing map[string]any
	absent := record.NewRecordWithData(record.NewKeyWithID("items", "missing"), &missing)
	if e := db.Get(context.Background(), absent); e == nil || !record.IsNotFound(e) {
		t.Fatal("native missing is not DALgo notfound", e)
	}
	if exists, e := db.Exists(context.Background(), record.NewKeyWithID("items", "missing")); e != nil || exists {
		t.Fatal("DALgo Exists lost missing", e)
	}
	var called bool
	if e := db.RunReadonlyTransaction(context.Background(), func(context.Context, dal.ReadTransaction) error { called = true; return nil }); !errors.Is(e, dal.ErrNotSupported) || called {
		t.Fatal("callback transaction falsely accepted", e)
	}
}
func TestDALgoStagesBeforeDataCallbacksAndFreshCancellation(t *testing.T) {
	var requests atomic.Int64
	var dataCalls atomic.Int64
	var denied atomic.Bool
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		id := strings.TrimPrefix(r.URL.Path, "/dbs/existing/colls/native/docs/")
		if denied.Load() && id == "b" {
			w.WriteHeader(403)
			return
		}
		fixtureJSON(w, 200, nativeDocument(id, "one"))
	})
	c := fixtureClient(t, s, nil)
	var a, b map[string]any
	r1 := observedRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "a"), &a), onData: func() { dataCalls.Add(1) }}
	r2 := observedRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "b"), &b), onData: func() { dataCalls.Add(1) }}
	denied.Store(true)
	e := c.DB().GetMulti(context.Background(), []record.Record{r1, r2})
	var status *HTTPError
	if !errors.As(e, &status) || status.Status != 403 || dataCalls.Load() != 0 || a != nil || b != nil {
		t.Fatal("transport failure partially delivered caller data", e)
	}
	denied.Store(false)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	before := requests.Load()
	var target map[string]any
	keyAbort := observedRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "a"), &target), onKey: cancel}
	if e = c.DB().Get(ctx, keyAbort); !errors.Is(e, context.Canceled) || requests.Load() != before {
		t.Fatal("caller Key callback dispatched after abort", e)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	target = map[string]any{"keep": "unchanged"}
	dataAbort := observedRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "a"), &target), onData: cancel}
	if e = c.DB().Get(ctx, dataAbort); !errors.Is(e, context.Canceled) || target["id"] != nil || target["keep"] != "unchanged" || !errors.Is(dataAbort.Error(), context.Canceled) {
		t.Fatal("Data callback cancellation delivered late native data", e)
	}
	ctx, cancel = context.WithCancel(context.Background())
	defer cancel()
	target = nil
	errorAbort := observedRecord{Record: record.NewRecordWithData(record.NewKeyWithID("items", "a"), &target), onError: func(error) { cancel() }}
	if e = c.DB().Get(ctx, errorAbort); !errors.Is(e, context.Canceled) || target != nil || !errors.Is(errorAbort.Error(), context.Canceled) {
		t.Fatal("SetError callback cancellation was not fresh", e)
	}
}
func TestNativeDALgoReaderAndConfiguredOnlyMetadata(t *testing.T) {
	var calls atomic.Int64
	s := fixtureServer(t, func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		fixtureJSON(w, 200, map[string]any{"Documents": []any{nativeDocument("a", "one")}, "_count": 1})
	})
	c := fixtureClient(t, s, nil)
	metadata := c.ConfiguredMetadata()
	if calls.Load() != 0 || !metadata.ReadOnly || metadata.PartitionField != "tenant" {
		t.Fatal("configured metadata performed service discovery")
	}
	metadata.Collections["items"] = Collection{Partition: "mutated"}
	if c.ConfiguredMetadata().Collections["items"].Partition != "one" {
		t.Fatal("configuration was not copied")
	}
	reader, e := c.QueryRecords(context.Background(), "items", Query{SQL: "SELECT * FROM c", Partition: "one", Limit: 1})
	if e != nil {
		t.Fatal(e)
	}
	defer func() { _ = reader.Close() }()
	row, e := reader.Next()
	if e != nil || row.Key().ID != "a" {
		t.Fatal("native records reader lost identity", e)
	}
	if _, e = reader.Next(); !errors.Is(e, dal.ErrNoMoreRecords) {
		t.Fatal("reader EOF changed", e)
	}
	if next, e := reader.NextContinuation(); e != nil || next != nil {
		t.Fatal("completed native page gained a cursor", e)
	}
	if _, e = reader.Cursor(); !errors.Is(e, dal.ErrNotSupported) {
		t.Fatal("opaque handle misrepresented as portable string")
	}
	_ = reader.Close()
	if _, e = reader.Next(); !errors.Is(e, ErrClosed) {
		t.Fatal("closed reader delivered data")
	}
	before := calls.Load()
	if _, e = c.QueryRecords(context.Background(), "items", Query{SQL: "SELECT * FROM c", Partition: "other", Limit: 1}); !errors.Is(e, ErrRequest) || calls.Load() != before {
		t.Fatal("bridge inferred a different partition", e)
	}
}
