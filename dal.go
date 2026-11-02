package dalgo2cosmosdb

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"maps"
	"sync"

	"github.com/dal-go/dalgo/dal"
	"github.com/dal-go/dalgo/recordset"
	"github.com/dal-go/record"
)

type backend struct {
	dal.NoConcurrency
	c *Client
}

var _ dal.Backend = (*backend)(nil)

// DB exposes configured fixed-partition collections only. It never infers a
// partition from ID, invents a managed envelope or collapses cross-partition IDs.
func (c *Client) DB() dal.DB            { return dal.NewDB(&backend{c: c}) }
func (b *backend) ID() string           { return b.c.opts.Database + "/" + b.c.opts.Container }
func (b *backend) Adapter() dal.Adapter { return dal.NewAdapter("dalgo2cosmosdb", "0.1.0") }
func (b *backend) Schema() dal.Schema   { return nil }
func (b *backend) RunReadonlyTransaction(context.Context, dal.ROTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (b *backend) RunReadwriteTransaction(context.Context, dal.RWTxWorker, ...dal.TransactionOption) error {
	return dal.ErrNotSupported
}
func (b *backend) ExecuteQueryToRecordsetReader(context.Context, dal.Query, ...recordset.Option) (dal.RecordsetReader, error) {
	return nil, dal.ErrNotSupported
}
func (b *backend) ExecuteQueryToRecordsReader(context.Context, dal.Query) (dal.RecordsReader, error) {
	return nil, dal.ErrNotSupported
}
func (b *backend) nativeKey(key *record.Key) (NativeKey, error) {
	if key == nil || key.Parent() != nil {
		return NativeKey{}, dal.ErrNotSupported
	}
	id, ok := key.ID.(string)
	if !ok {
		return NativeKey{}, dal.ErrNotSupported
	}
	binding, ok := b.c.opts.Collections[key.Collection()]
	if !ok {
		return NativeKey{}, ErrRequest
	}
	native := NativeKey{id, binding.Partition}
	return native, b.c.validateKey(native)
}
func (b *backend) Exists(ctx context.Context, key *record.Key) (bool, error) {
	native, e := b.nativeKey(key)
	if e != nil {
		return false, e
	}
	return b.c.Exists(ctx, native)
}
func (b *backend) Get(ctx context.Context, r record.Record) error {
	return b.GetMulti(ctx, []record.Record{r})
}
func (b *backend) GetMulti(parent context.Context, records []record.Record) error {
	ctx, cancel := context.WithTimeout(parent, b.c.opts.Limits.Timeout)
	defer cancel()
	check := func() error {
		if e := fresh(parent); e != nil {
			return e
		}
		if e := fresh(ctx); e != nil {
			return e
		}
		if b.c.base.Err() != nil {
			return ErrClosed
		}
		return nil
	}
	if e := check(); e != nil {
		return e
	}
	if len(records) > b.c.opts.Limits.MaxKeys || len(records) > b.c.opts.Limits.MaxRows {
		return ErrLimit
	}
	records = append([]record.Record(nil), records...)
	keys := make([]NativeKey, len(records))
	identities := make([]*record.Key, len(records))
	// These caller callbacks cannot be safely preempted. Keep them synchronous;
	// fresh guards after each prevent later I/O/delivery when they cross a deadline.
	for i, r := range records {
		if e := check(); e != nil {
			return e
		}
		if r == nil {
			return ErrRequest
		}
		key := r.Key()
		if e := check(); e != nil {
			return e
		}
		native, e := b.nativeKey(key)
		if e != nil {
			return e
		}
		keys[i] = native
		identities[i] = record.NewKeyWithID(key.Collection(), native.ID)
	}
	var admitted []NativeKey
	deadline, _ := ctx.Deadline()
	result, e := run(b.c, ctx, deadline, func(*operation) error { var err error; admitted, err = b.c.copyKeys(keys); return err }, func(op *operation) (BatchResult, error) { return b.c.readMany(op, admitted) })
	if e != nil {
		if len(records) > 0 {
			return setRecordError(records[0], e, check)
		}
		return e
	}
	// Caller Data mutations begin only after all transport/native validation.
	// Error notifications and mappings stay synchronous and cannot be rolled back.
	for i, r := range records {
		if e := check(); e != nil {
			return e
		}
		if result.Documents[i] == nil {
			e = dal.NewErrNotFoundByKey(identities[i], nil)
		} else {
			if e = setRecordError(r, nil, check); e != nil {
				return e
			}
			target := r.Data()
			if e = check(); e != nil {
				return setRecordError(r, e, check)
			}
			e = mapToTarget(target, result.Documents[i].Data)
			if cancelled := check(); cancelled != nil {
				e = cancelled
			}
		}
		if e = setRecordError(r, e, check); e != nil {
			return e
		}
	}
	return check()
}

// Valid Go contexts have sticky cancellation. Error priority only rises from
// the original outcome to Close, local deadline, then original caller cancel.
// Each callback is freshly checked; strict promotion bounds notifications to
// four without trusting callback counts or looping over hostile side effects.
func recordErrorPriority(e error) int {
	switch {
	case errors.Is(e, context.Canceled):
		return 3
	case errors.Is(e, context.DeadlineExceeded):
		return 2
	case errors.Is(e, ErrClosed):
		return 1
	default:
		return 0
	}
}
func setRecordError(r record.Record, classification error, check func() error) error {
	for {
		r.SetError(classification)
		latest := check()
		if latest == nil || recordErrorPriority(latest) <= recordErrorPriority(classification) {
			return classification
		}
		classification = latest
	}
}
func mapToTarget(target any, values map[string]any) error {
	switch v := target.(type) {
	case map[string]any:
		if v == nil {
			return ErrResponse
		}
		maps.Copy(v, values)
		return nil
	case *map[string]any:
		if v == nil {
			return ErrResponse
		}
		if *v == nil {
			*v = make(map[string]any, len(values))
		}
		maps.Copy(*v, values)
		return nil
	default:
		raw, e := json.Marshal(values)
		if e != nil {
			return ErrResponse
		}
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.UseNumber()
		if e = decoder.Decode(target); e != nil {
			return ErrResponse
		}
		return nil
	}
}

// NativeRecordsReader bridges a bounded native page to DALgo records. The
// configured logical collection must bind exactly the query's explicit partition.
// Generic dal.Query translation is deliberately not implemented in this slice.
type NativeRecordsReader struct {
	mu         sync.Mutex
	ctx        context.Context
	cancel     context.CancelFunc
	c          *Client
	collection string
	rows       []*Document
	next       *Continuation
	index      int
	closed     bool
}

func (c *Client) QueryRecords(ctx context.Context, collection string, q Query) (*NativeRecordsReader, error) {
	readCtx, cancel := context.WithTimeout(ctx, c.opts.Limits.Timeout)
	binding, ok := c.opts.Collections[collection]
	if !ok || binding.Partition != q.Partition {
		cancel()
		return nil, ErrRequest
	}
	reader := &NativeRecordsReader{ctx: readCtx, cancel: cancel, c: c, collection: collection}
	_, e := c.query(readCtx, q, func(page Page) error { reader.rows = page.Documents; reader.next = page.Next; return nil })
	if e != nil {
		cancel()
		return nil, e
	}
	// No fallible check/await after ACK: the internal query staged this factory
	// and guarded final delivery before consuming its incoming continuation.
	return reader, nil
}
func (r *NativeRecordsReader) Close() error {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.closed = true
	r.cancel()
	r.rows = nil
	return nil
}
func (r *NativeRecordsReader) Cursor() (string, error) { return "", dal.ErrNotSupported }

// NextContinuation is available only after this page's records were delivered.
// It is not a cross-process serialized cursor and it does not imply EOF.
func (r *NativeRecordsReader) NextContinuation() (*Continuation, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if e := fresh(r.ctx); e != nil {
		return nil, e
	}
	if r.c.base.Err() != nil {
		return nil, ErrClosed
	}
	if r.index != len(r.rows) {
		return nil, ErrRequest
	}
	return r.next, nil
}
func (r *NativeRecordsReader) Next() (record.Record, error) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return nil, ErrClosed
	}
	if e := fresh(r.ctx); e != nil {
		return nil, e
	}
	if r.c.base.Err() != nil {
		return nil, ErrClosed
	}
	if r.index == len(r.rows) {
		return nil, dal.ErrNoMoreRecords
	}
	row := r.rows[r.index]
	data := maps.Clone(row.Data)
	result := record.NewRecordWithData(record.NewKeyWithID(r.collection, row.Key.ID), &data)
	result.SetError(nil)
	if e := fresh(r.ctx); e != nil {
		return nil, e
	}
	if r.c.base.Err() != nil {
		return nil, ErrClosed
	}
	r.index++
	return result, nil
}

var _ dal.RecordsReader = (*NativeRecordsReader)(nil)
