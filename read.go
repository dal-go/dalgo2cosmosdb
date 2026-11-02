package dalgo2cosmosdb

import (
	"context"
	"time"
)

func (c *Client) validateKey(key NativeKey) error {
	if !validID(key.ID) || !validText(key.Partition, 2048) {
		return ErrRequest
	}
	return nil
}
func readOptions(options []ReadOptions) (ReadOptions, error) {
	if len(options) > 1 {
		return ReadOptions{}, ErrRequest
	}
	if len(options) == 1 {
		return options[0], nil
	}
	return ReadOptions{}, nil
}
func (c *Client) readOne(op *operation, key NativeKey, options ReadOptions) (ReadResult, error) {
	session, e := c.session(key.Partition, options.Session)
	if e != nil {
		return ReadResult{}, e
	}
	response, e := c.request(op, "GET", key.ID, key.Partition, nil, "", session, 0)
	if e != nil {
		return ReadResult{}, e
	}
	if response.status == 404 {
		return ReadResult{Metadata: response.meta}, nil
	}
	if response.continuation != "" {
		return ReadResult{}, ErrResponse
	}
	document, e := c.document(op, response.body, key, response.meta)
	if e != nil {
		return ReadResult{}, e
	}
	return ReadResult{document, response.meta}, nil
}

// Read includes metadata even when the document is absent. RequestChargePresent
// is false for an absent header: an unknown charge is never advertised as zero.
func (c *Client) Read(ctx context.Context, key NativeKey, options ...ReadOptions) (ReadResult, error) {
	var opt ReadOptions
	return run(c, ctx, time.Now().Add(c.opts.Limits.Timeout), func(*operation) error {
		var e error
		opt, e = readOptions(options)
		if e != nil {
			return e
		}
		if e = c.validateKey(key); e != nil {
			return e
		}
		_, e = c.session(key.Partition, opt.Session)
		return e
	}, func(op *operation) (ReadResult, error) { return c.readOne(op, key, opt) })
}
func (c *Client) Get(ctx context.Context, key NativeKey, options ...ReadOptions) (*Document, error) {
	var opt ReadOptions
	return run(c, ctx, time.Now().Add(c.opts.Limits.Timeout), func(*operation) error {
		var e error
		opt, e = readOptions(options)
		if e != nil {
			return e
		}
		if e = c.validateKey(key); e != nil {
			return e
		}
		_, e = c.session(key.Partition, opt.Session)
		return e
	}, func(op *operation) (*Document, error) { r, e := c.readOne(op, key, opt); return r.Document, e })
}
func (c *Client) Exists(ctx context.Context, key NativeKey, options ...ReadOptions) (bool, error) {
	var opt ReadOptions
	return run(c, ctx, time.Now().Add(c.opts.Limits.Timeout), func(*operation) error {
		var e error
		opt, e = readOptions(options)
		if e != nil {
			return e
		}
		if e = c.validateKey(key); e != nil {
			return e
		}
		_, e = c.session(key.Partition, opt.Session)
		return e
	}, func(op *operation) (bool, error) { r, e := c.readOne(op, key, opt); return r.Document != nil, e })
}
func (c *Client) copyKeys(keys []NativeKey) ([]NativeKey, error) {
	if len(keys) > c.opts.Limits.MaxKeys || len(keys) > c.opts.Limits.MaxRows {
		return nil, ErrLimit
	}
	copy := append([]NativeKey(nil), keys...)
	bytes := 0
	for _, key := range copy {
		if e := c.validateKey(key); e != nil {
			return nil, e
		}
		bytes += len(key.ID) + len(key.Partition)
		if bytes > c.opts.Limits.MaxRequestBytes {
			return nil, ErrLimit
		}
	}
	return copy, nil
}
func (c *Client) readMany(op *operation, keys []NativeKey) (BatchResult, error) {
	result := BatchResult{Documents: make([]*Document, 0, len(keys)), Metadata: make([]Metadata, 0, len(keys))}
	// Deliberately sequential. One deadline, aggregate raw-body/row/request budget,
	// ordered duplicates/misses, and no later dispatch after any failed read.
	for _, key := range keys {
		if e := op.check(); e != nil {
			return BatchResult{}, e
		}
		r, e := c.readOne(op, key, ReadOptions{})
		if e != nil {
			return BatchResult{}, e
		}
		result.Documents = append(result.Documents, r.Document)
		result.Metadata = append(result.Metadata, r.Metadata)
	}
	return result, nil
}
func (c *Client) ReadMany(ctx context.Context, keys []NativeKey) (BatchResult, error) {
	var admitted []NativeKey
	return run(c, ctx, time.Now().Add(c.opts.Limits.Timeout), func(*operation) error { var e error; admitted, e = c.copyKeys(keys); return e }, func(op *operation) (BatchResult, error) { return c.readMany(op, admitted) })
}
func (c *Client) GetMany(ctx context.Context, keys []NativeKey) ([]*Document, error) {
	var admitted []NativeKey
	return run(c, ctx, time.Now().Add(c.opts.Limits.Timeout), func(*operation) error { var e error; admitted, e = c.copyKeys(keys); return e }, func(op *operation) ([]*Document, error) { r, e := c.readMany(op, admitted); return r.Documents, e })
}
