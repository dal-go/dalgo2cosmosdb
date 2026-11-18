// Package dalgo2cosmosdb reads native existing Cosmos DB for NoSQL documents.
// It does not create a managed layout, provision resources, or perform retries.
package dalgo2cosmosdb

import (
	"encoding/json"
	"errors"
	"net/http"
	"time"
)

var (
	ErrConfiguration = errors.New("cosmos configuration is invalid")
	ErrRequest       = errors.New("cosmos read request is unsupported or invalid")
	ErrResponse      = errors.New("cosmos response is invalid")
	ErrLimit         = errors.New("cosmos read exceeds its configured budget")
	ErrTransport     = errors.New("cosmos request failed")
	ErrClosed        = errors.New("cosmos client is closed")
	ErrBusy          = errors.New("cosmos owned work capacity is exhausted")
)

// Limits bound local materialized results and dispatch work, not Cosmos engine
// memory, scanned work, credential-provider allocation or Azure charges.
type Limits struct {
	Timeout                                                                                             time.Duration
	MaxRows, MaxKeys, MaxPages, MaxRequests, MaxResponseBytes, MaxRequestBytes, MaxConcurrent, PageSize int
	MaxCursorBytes, MaxCursorNodes                                                                      int
}

// Collection binds a DALgo collection to one explicit native string partition.
// Native methods accept other explicit partitions without rewriting document ID.
type Collection struct{ Partition string }
type Options struct {
	Database, Container, PartitionField, AuthScopeID string
	Authorization                                    AuthorizationProvider
	Transport                                        *http.Transport
	Limits                                           Limits
	Collections                                      map[string]Collection
}
type NativeKey struct{ ID, Partition string }

// Session is an opaque same-client/container/partition/auth-scope handle. Its
// raw service token is not serialized or exposed and is never globally merged.
type Session struct {
	owner            *Client
	partition, token string
}

func (*Session) String() string   { return "cosmos session [redacted]" }
func (*Session) GoString() string { return "cosmos session [redacted]" }

type ReadOptions struct{ Session *Session }

// Continuation is instance-owned. Copying its value or serializing it does not
// produce a valid new handle. One successful resume consumes the prior handle.
type Continuation struct{ marker byte }
type Parameter struct {
	Name  string
	Value any
}
type Query struct {
	SQL          string
	Parameters   []Parameter
	Partition    string
	Limit        int
	Continuation *Continuation
	Session      *Session
}
type Metadata struct {
	RequestCharge        json.Number `json:"requestCharge,omitempty"`
	RequestChargePresent bool        `json:"requestChargePresent"`
	ActivityID           string
	ETag                 string
	Session              *Session `json:"-"`
}
type Document struct {
	Key      NativeKey
	Data     map[string]any
	Metadata Metadata
}
type Page struct {
	Documents []*Document
	Next      *Continuation
	Metadata  []Metadata
	Pages     int
}
type ReadResult struct {
	Document *Document
	Metadata Metadata
}
type BatchResult struct {
	Documents []*Document
	Metadata  []Metadata
}
type ConfiguredMetadata struct {
	Database, Container, PartitionField string
	ReadOnly                            bool
	Limits                              Limits
	Collections                         map[string]Collection
}

// HTTPError exposes only safe status/metadata. No server body, URL, SQL,
// authorization, transport cause or token appears in Error or Unwrap.
type HTTPError struct {
	Status   int
	Metadata Metadata
}

func (e *HTTPError) Error() string { return "cosmos HTTP read failed" }

type RateLimitError struct {
	HTTPError
	RetryAfter time.Duration
}

func (e *RateLimitError) Error() string { return "cosmos read was throttled" }
func (e *RateLimitError) As(target any) bool {
	if value, ok := target.(**HTTPError); ok {
		*value = &e.HTTPError
		return true
	}
	return false
}
