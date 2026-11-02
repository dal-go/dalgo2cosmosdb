# DALgo Go native Cosmos DB for NoSQL reads

This adapter reads **existing native documents** in an explicitly configured
Cosmos DB for NoSQL container and partition. It preserves native `id`, partition
field, `_rid`, `_etag`, `_ts`, `_self`, `_attachments` and custom JSON fields.
It creates no account, database, container, permission, index, managed key or
`data` envelope. Native API keys contain the original document ID and partition.

This is an initial source implementation. Owned verified TLS protocol fixtures
are included for contained qualification. They are not an actual Cosmos DB
engine/emulator, Azure resource-token/AAD authorization, session consistency,
RU/billing, hosted CORS, installed CLI, or published module acceptance proof.

## Native API

```go
client, err := dalgo2cosmosdb.New("https://account.documents.azure.com", dalgo2cosmosdb.Options{
    Database: "existing", Container: "documents", PartitionField: "tenant",
    AuthScopeID: "broker-reader-identity",
    Authorization: func(ctx context.Context, request dalgo2cosmosdb.AuthRequest) (dalgo2cosmosdb.Credential, error) {
        raw, err := brokerResourceToken(ctx, request) // application-owned broker
        if err != nil { return dalgo2cosmosdb.Credential{}, err }
        return dalgo2cosmosdb.ResourceToken(raw) // raw, NOT already percent encoded
    },
    Collections: map[string]dalgo2cosmosdb.Collection{"items": {Partition: "tenant-a"}},
})
// Handle err, then defer client.Close(). Construction performs no I/O.
result, err := client.Read(ctx, dalgo2cosmosdb.NativeKey{ID: "existing-id", Partition: "tenant-a"})
// result.Document == nil means a point-read404; result.Metadata still exists.
page, err := client.Query(ctx, dalgo2cosmosdb.Query{
    SQL: "SELECT * FROM c WHERE c.kind = @kind",
    Parameters: []dalgo2cosmosdb.Parameter{{Name: "@kind", Value: "open"}},
    Partition: "tenant-a", Limit: 100,
})
```

`Get` is the document-only convenience form of `Read`; `Exists` uses a point
read. `ReadMany` returns ordered documents and per-request metadata, including
missing-document charges. `GetMany` returns ordered documents with `nil` misses.
Duplicate keys produce independent native documents. Batches are deliberately
sequential under one original deadline and shared row/body/request budget;
there is no subsequent key dispatch after a failure. Inputs are copied before
asynchronous credential or transport work.

The first native partition profile is **one explicit nonempty string value**
from a top-level configured field. Numeric, boolean, null, empty, undefined,
unpartitioned, hierarchical and nested partition paths are **not implemented in
this adapter slice**. They are not blanket Cosmos DB limitations. No missing
partition is guessed from an ID, query predicate, or cross-partition setting.
A document ID is unique within a partition, not across the entire container.

`Query` sends the SQL text and named parameter values separately. It performs
only POST to the configured document-query endpoint, sets the exact
`application/query+json` content type, and never enables cross-partition fanout
or scans. This is not a SQL grammar/plan validator or service-work billing cap.
The current result profile requires complete native documents with the original
`id` and configured partition field. Scalar/aggregate rows, projected documents
without required identity, and joined duplicate identities are not implemented
result profiles. No native scalar is rewrapped as a fake record.

Parameter values accept closed JSON (`nil`, booleans, strings, `json.Number`,
`int`, `int64`, `uint64`, finite `float64`, `[]any`, `map[string]any`). Custom
marshalers/classes and other Go value types are refused before authentication.
Closed parameter copying admits the aggregate **escaped JSON wire bytes** before
marshalling, including SQL, names, map keys, punctuation, number lexemes and
every repeated string occurrence. HTML/control/Unicode escapes cannot bypass
that admission; the final actual request-byte check remains.
Responses are bounded before decode and use `json.Number`: number text is
preserved without a `float64` round trip. This preserves the **wire** value;
it does not claim Cosmos stored arbitrary-precision numbers. Invalid UTF8,
lone surrogate escapes, duplicate JSON members, trailing JSON values and
excessive tree depth/node counts fail the entire result.

## Authorization and transport

`Authorization` is called afresh inside the operation deadline/admission scope
with its context and the exact configured method/resource/AuthScopeID. Callers
supply broker-issued read-only authority. There is no account key, master-key
HMAC, permission creation, administrative or alternate-credential fallback.
Provider/network errors return safe sentinel errors without raw causes, URLs,
SQL text, tokens or service error bodies. Credential/session formatting is
redacted; secret fields and raw session tokens are not exported.

* `ResourceToken` takes raw `type=resource&ver=1.0&sig=...` and percent-encodes
  the entire value once, matching the official JS resource-token path. The
  accepted signature alphabet is ASCII alphanumeric plus `+/=_-`; this excludes
  ambiguous percent-encoded or extra-field forms and preserves plus/slash/equal
  bytes exactly. It does not Base64-encode the complete authorization string.
* `AADToken` takes a raw access token and emits
  `type=aad&ver=1.0&sig=<token>` like the official Go Cosmos bearer policy.
  Acquiring the token, audience and Azure RBAC are the provider's responsibility.
* The Microsoft access-control page's generic Base64 prose conflicts with its
  percent-encoding examples and official SDK behavior. These explicit per-mode
  contracts follow the official sources linked below; no encoding autodetection.

Endpoint URLs must be HTTPS, without userinfo/query/fragment or an arbitrary
base path. Each configured native path component is escaped once, preserving
case and ID. IDs have a local1..1024UTF8-byte cap and refuse separators,
controls and dot segments; actual service restrictions remain service-side
qualification gates. TLS verification remains enabled, including owned test CA
trust. An optional `*http.Transport` is cloned, never mutated or closed on the
caller's behalf. Ambient proxies, cookie jars, redirects and compression are
not used. Keepalive is disabled in this first profile to avoid reused-connection
replay. There is exactly one adapter dispatch per read/page and no automatic
429/5xx retry, region failover, sleep or backoff.

## Lifecycle and budgets

An original context deadline starts before input copying, authentication,
admission and serialization, and covers all pages or keys in a call. Monotonic
freshness checks follow callbacks, body cleanup, parsing, mapping and final
delivery. Caller cancellation/deadline and client Close stop local waiting;
ignored physical credential/transport/body-close work retains its bounded
admission lease until it really settles. `Close` is idempotent and cancels only
owned work/idle connections. It never destroys a credential provider or borrowed
client. Rejection and late-response cleanup are observed; no abandoned worker
can mutate a caller-owned DALgo record.

Default limits (explicitly configurable within constructor bounds):

| Limit | Default |
| --- | ---: |
| Whole-call timeout |30seconds |
| Physical concurrent operations |4; no unbounded waiting queue |
| Ordered batch keys |100 |
| Materialized rows |10000 |
| Query HTTP page hint |100 |
| Journey pages/owned attempts |100each |
| Aggregate response body |4MiB |
| Aggregate serialized request body |64KiB |
| Response headers |64KiB |
| Individual continuation/session token |16KiB |
| Aggregate retained cursor logical bytes/nodes |16MiB/65536 |
| JSON depth/nodes |32/65536 |

An overflow, invalid page/count, foreign partition, duplicate native identity or
malformed response returns an error with **no partial document prefix**. Response
body limits apply before JSON decoding. They do not bound TLS/kernel buffers,
service execution memory, scanned/index work, a single standard-library/network
allocation, arbitrary credential-provider allocation or caller callback work.

`HTTPError` exposes a safe status and bounded response metadata. A429 is a
`RateLimitError`, exposing the provider's bounded `RetryAfter` without retrying.
`RequestCharge` retains observed decimal text only when
`RequestChargePresent == true`; an absent charge is unknown and omitted from
JSON, never zero. `ReadMany`/`Page` keep per-request/page metadata rather than
silently rounding or inventing a total. There is **no prepaid RU or billing cap**.
Even an eventual observed-charge admission rule cannot undo already incurred,
unknown or server-side charges; it is not implemented by this first slice.

## Continuation and session scope

A short or empty document page with nonempty `x-ms-continuation` is not EOF.
`Query` keeps following pages under the same limits until its explicit `Limit`
or an absent/empty continuation. The opaque continuation is not parsed as a URL,
reconstructed from the last row, or accepted from arbitrary serialized input.

Handles are instance/container/partition/query/parameters/page-size/auth/session
scoped. A successful resume consumes its prior handle only at final delivery;
a failed resume preserves the prior position while retaining incurred journey
row/body/page/attempt work. Concurrent reuse is refused. Repeated service tokens
are refused to avoid loops. At most1000pending handles are retained, with an additional aggregate logical
byte/node admission across all live journeys. It includes UTF8 IDs/tokens and
conservative fixed entry charges; it is not an exact Go heap or process RAM
guarantee. Net replacement cost is checked atomically at ACK, preserving the
prior position on refusal. Expiry/Close refunds each retained entry once, and
active entries are not silently evicted. Scope fingerprints are fixed-size,
domain-separated SHA256 over length-delimited exact original scope components;
raw query JSON is not re-escaped into every handle. The original
journey deadline does not reset on resume and expired idle handles are retired.
This is a local admission/expiry policy, not a claim service tokens expire.

A response's opaque session token can be supplied as a same-client/partition
`ReadOptions.Session` or `Query.Session`; it is echoed exactly with deliberate
Session consistency. Observing a response session header alone never enables a
consistency override or token echo on later default-query pages/resumes.
No global last-token cache or unsafe concurrent token
merge exists. Scope changes require a new client/AuthScopeID. The label binds
local handles; it is not cryptographic verification of provider permissions.
Continuation/session handles intentionally are not portable across processes.

## DALgo bridge

`client.DB()` implements DALgo0.97 read point operations for configured logical
collections. Each collection explicitly binds this existing container and one
fixed partition. Native document IDs remain the DALgo key IDs. Different logical
collections can bind different partitions without collapsing equal native IDs.
Transport results are fully staged before `Data` mapping; caller `Key`, `Data`,
`SetError` and custom decoding callbacks stay synchronous on the caller path,
with fresh checks after returning, including every error reclassification.
Sticky caller cancellation outranks local deadline and Close; monotonic priority
bounds error-notification convergence without abandoning caller mutations.
Arbitrary blocking callback code cannot be
safely preempted, and arbitrary caller side effects cannot be rolled back.

`QueryRecords(ctx, collection, nativeQuery)` exposes a bounded native page as a
DALgo records reader. Its configured partition must match exactly. `Next` checks
caller deadline, reader Close and client Close before acknowledging each row.
After exhaustion, `NextContinuation` returns the optional owned native handle.
`Cursor()` returns `dal.ErrNotSupported` rather than presenting it as a portable
string. Reader construction and its final guard are staged before the same
query delivery ACK; an unsuccessful factory does not consume its incoming
continuation. Generic `dal.Query` translation and recordset/ordinal mapping are not
implemented; use the explicit native query surface.

Schema discovery/mutation, writes and callback/multi-request transactions are
not implemented. Cosmos itself supports writes and single-partition transaction
facilities; this adapter's read-only implementation does not mean the provider
intrinsically lacks them. Transaction callbacks are refused without invocation.

## Qualification

Source fixtures exercise synthetic owned verified TLS native paths/headers,
resource/AAD encoding, JSON values, partitions, sessions, continuation progress,
counts, whole-result bounds,429/no retry, dispatch/disconnect cancellation,
credential/late-body cleanup, physical admission, reopen and DALgo callback
staging. Execution receipts belong to the orchestration check lane; source-only
presence is not a passing test claim. Public clean-module/installed CLI/actual
Cosmos emulator/Azure authorization/query/session/RU qualification remain
separate gates. No Azure account or production data is used by these fixtures.

Protocol sources:

* [Get document](https://learn.microsoft.com/en-us/rest/api/cosmos-db/get-a-document)
* [Query documents](https://learn.microsoft.com/en-us/rest/api/cosmos-db/query-documents)
* [Pagination](https://learn.microsoft.com/cosmos-db/query/pagination)
* [Response metadata](https://learn.microsoft.com/en-us/rest/api/cosmos-db/common-cosmosdb-rest-response-headers)
* [Official resource-token encoding](https://github.com/Azure/azure-sdk-for-js/blob/main/sdk/cosmosdb/cosmos/src/auth.ts)
* [Official Go AAD policy](https://github.com/Azure/azure-sdk-for-go/blob/bcc10c163a2669db50edfda4e4cbc2898826861f/sdk/data/azcosmos/cosmos_policy_bearer_token.go)
* [Partition precision guidance](https://learn.microsoft.com/en-us/azure/cosmos-db/partitioning-overview)
