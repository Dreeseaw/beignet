# Wire contract

This is the v0.1 HTTP contract between heads, workers, and the Go control
plane. JSON fields not described as interpreted by the control plane are
opaque application data.

All endpoints are unauthenticated HTTP in v0.1. Use them only on loopback or a
trusted private network.

## Service state

### `GET /healthz`

Returns `200` when the HTTP process is serving. It does not imply that a Raft
leader exists or that writes can complete.

### `GET /readyz`

Returns `200` when the node is the leader or can route writes to a known
leader. Otherwise it returns `503`.

### `GET /v1/status`

Returns the contacted node's current Raft view:

```json
{
  "node_id": "node2",
  "state": "Follower",
  "leader_id": "node1",
  "leader_raft_addr": "127.0.0.1:7001",
  "leader_http_addr": "127.0.0.1:4710"
}
```

Leader fields may be absent while there is no known or routable leader.

## Immutable artifacts

An artifact ID is exactly 64 lowercase hexadecimal characters: the SHA-256 of
its bytes. Each request body is limited to 64 MiB.

### `PUT /v1/blob/{hash}`

Uploads the raw bytes. The server verifies the path hash, stores the bytes,
then commits their hash and size through Raft.

- `200`: stored, or the same immutable object was already present
- `400`: malformed hash or hash mismatch
- `413`: body exceeds 64 MiB or could not be read within the limit
- `503`: artifact-store or consensus failure

The operation is safe to retry with the same hash and bytes. A storage success
followed by a consensus failure can leave an unreferenced object.

### `GET /v1/blob/{hash}`

Returns the exact bytes as `application/octet-stream`.

- `200`: bytes returned
- `404`: the contacted replica has no committed metadata for the hash
- `500`: metadata exists but the shared store has no corresponding bytes
- `503`: artifact-store read failed

### `POST /v1/blobs/missing`

Checks the contacted replica's local metadata:

```json
{ "hashes": ["<sha256-a>", "<sha256-b>"] }
```

The response preserves the request order for hashes that are not known:

```json
{ "missing": ["<sha256-b>"] }
```

This is a local, possibly stale read. A client may safely retry uploads that a
lagging replica reports as missing.

## Submit a step

### `POST /v1/step?wait=false`

```json
{
  "step_id": "step-opaque-unique-id",
  "session": "session-42",
  "kind": "llm",
  "spec": { "application": "opaque" },
  "requirements": { "pool": "gpu", "zone": "east" }
}
```

`step_id`, `session`, and `spec` are required. `kind` is `llm` or `tool`.
`requirements`, when present, is a string-to-string map.

With `wait=false`, a successful submission returns `202`:

```json
{ "step_id": "step-opaque-unique-id" }
```

Without `wait=false`, the request long-polls until that step is `done` and
returns `200` with its accepted result:

```json
{ "result": { "application": "opaque" } }
```

There is no server-defined long-poll deadline. If the request context ends,
the server reports `504` when it can still send a response.

Step IDs are global deduplication keys. Reusing an ID does not create another
execution, and the server does not compare a duplicate request's session,
kind, spec, or requirements with the first request. Generate an opaque fresh
ID for every independent logical invocation. Retrying the same invocation
must reuse its original ID.

Validation failures return `400`; an unavailable leader or failed Raft apply
returns `503`. After an ambiguous timeout, resubmit the same step ID and then
read its session rather than inventing a replacement ID.

## Pull work

### `POST /v1/work/claim`

```json
{
  "worker_id": "worker-a-unique",
  "labels": { "pool": "gpu", "zone": "east", "extra": "allowed" }
}
```

Every required key/value on a step must exactly equal the worker label. Extra
worker labels are allowed. The control plane prefers the oldest eligible claim
already owned by this worker ID, then the oldest eligible pending step.

- `200`: claim won
- `204`: no eligible work, or another claim won the race
- `400`: missing worker ID or invalid JSON
- `503`: the write could not reach consensus

A successful response contains the opaque execution input and fence:

```json
{
  "step_id": "step-opaque-unique-id",
  "session": "session-42",
  "kind": "tool",
  "spec": { "application": "opaque" },
  "requirements": { "pool": "gpu" },
  "attempt": 0
}
```

If a response is lost, retrying with the same unique worker ID recovers its
existing claim and attempt. Never run concurrent worker processes with the
same ID.

### `POST /v1/work/renew`

```json
{
  "worker_id": "worker-a-unique",
  "step_id": "step-opaque-unique-id",
  "attempt": 0
}
```

- `200` with `{"renewed":true}`: the matching claim was renewed
- `409` with `{"renewed":false,"reason":"..."}`: unknown or fenced claim
- `400`: required identity is missing or JSON is invalid
- `503`: the write could not reach consensus

The reference worker renews every 10 seconds. A claim is released and its
attempt incremented after more than 30 leader-committed logical ticks without
a renewal.

### `POST /v1/work/commit`

```json
{
  "worker_id": "worker-a-unique",
  "step_id": "step-opaque-unique-id",
  "attempt": 0,
  "result": { "application": "opaque" },
  "next": {
    "step_id": "new-opaque-unique-id",
    "session": "session-42",
    "kind": "llm",
    "spec": { "application": "opaque" }
  }
}
```

`next` is optional. When present, all four of its fields are required and its
kind is `llm` or `tool`. It inherits the committed step's requirements; a
worker cannot change routing requirements mid-turn.

- `200` with `{"committed":true}`: result and optional successor accepted
- `409` with `{"committed":false,"reason":"..."}`: duplicate, unknown, fenced
  attempt, or an already-existing successor ID
- `400`: required data is missing, JSON is invalid, or `next` is invalid
- `503`: the write could not reach consensus

The result and a fresh-ID successor are one Raft transition: both become
visible or neither does. An existing successor ID rejects the whole commit and
leaves the current claim live. A worker must upload artifacts referenced by
either value before the commit. Because those values are opaque JSON, the
control plane does not validate embedded artifact references.

A timeout is ambiguous. Retry the exact commit, then inspect the session. A
duplicate returns `409` even when the first commit succeeded, while a fenced
response means another attempt may have taken ownership. Never interpret a
transport error as proof that the commit did not happen.

## Read a session

### `GET /v1/session/{session}/steps?since=N`

Returns locally visible steps for the session in global insertion order:

```json
{
  "steps": [
    {
      "index": 0,
      "step_id": "step-opaque-unique-id",
      "kind": "llm",
      "state": "done",
      "spec": { "application": "opaque" },
      "requirements": { "pool": "gpu" },
      "result": { "application": "opaque" }
    }
  ]
}
```

`state` is `pending`, `claimed`, or `done`. `since` is a zero-based session
cursor: the response starts at that array index. An omitted, invalid, or
non-positive value starts at zero. Unknown sessions return `{"steps":[]}`.

This endpoint does not perform a Raft barrier and can lag on a follower. Poll
until the desired state appears or query the current leader when freshness is
required.

## Cluster-internal routes

`GET /join` changes Raft membership and `POST /v1/internal/apply` accepts
forwarded Raft operations. They have no authentication in v0.1 and are not
application-client APIs. Network policy must restrict both routes together
with the public-looking API; path-based exposure is not a security boundary.

## Execution guarantee

The protocol is at least once. Consensus and attempt fencing choose at most one
accepted result for a current execution generation. They cannot make an
external tool effect exactly once: a worker may act, fail before commit, and be
replaced. Tools need their own idempotency keys, transactions, or compensation.
