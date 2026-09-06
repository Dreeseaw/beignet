# Beignet

Beignet keeps an agent turn running after the client that started it
disconnects. A small Go/Raft control plane records the authoritative execution
history, independent workers pull and execute LLM or tool steps, and large
immutable artifacts live outside consensus on a shared filesystem or in an
S3-compatible object store.

It is an experimental harness for making long-running agent failure semantics
explicit and testable, not a production agent platform or a general durable
workflow engine.

> [!WARNING]
> Beignet v0.1 has no authentication, authorization, or transport encryption.
> Bind it only to loopback or a trusted private network. Workers execute model-
> selected shell commands with their operating-system identity. Do not expose
> this release to the public internet or mutually untrusted users.

## The v0.1 contract

> Submit an agent turn, disconnect, and let an eligible worker finish it.
> Control state survives a Raft leader change. A failed worker's claim expires,
> another worker may retry it, and a stale attempt cannot replace the accepted
> result.

Execution is at least once, not exactly once. Attempt fencing chooses which
result enters the ledger; it cannot undo an email, Git push, API charge, or
filesystem mutation performed by an earlier attempt.

## Run the scripted cluster

You need Go 1.22.2 or newer and Node.js 24 or newer.

```bash
npm ci
./dev-cluster.sh
```

The script builds three local Raft voters, starts three pull workers, submits a
token-free scripted turn, detaches the head, and reads the completed ledger
from another node. Press Ctrl+C when finished; the printed temporary directory
contains the logs and proof file.

For manual setup, S3 configuration, multi-voter operation, and live model
turns, see the [quickstart](docs/quickstart.md) and
[operations guide](docs/operations.md).

## System model

```text
                         replicated control state
                    +-------------------------------+
                    |                               |
head  -------->  voter 1  <---- Raft ---->  voter 2  <---->  voter 3
  |                 ^                              ^             |
  | submit/watch    | claim / renew / commit      |             |
  +-- may exit      +---------- pull workers -----+-------------+

                    shared immutable artifact store
                    filesystem mount or S3-compatible bucket
```

- A **head** uploads initial context, submits a step, prints its identifiers,
  and may exit. Another head can inspect or continue the session.
- **Voters** replicate step identity, order, state, requirements, ownership,
  attempts, accepted results, artifact metadata, and logical time. They do not
  call models or run tools.
- **Workers** advertise exact labels, pull compatible work, renew claims, run
  opaque step specifications, upload artifacts, and offer a result with at
  most one successor.
- The **artifact store** holds bytes. Raft stores only each artifact's SHA-256
  and size, and every voter must use the same artifact namespace.

These roles can share one machine for development, but voters and workers are
not paired one-to-one.

## Why it is built this way

### Consensus records decisions, not execution

Raft orders the small facts that require agreement: which step exists, who
owns its current attempt, whether that attempt is live, and which result was
accepted. Model calls and tools run in pull workers because they may be slow,
expensive, hardware-specific, or unsafe; they never block the replicated state
machine.

Pulling also lets workers operate across NAT and advertise the model access,
toolchain, operating system, or workspace they provide. Exact-match labels
route a whole turn. The tradeoff is locality: Beignet does not copy a mutable
checkout to a replacement worker, so a shared workspace or routing requirement
must keep path-dependent work on a suitable host.

### Logical leases fence accepted results

The leader proposes logical ticks through Raft. A claim is released after more
than 30 committed ticks without renewal, and release increments its attempt. A
commit must match both the worker ID and current attempt, so a paused worker's
late result is rejected after a replacement takes over. Replicas make the same
lease decision without depending on synchronized host clocks.

Retrying a claim with the same unique worker ID recovers a committed claim
after an ambiguous response. Concurrent processes must therefore never share a
worker ID.

### A turn advances one link at a time

An agent turn is a linked sequence, not a general DAG. A worker offers the
current result and at most one successor in one Raft operation, so replicas
accept both or neither. An existing successor ID rejects the whole commit and
leaves the current claim live.

Step IDs name logical invocations, while artifact hashes name content. Each
independent model sample needs a fresh step ID even when its input is identical;
only retries of the same invocation reuse an ID.

### Bytes are published before references

Clients verify and store immutable bytes, commit their `{hash, size}` metadata,
and only then submit a step that references them. An interrupted publication
may leave an unreferenced object, but not a newly committed reference to bytes
that the operation failed to store. Garbage collection is deferred.

A multi-voter filesystem deployment needs a genuinely shared mount. The S3
backend verifies content on upload and download; SeaweedFS 4.41 is the pinned
self-hosted compatibility target.

### Writes follow the leader; reads may lag

Every voter serves HTTP. Followers forward writes once to the known leader and
return `503` when no leader is routable. `/readyz` means the node can route a
write; `/healthz` means only that its HTTP process is alive.

Session reads and artifact-metadata checks use the contacted replica without a
Raft barrier, so they may briefly lag a successful write. Callers that require
freshness must poll, try another ready node, or query the reported leader.

## Evidence

### Synthetic control-plane throughput

On 2026-09-05, three matched 100,000-turn runs used three voters on one WSL2
machine with 10 logical CPUs and 23 GiB of visible memory, one synthetic worker
process per voter, 512 logical worker slots per process, 64-item worker batches,
and 128-way batched submission. Every run retained the same leader and passed
an exact audit that independently derived all expected step IDs and result
tokens.

| Completed turns/s | Elapsed | Submit p99 | Missing / duplicate / bad results |
| ---: | ---: | ---: | ---: |
| 11,047.46 | 9.052 s | 236.40 ms | 0 / 0 / 0 |
| 13,078.50 | 7.646 s | 256.90 ms | 0 / 0 / 0 |
| 13,188.56 | 7.582 s | 163.14 ms | 0 / 0 / 0 |

The mechanism is a derived pending/owned index, stable hash-derived worker
starting positions, summary polling, and batches of up to 256 submits, claims,
renewals, or commits per Raft entry without removing per-item identity,
fencing, or verdicts. These are synthetic control-plane turns, not model calls,
and the result is specific to this machine and configuration. It neither
predicts LLM latency nor establishes the same throughput on small cloud VMs.
See the reproducible launcher and full methodology in the
[benchmark notes](benchmarks/README.md).

### Reliability and adverse results

- **Snapshot and restart:** an automatic snapshot was forced after more than
  8,400 committed ticks, and restart from compacted state preserved a
  pre-snapshot sentinel. Snapshots include steps, artifact metadata, node
  addresses, logical time, and insertion sequence. Focused tests also verify
  that encoding and sink-close failures cancel rather than publish an
  incomplete snapshot.
- **Worker loss:** a test stops worker A after its tool begins, expires attempt
  0, lets worker B claim attempt 1, and resumes A. A's late commit is fenced and
  B's result is accepted. The original tool may still have produced an external
  side effect.
- **Leader loss on independent hosts:** in a three-host Hetzner exercise, the
  leader VM was hard-powered off while another host ran a 46-second race test.
  A survivor became leader about 5.8 seconds after the power-off request; the
  in-flight tool committed and the session continued to an exact terminal
  result with 71 total steps. The two survivors returned byte-identical
  ledgers, and all 73 referenced artifacts verified against SHA-256 through
  both. The executing worker survived, all claims remained attempt 0, and the
  NFS host stayed up: this tested control-state failover, not simultaneous
  worker loss or artifact-store availability.
- **Wide worker exercise:** across 24 isolated Git workspaces and 24 Pi
  workers, 21 sessions were observed claimed concurrently. The successful path
  committed 1,104 LLM/tool steps; all three voters returned byte-identical
  ledgers, and 1,143 artifact hashes verified through every voter. Visible
  tests accepted all 24 patches, but an independent hidden oracle found two
  semantic misses: the quality result was 22/24 even though the distributed
  mechanics passed.
- **Follower staleness:** an immediate follower claim after a forwarded submit
  was stale in 50/50 probes. Retries made all 50 visible, with 28.075 ms minimum,
  79.593 ms median, 106.425 ms p95, and 110.439 ms maximum latency. This is
  observed behavior, not a general latency bound.

## Known v0.1 limits

- An unsupported model-selected tool can poison a durable step. In one run, the
  reference executor returned HTTP 500 and the step retried 857 times before
  workers were stopped. Unsupported tools need a committed non-retryable result
  or terminal executor outcome.
- A worker polls one configured control-plane URL and does not rotate to another
  ready voter when that endpoint dies.
- There is no whole-session step, token, time, or dollar budget.
- Workers are not sandboxes. In one read-only audit a model installed a missing
  compiler because the worker ran as root. Use unprivileged identities, explicit
  tool policy, isolated workspaces, and narrowly scoped credentials.
- Raft preserves the execution ledger, not a worker's mutable checkout or live
  subprocess. Those do not migrate after worker loss.
- Artifact availability is whatever the shared backend provides. Separate local
  directories are not replication, and a shared filesystem may be a single
  point of failure.
- v0.1 has one Raft group and linked-list turns. It has no partitioning, general
  DAGs, multi-tenancy, quotas, online backup, rolling-upgrade contract, voter
  replacement, or artifact garbage collection.

## Documentation and development gates

The [architecture](docs/DESIGN.md), [wire contract](docs/CONTRACT.md),
[v0.1 scope](docs/V0.1.md), [security limits](docs/security.md),
[operations guide](docs/operations.md), and
[benchmark methodology](benchmarks/README.md) carry the details omitted here.

```bash
go vet ./...
go test -race ./...
npm test
npm run docs:build
npm run conformance -- http://127.0.0.1:4700
```

The conformance probe runs against both the Go/Raft service and the in-memory
TypeScript sidecar. Public CI also checks the artifact-store contract against a
checksum-pinned SeaweedFS 4.41 release.

Beignet is licensed under the [Apache License 2.0](LICENSE).
