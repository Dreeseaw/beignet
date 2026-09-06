# Beignet

Beignet is a distributed system for one object: an agent turn that should
outlive the client that started it.

A head submits a turn and can disconnect. Independent workers pull its LLM and
tool steps, while a small Go/Raft control plane preserves the authoritative
execution history through worker loss and leader changes. Large immutable
artifacts stay outside consensus on a shared filesystem or in an S3-compatible
object store.

Beignet is an experimental harness, not a general durable-object database or a
production agent platform. Its purpose is to make the failure semantics of a
long-running agent turn explicit, inspectable, and testable.

> [!WARNING]
> Beignet v0.1 has no authentication, authorization, or transport encryption.
> Bind it only to loopback or a trusted private network. Workers execute model-
> selected shell commands with their operating-system identity. Do not expose
> this release to the public internet or mutually untrusted users.

## The v0.1 claim

> Submit an agent turn, disconnect, and let an eligible worker finish it.
> Control state survives a Raft leader change. A failed worker's claim expires,
> another worker may retry it, and a stale attempt cannot replace the accepted
> result.

That is deliberately narrower than "exactly-once agents." Execution is at
least once. Fencing decides which result enters the ledger; it cannot undo an
email, Git push, API charge, or filesystem mutation performed by an earlier
attempt.

## Try the scripted cluster

You need Go 1.22.2 or newer and Node.js 24 or newer.

```bash
npm ci
./dev-cluster.sh
```

The script builds three local Raft voters, starts three pull workers, submits a
token-free scripted turn, detaches the head, and reads the completed ledger
from a different node. Press Ctrl+C when finished; the printed temporary
directory contains the logs and proof file.

For manual setup, S3 configuration, multi-voter operation, and live model
turns, read the [quickstart](docs/quickstart.md) and
[operations guide](docs/operations.md).

## System shape

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

The roles are separate on purpose:

- **Heads are disposable clients.** They upload initial context, submit a step,
  print its identifiers, and may exit. Another head can inspect or continue the
  session later.
- **Voters replicate decisions.** They store step identity, order, state,
  requirements, ownership, attempts, accepted results, artifact metadata, and
  logical time. They do not call models or run tools.
- **Workers execute.** A worker advertises exact labels, pulls compatible work,
  renews its claim, runs the opaque LLM or tool specification, uploads produced
  artifacts, and offers a result plus at most one successor.
- **The artifact store holds bytes.** Raft stores only each artifact's SHA-256
  and size. Every voter must use the same artifact namespace.

One machine can run every role for development. A real deployment can place
voters and workers independently; they are not paired one-to-one.

## Distributed design decisions

### Replicate the ledger, not the execution

The control plane needs agreement on a small set of facts: which logical step
exists, who owns its current attempt, whether that attempt is still live, and
which result was accepted. HashiCorp Raft serializes those decisions and makes
them recoverable from a quorum.

Model calls and tools deliberately run outside the Raft processes. They can be
slow, expensive, hardware-specific, or unsafe. Keeping them in independent
pull workers prevents a long model response or subprocess from blocking the
replicated state machine and keeps the voters focused on consensus.

### Pull work instead of pushing it

A worker initiates every control-plane connection. This works across NAT and
private networks without teaching voters how to reach every executor. Exact
string labels route a whole turn to workers with the required model access,
toolchain, operating system, or workspace.

The tradeoff is explicit: a path in a step specification is meaningful only on
the worker that executes it. Beignet v0.1 does not copy a mutable checkout to a
replacement worker. A shared workspace or a routing requirement must preserve
that locality.

### Use logical leases and attempt fencing

Claims expire according to leader-committed ticks, not host wall clocks. The
leader proposes one tick per second; a claim is released after more than 30
committed ticks without renewal. Every release increments the step's attempt.

A commit must match both the worker ID and the current attempt. If worker A is
paused, loses its lease, and later returns after worker B has taken attempt 1,
A's attempt-0 result is rejected. Because every replica applies the same tick
entries, clock skew between machines cannot produce different lease decisions.

Retrying a claim with the same unique worker ID recovers an already-committed
claim after an ambiguous response. Running two concurrent processes under the
same worker ID would defeat that recovery rule, so worker IDs must be unique.

### Commit the result and continuation together

An agent turn is a linked sequence of LLM and tool steps, not a general DAG.
The current worker offers the result and at most one next step in a single Raft
operation. Replicas therefore cannot observe a completed step without its
accepted continuation.

The successor ID is validated before the current result is stored. If that ID
already exists, the entire commit is rejected and the current claim remains
live. This ordering matters: accepting the result first would silently sever a
turn when a caller accidentally reused an ID.

### Keep execution identity separate from content identity

Artifact IDs are content hashes; step IDs are logical invocation IDs. Early
versions derived step IDs from input content, which caused two independent
stochastic model samples with identical inputs to collapse into one execution.
The client now creates a fresh step ID for each invocation and reuses it only
when retrying that same invocation.

### Store bytes before publishing references

Large context and result bytes do not belong in Raft entries or snapshots.
Clients first verify and store immutable bytes, then commit `{hash, size}`
metadata, and only then submit a step that references the hash.

This ordering permits an unreferenced object after an interrupted publication,
but it prevents a newly committed reference from naming bytes that this
operation failed to store. Garbage collection is deferred; correctness of the
live ledger is not.

The filesystem backend atomically publishes a verified temporary file by
rename. In a multi-voter cluster it requires a genuinely shared mount. The S3
backend verifies bytes on both upload and download and distinguishes a missing
object from a missing bucket or backend failure. SeaweedFS 4.41 is the pinned
self-hosted compatibility target.

### Forward writes and admit stale reads

Every voter serves HTTP. A write received by a follower is forwarded once to
the known leader; a node without a routable leader returns `503`. `/readyz`
means that the contacted node can currently route a write, whereas `/healthz`
only means that its HTTP process is alive.

Session reads and artifact-metadata checks use the contacted replica without a
Raft barrier. They can briefly lag a successful forwarded write. This keeps
polling cheap and makes the consistency cost visible to callers instead of
hiding a consensus round trip in every read.

### Snapshot the complete state-machine contract

Raft log compaction is safe only if a snapshot contains everything needed to
reconstruct the same machine: steps, artifact metadata, node addresses,
logical time, and insertion sequence. Snapshot capture is synchronized with
applies and copies mutable values so later changes cannot leak into the image.
Persistence cancels a failed snapshot sink, and restore rejects unknown format
versions rather than guessing at a migration.

## Performance work

Performance changes followed an exact synthetic benchmark rather than an HTTP
request counter. The benchmark independently derives every expected step ID
and result token, and fails on missing, duplicate, unexpected, malformed, or
incorrectly committed results. It also records retries, fences, worker errors,
leadership at the start and end, request latency, and complete run logs.

The first implementation made four expensive choices:

1. claims scanned the complete step ledger, including completed work;
2. every worker began at the oldest pending step, creating a claim herd;
3. every submit, claim, renewal, and commit occupied its own Raft entry; and
4. completion polling repeatedly serialized the complete growing session.

The current path instead:

- maintains pending and owned indexes as part of the replicated FSM invariant;
- gives workers a stable hash-derived starting position in pending work, which
  spreads claimers while retaining deterministic selection;
- batches up to 256 submits, claims, renewals, or commits into one Raft entry,
  while preserving a separate identity, attempt fence, and verdict per item;
- polls compact session counts and fetches the full ledger once for the final
  exact audit; and
- reuses one stopped ticker per synchronous waiter instead of allocating a new
  timer every 25 milliseconds.

### Measured local control-plane result

Three matched 100,000-turn runs used three voters on one WSL2 machine, one
synthetic worker process per voter, 512 logical worker slots per process,
64-item worker batches, and 128-way batched submission. All three retained the
same leader and passed the exact audit:

| Completed turns/s | Elapsed | Submit p99 | Missing / duplicate / bad results |
| ---: | ---: | ---: | ---: |
| 11,047.46 | 9.052 s | 236.40 ms | 0 / 0 / 0 |
| 13,078.50 | 7.646 s | 256.90 ms | 0 / 0 / 0 |
| 13,188.56 | 7.582 s | 163.14 ms | 0 / 0 / 0 |

These are synthetic control-plane turns, not model calls, and they are specific
to that machine and configuration. They support a local scheduling-throughput
claim; they do not predict LLM latency or prove the same rate on small cloud
VMs. The reproducible launcher and fuller methodology are in
[`benchmarks/README.md`](benchmarks/README.md).

Earlier development observations used different workloads and concurrency, so
they are diagnostic rather than a valid before/after series. They nevertheless
found the important bottlenecks: the oldest-step path completed 1,000 turns at
6.54 turns/s; indexing and claim spreading reached 2,078 turns/s on 10,000
turns; and a 100,000-turn run that repeatedly fetched the full session caused
leader elections and achieved only 1,860 turns/s. The final summary-polling
design was adopted only after matched 100,000-turn runs stayed exact and kept a
stable leader.

## Reliability evidence

The tests start from a precise claim and inject a fault that could falsify it.
A clean run without the intended fault is not counted as failover evidence.

### Snapshot and restart

An automatic snapshot was forced after more than 8,400 committed ticks, the
process was restarted from compacted state, and a sentinel written before the
snapshot remained readable. Focused failures also verify that an encoding or
sink-close error cancels the snapshot instead of publishing it as complete.

### Worker loss and fencing

The end-to-end worker test stops worker A after its tool begins, advances
logical time until attempt 0 expires, lets worker B claim attempt 1, and then
resumes A. A's explicit late commit is rejected as fenced; B's result is the
one accepted into the ledger.

This verifies accepted-result fencing. The first tool may still have produced
an external side effect before it was stopped.

### Independent-host leader loss

A three-host Hetzner exercise ran one voter and one worker on each VM. During a
real coding turn, the current leader VM was hard-powered off while a different
host was running a 46-second race test. A survivor became leader about 5.8
seconds after the power-off request. The in-flight tool later committed, and
the session continued to an exact terminal result with 71 total steps.

The two survivors returned byte-identical final ledgers. Every one of the 73
referenced artifacts was fetched through both survivor endpoints and verified
against its SHA-256. All claims remained attempt 0 because the executing worker
survived; this was a leader-loss test, not a simultaneous worker-loss test.

The run also exposed an honest limitation: the shared NFS service lived on the
third VM and was not killed. The result proves control-state failover, not
artifact-store high availability.

### Wide pull-worker exercise

A separate three-host exercise created 24 isolated Git workspaces and 24 Pi
workers. Twenty-one sessions were observed claimed concurrently. The
successful path committed 1,104 LLM/tool steps; all three voters returned
byte-identical final ledgers, and all 1,143 referenced artifact hashes were
retrieved and verified through every voter.

The distributed mechanics passed, but model quality was not confused with
control-plane correctness. Visible tests accepted all 24 generated patches; an
independent hidden oracle found two semantic misses, for an honest result of
22/24. Provider and tool failures from the same run remain important v0.1
findings rather than being excluded from the story.

### Observable follower staleness

In the three-host environment, an immediate claim on a follower after a
forwarded submission was stale in 50/50 probes. Retrying made all 50 visible:
28.075 ms minimum, 79.593 ms median, 106.425 ms p95, and 110.439 ms maximum.
The reference worker's 250 ms polling cadence tolerated this, but the result is
why the public contract does not promise read-your-writes from an arbitrary
follower.

## Known v0.1 failures and boundaries

- **Unknown tools can poison a step.** One model emitted an unsupported tool
  name. The reference executor returned HTTP 500, so the durable step retried
  857 times until the workers were stopped. Unsupported tools need a committed
  non-retryable result or terminal executor outcome.
- **A worker has one control-plane URL.** It does not rotate through multiple
  ready voters when its configured endpoint dies.
- **There is no whole-session budget.** Step, token, time, and dollar ceilings
  must currently be enforced by an external operator.
- **Workers are not sandboxes.** A model asked to perform a read-only audit once
  installed a missing compiler because its worker ran as root. Production
  workers need an unprivileged identity, an explicit tool policy, and isolated
  workspaces.
- **Mutable workspaces do not migrate.** Raft preserves the execution ledger,
  not an arbitrary checkout or subprocess on a worker host.
- **Artifact availability comes from the backend.** Three local directories
  are not replication. A shared filesystem can be a single point of failure;
  an S3-compatible service brings its own durability and compatibility limits.
- **Exactly-once external effects are not provided.** Attempt fencing protects
  the accepted result, not the world outside Beignet.
- **Follower reads can lag.** Poll, retry another ready node, or query the
  reported leader when freshness matters.
- **Operations are intentionally incomplete.** v0.1 has no supported rolling
  upgrade, voter replacement, online backup, artifact garbage collection,
  partitioning, quotas, multi-tenancy, or public-network security.

## Contract and development gates

The detailed [architecture](docs/DESIGN.md),
[wire contract](docs/CONTRACT.md), [v0.1 scope](docs/V0.1.md), and
[security limits](docs/security.md) are part of the release contract.

```bash
go vet ./...
go test -race ./...
npm test
npm run docs:build
npm run conformance -- http://127.0.0.1:4700
```

The same conformance probe runs against the Go/Raft service and the in-memory
TypeScript sidecar. Public CI also verifies the artifact-store contract against
a checksum-pinned SeaweedFS 4.41 release.

Beignet is licensed under the [Apache License 2.0](LICENSE).
