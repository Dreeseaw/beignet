# Architecture

Beignet v0.1 has one narrow job: keep an agent turn moving after the client
that submitted it disconnects. It separates replicated control state from
execution and from immutable artifact bytes.

## Components

### Head

The TypeScript head is a disposable client. It stages the initial context as
content-addressed artifacts, submits one `llm` step, prints the session and
step IDs, and exits. A later head can watch the session or append another turn.

### Control plane

One or more Go processes form a single HashiCorp Raft group. Every process
serves the HTTP API. The Raft finite-state machine owns:

- artifact metadata: SHA-256 and byte size, never artifact bytes;
- steps: identity, session, opaque spec, requirements, state, claim, attempt,
  insertion order, and accepted result;
- node IDs and HTTP addresses used to forward writes to the leader;
- logical lease time and the global insertion sequence.

A control-plane node is a Raft voter. It does not execute model or tool work.

### Worker

The TypeScript worker is a client of the control plane, not a Raft member. It
polls any routable control-plane node, claims one compatible step, renews the
claim while executing, and offers a result and at most one successor.

Voters and workers can be scaled and placed independently. The local cluster
script starts one worker beside each voter only to make the demonstration easy
to inspect.

### Artifact store

Immutable bytes live behind one `ArtifactStore` interface:

- `fs` writes content-addressed files. A multi-node cluster needs one genuinely
  shared mount visible to every control-plane node.
- `s3` writes content-addressed objects to a shared S3-compatible bucket.
  SeaweedFS is the tested self-hosted target for v0.1.

All participants must use the same artifact namespace. A separate local
artifact directory per voter is not a replicated store.

## Turn sequence

1. The head hashes and uploads context artifacts.
2. The head submits a unique logical step with `wait=false` and may exit after
   the control plane accepts it.
3. A worker sends its ID and labels to `/v1/work/claim`.
4. The control plane chooses the oldest locally visible eligible step and
   commits its owner and attempt through Raft.
5. The worker executes the opaque step spec and renews the same fenced attempt
   while it runs.
6. The worker uploads any artifacts needed by the result or successor.
7. The worker commits the result and optional fresh-ID successor. Raft accepts
   them in one state-machine operation or rejects both.
8. A watcher reads the session as an insertion-ordered list of steps.

An agent turn is a linked list, not a general workflow graph. An LLM result may
create the first tool step, tool calls run as consecutive steps, and the last
tool result may create the next LLM step. Each accepted commit creates at most
one successor.

## Replicated state machine

A step has three states:

```text
Pending --claim--> Claimed --commit--> Done
   ^                   |
   +---- lease expiry--+
```

Claiming is first-writer-wins through Raft. A repeated claim from the same
worker ID returns that worker's existing attempt; this makes a lost claim
response recoverable. Worker IDs therefore must be unique among concurrently
executing worker processes.

Lease time is logical, not wall-clock time. The leader proposes one tick per
second. A claim is released after more than 30 committed ticks without a
successful renewal, and releasing it increments the attempt. The reference
worker renews every 10 seconds. Pausing a leader pauses logical lease progress;
electing a new leader resumes it without relying on synchronized host clocks.

A commit must match both the current worker ID and attempt. A late result from
an expired attempt is rejected even if that execution eventually finishes.
This fences the accepted ledger result; it cannot undo external effects the
old execution already caused.

Step IDs name logical invocations rather than input content. The first insert
of an ID wins and later submissions with that ID are deduplicated. Clients must
use a fresh ID for every independent sample, even when the specs are identical.

## Artifact publication

The reference head and worker follow this order:

1. validate that the path is the lowercase SHA-256 of the bytes;
2. store the immutable bytes;
3. commit the hash and size through Raft;
4. only then submit or commit the step value that uses the hash.

A failure between steps 2 and 3 can leave an unreferenced object. That is safe
and can be handled by future garbage collection. The control plane deliberately
treats specs and results as opaque JSON, so it does not discover or validate
hashes embedded in them. Custom protocol clients are responsible for the same
store-before-reference order.

## Consistency and routing

All public writes are applied by the Raft leader. A follower discovers the
leader through Raft and uses the replicated node-address map to forward the
operation once. If there is no leader or no HTTP address for it, the write
fails with `503` instead of forwarding in a loop.

Health and session/artifact-metadata reads are served from the contacted node's
local state without a Raft barrier. A follower can therefore return a
temporarily stale session view. Clients that need the newest view should poll,
retry another ready node, or target the current leader reported by
`/v1/status`.

Artifact bytes are read from the configured shared store only after the local
replica knows their metadata. Replicating metadata does not replicate a local
filesystem.

## Persistence and recovery

Raft logs, stable state, and snapshots live under `raft-data/<node-id>` in each
node's working directory. Snapshot format version 2 contains artifact metadata,
steps, node addresses, logical time, and insertion sequence. It contains no
artifact bytes.

The format rejects other snapshot versions instead of guessing at a migration.
Data created before snapshot version 2 must be recreated for v0.1. See
[Operations](operations.md) for backup and restart limits.

## Deliberate limits

v0.1 has one Raft group, one linked-list turn shape, exact-match labels, and an
at-least-once execution guarantee. It has no authentication, TLS termination,
multi-tenancy, quota enforcement, artifact garbage collection, partitioning,
or exactly-once external tool effects. See [Security](security.md) before
running anything beyond a local machine.
