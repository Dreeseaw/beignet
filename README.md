# Beignet

Beignet is an experimental runtime for agent turns that should outlive the
client that started them.

Submit a turn, disconnect, and let an eligible worker finish it. A small
Go/Raft control plane preserves step state across a leader change; independent
TypeScript workers pull and execute LLM or tool steps; immutable artifacts live
on a filesystem or in an S3-compatible object store.

> [!WARNING]
> Beignet v0.1 has no authentication, authorization, or transport encryption.
> Bind it only to loopback or a trusted private network. It is not safe on the
> public internet or as a multi-tenant service.

## Try the scripted cluster

You need Go 1.22.2 or newer and Node.js 24 or newer.

```bash
npm ci
./dev-cluster.sh
```

That command builds three local Raft voters, starts three pull workers, submits
a token-free scripted turn, detaches the head, and reads the completed ledger
from a different node. Press Ctrl+C when you are finished; the printed run
directory contains the logs and proof file.

## What v0.1 guarantees

- Control state and accepted results are replicated through Raft.
- A disconnected head is not load-bearing for turn completion.
- Work is at least once, with attempt fencing so only a current claim can
  commit a result and its successor.
- Worker requirements use exact key/value matching and follow a whole turn.
- Artifact bytes stay outside Raft and can use a shared filesystem or an
  S3-compatible service such as SeaweedFS.

It does not provide exactly-once external side effects, public-network
security, artifact garbage collection, or general workflow DAGs.

Read the [quickstart](docs/quickstart.md), [architecture](docs/DESIGN.md),
[wire contract](docs/CONTRACT.md), and [security limits](docs/security.md).
The full documentation site runs with `npm run docs:dev` and builds with
`npm run docs:build`.

## Development gates

```bash
go test -race ./...
npm test
npm run docs:build
npm run conformance -- http://127.0.0.1:4700
```

The SeaweedFS artifact contract is automated by
`scripts/test-seaweedfs.sh /path/to/weed` and runs in public CI against a
checksum-pinned SeaweedFS release.
