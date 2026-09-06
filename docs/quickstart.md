# Quickstart

## Prerequisites

- Go 1.22.2 or newer
- Node.js 24 or newer
- Provider credentials or a Pi OAuth login for live LLM turns

Install the pinned TypeScript dependencies:

```bash
npm ci
```

## Run the token-free demonstration

```bash
./dev-cluster.sh
```

The script starts three Raft voters and three workers, each worker polling its
local control-plane node. It submits a scripted turn and exits the head before
execution finishes. The turn writes `proof.txt`, and the script reads the final
step list from node 3.

The cluster remains available so you can inspect it. Press Ctrl+C to stop its
processes. Logs and the proof file remain in the printed temporary directory.

## Run one node manually

Start the control plane:

```bash
go run . \
  --id node1 \
  --http 127.0.0.1:4700 \
  --raft 127.0.0.1:7000 \
  --artifact-store fs \
  --artifact-dir ./artifact-data
```

In another terminal, start a worker. It reuses the provider configuration and
OAuth credentials understood by Pi's model runtime.

```bash
BEIGNET_SIDECAR_URL=http://127.0.0.1:4700 \
BEIGNET_WORKER_ID=worker-local \
npm run worker
```

Open a turn from the directory the tools should operate in:

```bash
npm run head -- start \
  --session demo \
  --model anthropic/claude-haiku-4-5 \
  --cwd "$PWD" \
  "Inspect this directory and summarize the project."
```

The command prints the session and step IDs, then exits. Watch from a new
client:

```bash
npm run head -- watch --session demo --follow
```

## Route work by exact labels

Worker labels are a JSON string map:

```bash
BEIGNET_WORKER_LABELS='{"pool":"gpu","zone":"east"}' npm run worker
```

Add one or more requirements when starting a turn:

```bash
npm run head -- start \
  --session routed \
  --require pool=gpu \
  --require zone=east \
  "Run the task."
```

A worker may have extra labels. Every required key must exist with exactly the
same value. The accepted result inserts its successor with the same
requirements, so routing applies to the whole turn.

## Verify a sidecar implementation

Run a sidecar with no workers, then execute the pull-protocol probe:

```bash
npm run conformance -- http://127.0.0.1:4700
```

It checks artifacts, detached submission, eligibility, ambiguous claim
recovery, renewal, fencing, atomic successor insertion, deduplication, and
ordered session reads.
