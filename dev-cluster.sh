#!/usr/bin/env bash
# Boot a 3-node local cluster (sidecar + pull worker per node) and run one turn.
#
#   ./dev-cluster.sh              scripted LLM, no tokens
#   ./dev-cluster.sh --live       real model (anthropic/claude-haiku-4-5)
#
# Ports: node N -> sidecar 47N0, raft 700N
# Everything runs in a fresh temp dir; nothing touches ./raft-data.
set -euo pipefail
cd "$(dirname "$0")"
REPO="$PWD"

LIVE=""
[[ "${1:-}" == "--live" ]] && LIVE=1

RUN="$(mktemp -d)"
WORK="$RUN/work"
mkdir -p "$WORK"
echo "run dir: $RUN"
go build -o "$RUN/beignet" . || { echo "build failed"; exit 1; }

if [[ -z "$LIVE" ]]; then
  cat > "$RUN/script.json" <<SCRIPT
[{"role":"assistant","content":[{"type":"text","text":"Writing the file."},{"type":"toolCall","id":"c1","name":"bash","arguments":{"command":"echo trace-42 > proof.txt"}}],"api":"anthropic-messages","provider":"t","model":"s","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"toolUse","timestamp":1},
 {"role":"assistant","content":[{"type":"toolCall","id":"c2","name":"bash","arguments":{"command":"cat proof.txt"}}],"api":"anthropic-messages","provider":"t","model":"s","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"toolUse","timestamp":1},
 {"role":"assistant","content":[{"type":"text","text":"TRACE_COMPLETE"}],"api":"anthropic-messages","provider":"t","model":"s","usage":{"input":1,"output":1,"cacheRead":0,"cacheWrite":0,"totalTokens":2,"cost":{"input":0,"output":0,"cacheRead":0,"cacheWrite":0,"total":0}},"stopReason":"stop","timestamp":1}]
SCRIPT
fi

WORKER_ENV=()
[[ -z "$LIVE" ]] && WORKER_ENV=("BEIGNET_FAKE_LLM=$RUN/script.json")

PIDS=()
cleanup() { for p in "${PIDS[@]:-}"; do kill "$p" 2>/dev/null || true; done; }
trap cleanup EXIT

# --- sidecars (raft data isolated per node, under the run dir) ---
for i in 1 2 3; do
  mkdir -p "$RUN/node$i"
  JOIN=()
  [[ $i -ne 1 ]] && JOIN=(--join 127.0.0.1:4710)
  ( cd "$RUN/node$i" && exec "$RUN/beignet" \
      --id "node$i" \
      --http "127.0.0.1:47${i}0" \
      --raft "127.0.0.1:700${i}" \
      --artifact-dir "$RUN/artifacts" \
      "${JOIN[@]}" \
    ) > "$RUN/node$i.log" 2>&1 &
  PIDS+=($!)
  [[ $i -eq 1 ]] && sleep 3   # let node1 win its election before others join
done
sleep 4

# --- workers (one per node, each polling its OWN sidecar) ---
for i in 1 2 3; do
  env BEIGNET_SIDECAR_URL="http://127.0.0.1:47${i}0" \
      BEIGNET_WORKER_ID="node$i-worker" \
      BEIGNET_WORKER_LABELS="{\"pool\":\"default\",\"node\":\"node$i\"}" \
      "${WORKER_ENV[@]}" \
      node "$REPO/executor/worker.ts" > "$RUN/worker$i.log" 2>&1 &
  PIDS+=($!)
done
sleep 2

echo "=== cluster up (leader: node1) ==="
echo "--- submitting one turn, then the head exits ---"
( cd "$WORK" && BEIGNET_SIDECAR_URL=http://127.0.0.1:4710 \
    node "$REPO/head/head.ts" start --session trace1 --cwd "$WORK" --require pool=default \
    "Use bash to write trace-42 into proof.txt, then cat it. Reply TRACE_COMPLETE." )

echo
echo "--- who executed what (all nodes) ---"
sleep 8
for i in 1 2 3; do
  sed -n "s/.*] claimed/node$i  claimed/p" "$RUN/worker$i.log" || true
done | sort -k4

echo
echo "--- session as seen from node3 (a different node than the head used) ---"
curl -s "http://127.0.0.1:4730/v1/session/trace1/steps" \
  | node -e 'const d=JSON.parse(require("fs").readFileSync(0));for(const s of d.steps)console.log(`[${s.index}] ${s.kind.padEnd(4)} ${s.state.padEnd(7)} ${s.step_id.slice(0,12)}`)' || true

echo
echo "workdir: $WORK   (proof.txt should exist)"
ls -l "$WORK" || true
echo
echo "logs: $RUN/node{1,2,3}.log  $RUN/worker{1,2,3}.log"
echo "cluster still running — press Ctrl+C to stop."
wait
