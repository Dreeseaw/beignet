#!/usr/bin/env bash
# Live smoke: a real LLM drives a real turn through the cluster, unattended.
# Burns a few tokens. Usage: ./smoke.sh [provider/model]
set -euo pipefail
cd "$(dirname "$0")"

export BEIGNET_EXECUTOR_PORT=4701
export BEIGNET_SIDECAR_PORT=4700
export BEIGNET_EXECUTOR_URL="http://127.0.0.1:$BEIGNET_EXECUTOR_PORT"
export BEIGNET_SIDECAR_URL="http://127.0.0.1:$BEIGNET_SIDECAR_PORT"
MODEL="${1:-anthropic/claude-haiku-4-5}"

node executor/executor.ts & EXEC_PID=$!
node executor/fakecar.ts & CAR_PID=$!
trap 'kill $EXEC_PID $CAR_PID 2>/dev/null || true' EXIT
sleep 1

workdir="$(mktemp -d)"
session="smoke-$(date +%s)"

# Start the turn and immediately walk away.
node head/head.ts start --session "$session" --model "$MODEL" --cwd "$workdir" \
  "Use bash to write the text smoke-42 into a file called proof.txt in the current directory, then confirm it exists with cat. Reply DONE when finished."

echo "--- head exited; cluster owns the turn. watching from a NEW client ---"
sleep 2
node head/head.ts watch --session "$session" --follow

echo
echo "workdir: $workdir"
grep -q 'smoke-42' "$workdir/proof.txt" && echo "smoke: OK — file written by the cluster, no client attached"
