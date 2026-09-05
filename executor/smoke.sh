#!/usr/bin/env bash
# Live smoke: real pi head -> fakecar -> executor -> real LLM + real bash.
# Burns a few tokens. Usage: ./smoke.sh [provider/model]
set -euo pipefail
cd "$(dirname "$0")"

export BEIGNET_SIDECAR_PORT=4700
export BEIGNET_SIDECAR_URL="http://127.0.0.1:$BEIGNET_SIDECAR_PORT"
export BEIGNET_MODEL="${1:-anthropic/claude-haiku-4-5}"

node fakecar.ts & CAR_PID=$!
node worker.ts & WORKER_PID=$!
trap 'kill $WORKER_PID $CAR_PID 2>/dev/null || true' EXIT
sleep 1

workdir="$(mktemp -d)"
cd "$workdir"
pi --mode json -p \
  -e ~/beignet/zombie/shim/beignet.ts \
  --no-builtin-tools --no-extensions \
  "Use bash to run: echo smoke-\$((6*7)). Then reply DONE." </dev/null \
  | grep -o 'smoke-42\|DONE' | sort -u

echo "smoke: OK (model: $BEIGNET_MODEL, workdir: $workdir)"
