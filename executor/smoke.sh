#!/usr/bin/env bash
# Live smoke: real pi head -> fakecar -> executor -> real LLM + real bash.
# Burns a few tokens. Usage: ./smoke.sh [provider/model]
set -euo pipefail
cd "$(dirname "$0")"

export BEIGNET_EXECUTOR_PORT=4701
export BEIGNET_SIDECAR_PORT=4700
export BEIGNET_EXECUTOR_URL="http://127.0.0.1:$BEIGNET_EXECUTOR_PORT"
export BEIGNET_SIDECAR_URL="http://127.0.0.1:$BEIGNET_SIDECAR_PORT"
export BEIGNET_MODEL="${1:-anthropic/claude-haiku-4-5}"

node executor.ts & EXEC_PID=$!
node fakecar.ts & CAR_PID=$!
trap 'kill $EXEC_PID $CAR_PID 2>/dev/null || true' EXIT
sleep 1

workdir="$(mktemp -d)"
cd "$workdir"
pi --mode json -p \
  -e ~/beignet/zombie/shim/beignet.ts \
  --no-builtin-tools --no-extensions \
  "Use bash to run: echo smoke-\$((6*7)). Then reply DONE." </dev/null \
  | grep -o 'smoke-42\|DONE' | sort -u

echo "smoke: OK (model: $BEIGNET_MODEL, workdir: $workdir)"
