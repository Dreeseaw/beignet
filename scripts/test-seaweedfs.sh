#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
weed_bin="${1:-weed}"
seaweed_run_dir="$(mktemp -d)"
seaweed_s3_port="${BEIGNET_TEST_SEAWEED_S3_PORT:-18333}"
seaweed_pid=""

cleanup() {
  [[ -n "$seaweed_pid" ]] && kill "$seaweed_pid" 2>/dev/null || true
  [[ -n "$seaweed_pid" ]] && wait "$seaweed_pid" 2>/dev/null || true
  rm -rf -- "$seaweed_run_dir"
}
trap cleanup EXIT

cat > "$seaweed_run_dir/s3.json" <<'JSON'
{
  "identities": [{
    "name": "beignet-test",
    "credentials": [{
      "accessKey": "beignet-access-key",
      "secretKey": "beignet-secret-key"
    }],
    "actions": ["Admin", "Read", "List", "Tagging", "Write"]
  }]
}
JSON
mkdir -p "$seaweed_run_dir/data"

"$weed_bin" server -s3 \
  -dir="$seaweed_run_dir/data" \
  -ip=127.0.0.1 \
  -ip.bind=127.0.0.1 \
  -master.port=19333 \
  -volume.port=18080 \
  -filer.port=18888 \
  -s3.port="$seaweed_s3_port" \
  -s3.config="$seaweed_run_dir/s3.json" \
  -master.volumeSizeLimitMB=64 \
  -volume.max=5 \
  -volume.minFreeSpace=0 \
  >"$seaweed_run_dir/seaweed.log" 2>&1 &
seaweed_pid=$!

for _ in {1..120}; do
  if curl --silent --connect-timeout 1 --max-time 1 \
      --output /dev/null "http://127.0.0.1:$seaweed_s3_port/"; then
    break
  fi
  if ! kill -0 "$seaweed_pid" 2>/dev/null; then
    cat "$seaweed_run_dir/seaweed.log"
    exit 1
  fi
  sleep 0.25
done
if ! curl --silent --show-error --connect-timeout 1 \
    --output /dev/null "http://127.0.0.1:$seaweed_s3_port/"; then
  cat "$seaweed_run_dir/seaweed.log"
  exit 1
fi

cd "$repo_dir"
AWS_ACCESS_KEY_ID=beignet-access-key \
AWS_SECRET_ACCESS_KEY=beignet-secret-key \
AWS_REGION=us-east-1 \
AWS_EC2_METADATA_DISABLED=true \
BEIGNET_TEST_S3_BUCKET=beignet-contract \
BEIGNET_TEST_S3_PREFIX=ci/nested-prefix \
BEIGNET_TEST_S3_ENDPOINT="http://127.0.0.1:$seaweed_s3_port" \
BEIGNET_TEST_S3_PATH_STYLE=true \
BEIGNET_TEST_S3_CREATE_BUCKET=true \
go test -run '^TestS3ArtifactStoreContract$' -count=1 .
