#!/usr/bin/env bash
# Reproduce the three-voter synthetic throughput benchmark on one machine.
set -euo pipefail

repo_dir="$(cd "$(dirname "$0")/.." && pwd)"
run_id="${1:-local3-$(date -u +%Y%m%dT%H%M%SZ)}"
output_dir="${2:-$repo_dir/artifacts/benchmarks/$run_id}"
turns="${BEIGNET_BENCH_TURNS:-100000}"
worker_concurrency="${BEIGNET_BENCH_WORKERS_PER_NODE:-512}"
worker_batch_size="${BEIGNET_BENCH_WORKER_BATCH_SIZE:-64}"
submit_concurrency="${BEIGNET_BENCH_SUBMIT_CONCURRENCY:-128}"
submit_batch_size="${BEIGNET_BENCH_SUBMIT_BATCH_SIZE:-128}"

for command in curl go gzip jq; do
	command -v "$command" >/dev/null || { printf 'missing command: %s\n' "$command" >&2; exit 1; }
done
if [[ -e "$output_dir" ]]; then
	printf 'output path already exists: %s\n' "$output_dir" >&2
	exit 1
fi
mkdir -p "$output_dir"
run_dir="$(mktemp -d /tmp/beignet-local-bench.XXXXXX)"
control_bin="$run_dir/beignet"
bench_bin="$run_dir/beignet-bench"
control_pids=()
worker_pids=()

stop_processes() {
	local signal=$1
	shift
	for process_id in "$@"; do
		kill "-$signal" "$process_id" 2>/dev/null || true
	done
	for process_id in "$@"; do
		wait "$process_id" 2>/dev/null || true
	done
}

cleanup() {
	local exit_status=$?
	trap - EXIT
	stop_processes TERM "${worker_pids[@]-}"
	stop_processes TERM "${control_pids[@]-}"
	for node_number in 1 2 3; do
		if [[ -f "$run_dir/node$node_number.log" ]]; then
			gzip -c "$run_dir/node$node_number.log" > "$output_dir/node$node_number.log.gz"
		fi
	done
	case "$run_dir" in
		/tmp/beignet-local-bench.*) find "$run_dir" -depth -delete ;;
	esac
	exit "$exit_status"
}
trap cleanup EXIT

(
	cd "$repo_dir"
	go build -trimpath -o "$control_bin" .
	go build -trimpath -o "$bench_bin" ./cmd/beignet-bench
)
mkdir -p "$run_dir/shared-artifacts"
for node_number in 1 2 3; do
	mkdir -p "$run_dir/node$node_number"
done

wait_ready() {
	local url=$1
	for _ in $(seq 1 120); do
		if curl -fsS "$url/readyz" >/dev/null 2>&1; then
			return 0
		fi
		sleep 0.25
	done
	printf 'node did not become ready: %s\n' "$url" >&2
	return 1
}

(
	cd "$run_dir/node1"
	exec "$control_bin" --id node1 --http 127.0.0.1:24710 --raft 127.0.0.1:27001 \
		--artifact-dir "$run_dir/shared-artifacts"
) >"$run_dir/node1.log" 2>&1 &
control_pids+=($!)
wait_ready http://127.0.0.1:24710

for node_number in 2 3; do
	http_port=$((24700 + node_number * 10))
	raft_port=$((27000 + node_number))
	(
		cd "$run_dir/node$node_number"
		exec "$control_bin" --id "node$node_number" --http "127.0.0.1:$http_port" \
			--raft "127.0.0.1:$raft_port" --join 127.0.0.1:24710 \
			--artifact-dir "$run_dir/shared-artifacts"
	) >"$run_dir/node$node_number.log" 2>&1 &
	control_pids+=($!)
done
wait_ready http://127.0.0.1:24720
wait_ready http://127.0.0.1:24730

write_statuses() {
	local destination=$1
	for port in 24710 24720 24730; do
		curl -fsS "http://127.0.0.1:$port/v1/status"
	done | jq -s . > "$destination"
	jq -e '(map(select(.state == "Leader")) | length) == 1 and
		([.[].leader_id] | unique | length) == 1' "$destination" >/dev/null
}

write_statuses "$output_dir/statuses-start.json"
start_leader="$(jq -r 'map(select(.state == "Leader"))[0].node_id' "$output_dir/statuses-start.json")"

for node_number in 1 2 3; do
	http_port=$((24700 + node_number * 10))
	"$bench_bin" worker --targets "http://127.0.0.1:$http_port" --run "$run_id" \
		--worker-prefix "node$node_number" --concurrency "$worker_concurrency" \
		--batch-size "$worker_batch_size" --duration 10m \
		>"$output_dir/worker$node_number.json" 2>"$output_dir/worker$node_number.stderr" &
	worker_pids+=($!)
done
sleep 1

set +e
"$bench_bin" run --targets http://127.0.0.1:24710,http://127.0.0.1:24720,http://127.0.0.1:24730 \
	--run "$run_id" --turns "$turns" --submit-concurrency "$submit_concurrency" \
	--submit-batch-size "$submit_batch_size" --workers 0 --audit-interval 25ms --timeout 10m \
	>"$output_dir/result.json" 2>"$output_dir/result.stderr"
benchmark_status=$?
set -e

write_statuses "$output_dir/statuses-end.json"
end_leader="$(jq -r 'map(select(.state == "Leader"))[0].node_id' "$output_dir/statuses-end.json")"
stop_processes TERM "${worker_pids[@]}"
worker_pids=()

if [[ "$benchmark_status" -ne 0 ]]; then
	printf 'benchmark exited %d; see %s\n' "$benchmark_status" "$output_dir" >&2
	exit "$benchmark_status"
fi
jq -e --argjson turns "$turns" '.valid == true and .audit.expected == $turns and
	.audit.done == $turns and .audit.missing == 0 and .audit.duplicate == 0 and
	.audit.unexpected == 0 and .audit.bad_spec == 0 and .audit.bad_result == 0' \
	"$output_dir/result.json" >/dev/null
if [[ "$start_leader" != "$end_leader" ]]; then
	printf 'leadership changed from %s to %s; run is fault evidence, not a throughput result\n' \
		"$start_leader" "$end_leader" >&2
	exit 2
fi

{
	uname -a
	go version
	lscpu | sed -n '1,22p'
} > "$output_dir/environment.txt"
jq '{valid, completion_rate_per_second, elapsed_ms, submissions, audit}' "$output_dir/result.json"
printf 'stable leader: %s\nartifacts: %s\n' "$start_leader" "$output_dir"
