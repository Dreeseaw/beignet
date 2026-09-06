# Synthetic throughput benchmark

`beignet-bench` measures the control plane with deterministic one-step tool
turns. It can submit work, run synthetic executors in-process or as a separate
fleet, and perform an exact final audit. The audit independently derives every
expected step ID and token and rejects missing, duplicate, unexpected, malformed,
or incorrectly committed results.

This measures durable scheduling throughput, not model inference or tool runtime.

## Reproduce locally

The promotion-sized local run uses three real Raft voters, one separately
targeted synthetic worker process per voter, 512 worker slots per process,
64-slot claim/commit batches, and 100,000 turns:

```bash
./scripts/benchmark-local.sh
```

The script refuses to overwrite an output directory, records start/end leader
views and worker summaries, requires an unchanged leader, and retains compressed
Raft logs plus the exact audit JSON under `artifacts/benchmarks/`. Those artifacts
are intentionally ignored by Git because each run is machine-specific.

The workload size and concurrency can be changed with `BEIGNET_BENCH_TURNS`,
`BEIGNET_BENCH_WORKERS_PER_NODE`, `BEIGNET_BENCH_WORKER_BATCH_SIZE`,
`BEIGNET_BENCH_SUBMIT_CONCURRENCY`, and `BEIGNET_BENCH_SUBMIT_BATCH_SIZE`.

## Local evidence

On 2026-09-05, three same-configuration runs completed on WSL2 with 10 logical
CPUs from an AMD Ryzen 9 7900X and 23 GiB of visible memory:

| Turns | Completed turns/s | Elapsed | Submit p99 | Audit | Leader |
| ---: | ---: | ---: | ---: | --- | --- |
| 100,000 | 11,047.46 | 9.052 s | 236.40 ms | exact, clean | stable |
| 100,000 | 13,078.50 | 7.646 s | 256.90 ms | exact, clean | stable |
| 100,000 | 13,188.56 | 7.582 s | 163.14 ms | exact, clean | stable |

All three runs had zero submission retries, missing steps, duplicates, unexpected
steps, bad specs, bad results, fenced commits, or worker failures. The result
supports a local control-plane claim above 10,000 completed synthetic turns/s.
It does not establish the three-small-cloud-node claim; that remains gated on a
successful run of the AWS lab.

The third row used the script added here against control and benchmark code
identical to commit `fecd7e5`; its start/end status records both identify
`node1` as leader.

The mechanism is explicit batching, not relaxed durability. Up to 256 submits,
claims, renewals, or commits share one Raft log entry, while each item retains
its own owner/attempt fence and verdict. A pending/owned work index and stable
worker-dependent start slot avoid the former all-workers/oldest-step claim herd.
Completion polling reads only state counts and fetches the full session once for
the exact audit, preventing large repeated JSON serialization from starving
Raft heartbeats.

Earlier results used different workload sizes or concurrency and are therefore
diagnostic context, not matched headline comparisons. The original oldest-step
path completed 1,000 turns at 6.54 turns/s with 96 worker slots. Spreading and
indexing reached 2,078 turns/s on a 10,000-turn, 192-slot run. An intermediate
100,000-turn run that repeatedly serialized the full session triggered leader
elections and achieved only 1,860 turns/s; it is fault evidence, not a valid
throughput replication.
