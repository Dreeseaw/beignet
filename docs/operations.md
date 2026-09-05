# Operations

Beignet v0.1 is an experimental developer preview, not a production service.
It has no online migration, membership-management CLI, rolling-upgrade
contract, or built-in backup command.

## Process layout

Run an odd number of Go control-plane voters, normally three, and any number of
independent TypeScript workers. A worker can poll any node whose `/readyz`
returns `200`; it does not need to run on the same host as a voter.

Keep the HTTP and Raft listeners on loopback or a trusted private network. Both
ports are unauthenticated and unencrypted.

## Artifact backends

### Filesystem

```bash
go run . \
  --id node1 \
  --http 127.0.0.1:4700 \
  --raft 127.0.0.1:7000 \
  --artifact-store fs \
  --artifact-dir /srv/beignet/artifacts
```

For one node, the directory can be local. For multiple nodes, that path must
name the same durable shared filesystem namespace from every node. Raft
replicates only artifact metadata; it does not copy files between local disks.

### S3-compatible store

```bash
AWS_REGION=us-east-1 \
AWS_ACCESS_KEY_ID=... \
AWS_SECRET_ACCESS_KEY=... \
go run . \
  --id node1 \
  --http 127.0.0.1:4700 \
  --raft 127.0.0.1:7000 \
  --artifact-store s3 \
  --s3-bucket beignet-artifacts \
  --s3-prefix cluster-a \
  --s3-endpoint https://s3.internal.example \
  --s3-path-style
```

The S3 implementation uses the AWS SDK's default credential chain. Region and
endpoint overrides are optional; path-style addressing is useful for some
self-hosted services. Create the bucket before starting Beignet and configure
every voter with the same bucket and prefix.

Use HTTPS with certificate verification and short-lived credentials outside
an isolated local test. `scripts/test-seaweedfs.sh` launches a disposable,
loopback-only SeaweedFS instance with fixed test credentials. It is a contract
test, not a production deployment guide.

## Form a three-voter cluster

Start the first voter without `--join`, wait for `/readyz`, then start two
voters with the first leader's HTTP address:

```bash
# host or process 1
beignet --id node1 --http 10.0.0.11:4700 --raft 10.0.0.11:7000 \
  --artifact-store s3 --s3-bucket beignet-artifacts --s3-prefix cluster-a

# host or process 2
beignet --id node2 --http 10.0.0.12:4700 --raft 10.0.0.12:7000 \
  --join 10.0.0.11:4700 \
  --artifact-store s3 --s3-bucket beignet-artifacts --s3-prefix cluster-a

# host or process 3
beignet --id node3 --http 10.0.0.13:4700 --raft 10.0.0.13:7000 \
  --join 10.0.0.11:4700 \
  --artifact-store s3 --s3-bucket beignet-artifacts --s3-prefix cluster-a
```

Each node writes Raft state to `raft-data/<node-id>` beneath its current
working directory. Give every voter a stable, unique ID and a stable private
address. The advertised HTTP and Raft addresses must be reachable by the other
voters; wildcard listen addresses are not useful advertised addresses.

The join endpoint only accepts a request on the current leader. v0.1 has no
supported remove-voter, replace-voter, or address-change workflow. Rehearse
cluster creation with the exact network and storage layout before relying on
it.

For a self-contained local example with three voters, shared filesystem
artifacts, three workers, and a detached scripted turn, run:

```bash
./dev-cluster.sh
```

## Run workers

```bash
BEIGNET_SIDECAR_URL=http://10.0.0.11:4700 \
BEIGNET_WORKER_ID=worker-a-unique \
BEIGNET_WORKER_LABELS='{"pool":"default","zone":"east"}' \
npm run worker
```

- `BEIGNET_SIDECAR_URL` selects the polled control-plane node.
- `BEIGNET_WORKER_ID` must be unique among concurrently running workers. When
  omitted, the reference worker derives one from hostname plus a random suffix.
- `BEIGNET_WORKER_LABELS` is a JSON string map used for exact-match routing.
- `BEIGNET_RENEW_INTERVAL_MS` defaults to 10000. Keep it comfortably below the
  30-logical-tick lease threshold.

Workers execute the Pi model runtime and coding tools on their own hosts. The
worker process therefore needs provider credentials or Pi OAuth state and
access to every working directory named by its steps. A `cwd` meaningful on
the head host is not automatically present on another worker host; use a
shared workspace or routing labels to target a suitable worker.

## Health and inspection

- `/healthz` says only that the HTTP process is serving.
- `/readyz` says the node can currently route writes to a leader.
- `/v1/status` reports node state and the known leader addresses.
- `/v1/session/<id>/steps` provides an insertion-ordered, possibly stale local
  view suitable for polling.

Route traffic only to ready nodes. A ready follower forwards writes to the
leader, while local reads can briefly lag. There is no metrics endpoint,
distributed tracing, admission control, or automatic alerting in v0.1.

## Shutdown, backup, and restore

Use `SIGINT` or `SIGTERM` for workers. The Go server has no coordinated
graceful-shutdown or drain protocol in v0.1, so stop submitting work and stop
workers before stopping voters when you need a controlled snapshot.

There is no supported online backup. For a recoverable offline backup:

1. quiesce submissions and workers;
2. stop every voter;
3. archive each voter's complete `raft-data/<node-id>` directory while
   preserving which stable ID and addresses it belongs to;
4. snapshot or copy the entire shared artifact namespace at the same point;
5. test the restore in an isolated network before treating the backup as good.

Artifact bytes and Raft metadata are both required. Copying only one voter,
only a generated snapshot file, or only the object bucket is not a complete
backup procedure.

Snapshot format version 2 is the only accepted format in v0.1. Pre-version-2
development data has no migration path and must be recreated. Restoring onto
different voter IDs or addresses is not supported by the v0.1 tooling.
