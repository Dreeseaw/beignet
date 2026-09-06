# Release notes

## v0.1 — unreleased

Beignet v0.1 is an experimental public developer preview for durable,
detachable agent turns.

### Included

- A Go control plane backed by one HashiCorp Raft group.
- Detached step submission and insertion-ordered session watching.
- Independent TypeScript/Pi pull workers for LLM and coding-tool steps.
- At-least-once recovery with claim renewal, logical lease expiry, and attempt
  fencing.
- Atomic acceptance of a result and its optional successor.
- Exact-match worker requirements inherited across a turn.
- Content-addressed artifacts on a local/shared filesystem or an S3-compatible
  object store.
- A SeaweedFS S3 contract test, a three-voter scripted demonstration, public
  CI, and a sidecar conformance probe.

### Compatibility

- Go 1.22.2 or newer is required to build the control plane.
- Node.js 24 or newer is required for the head, worker, tests, and docs.
- TypeScript runtime dependencies and CI actions are pinned.
- Snapshot format version 2 is the only supported snapshot format. Earlier
  development data must be recreated; there is no migration command.
- The S3 implementation uses the AWS SDK credential chain and is tested
  against the checksum-pinned SeaweedFS 4.41 S3 endpoint.

### Known limits

- HTTP and Raft traffic have no authentication or encryption. Private networks
  only.
- External tool effects are not exactly once.
- There is no supported online backup, rolling upgrade, voter replacement, or
  artifact garbage collection.
- There are no partitions, fan-out/joins, general DAGs, multi-tenancy, billing,
  hosted control plane, or web dashboard.
- Filesystem artifacts are safe across voters only when backed by a genuinely
  shared mount.

### Release validation

The release gates cover Go vet and race-enabled tests, TypeScript unit and
fault tests, a real three-voter detached-turn demonstration, leader failover,
snapshot/restart, the pull-protocol conformance probe, the filesystem artifact
contract, the SeaweedFS-backed S3 contract, and the documentation build.

### License

Beignet is licensed under the Apache License 2.0.
