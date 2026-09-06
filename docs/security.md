# Security

Beignet v0.1 is safe to evaluate only inside a trusted environment. It is not
safe to expose directly to the public internet or to untrusted tenants.

## No protocol security

The HTTP and Raft listeners have:

- no authentication or authorization;
- no transport encryption;
- no request signing or replay protection;
- no tenant isolation or per-session access control.

Any caller that can reach the HTTP listener can submit work, read sessions and
artifacts, upload data, claim work, attempt commits, and reach membership or
internal forwarding routes. Any peer that can reach the Raft listener can
attack an unencrypted cluster protocol.

Bind both listeners to loopback for local use. For a private cluster, use host
firewalls and network policy to allow only explicit heads, workers, and voters.
Do not rely on URL paths as a security boundary. If a gateway supplies TLS or
authentication, remember that the Raft port still needs an equally deliberate
private boundary.

## Workers run arbitrary tools

Workers execute model-selected coding tools, including shell commands, with
the worker process's operating-system identity. A submitted spec can name a
working directory and carry opaque tool arguments. Treat every worker as a
high-trust code-execution service.

Run workers with least-privilege OS accounts and isolated workspaces. Give
them only the repositories, commands, network destinations, and temporary
credentials required for their pool. Do not place unrelated secrets in their
environment or home directory. Exact-match labels are scheduling metadata,
not an authorization mechanism.

Provider API keys and Pi OAuth state live with the workers that call models.
They are not protected by the Beignet protocol. Logs, tool output, model
prompts, step specs, and result artifacts can all contain sensitive data.

## At-least-once effects

Attempt fencing protects which result the replicated ledger accepts. It does
not roll back an external effect. A worker can send an email, mutate a
repository, charge an API, or alter a database and then die before its commit
reaches Raft. A replacement worker may execute the same step again.

Use tools and downstream services that support idempotency keys or
transactions. Otherwise constrain effects to disposable workspaces or design
an explicit compensation process. Do not describe a fenced result as
exactly-once execution.

## Artifact-store boundary

Filesystem artifacts need a shared mount with permissions restricted to the
Beignet service accounts. S3 deployments should use a dedicated bucket or
prefix, scoped credentials, encryption in transit, and the storage provider's
encryption and audit controls.

The server verifies artifact content against its SHA-256 on write and read.
This detects corruption but is not access control or confidentiality.

The per-request upload limit is 64 MiB. v0.1 has no aggregate quota, rate
limit, retention policy, garbage collector, malware scan, content-type policy,
or protection against an authorized caller filling the ledger or artifact
store. Apply network-level admission controls and storage limits in the
surrounding environment.

## Unsupported security postures

Do not use v0.1 as:

- a public endpoint;
- a multi-tenant service;
- a sandbox for mutually untrusted users;
- a secrets store;
- an exactly-once job runner;
- a production control plane requiring audited upgrades or recovery.

These are product boundaries, not configuration gaps that can be fixed by
changing one flag.
