---
layout: home

hero:
  name: Beignet
  text: Agent turns that outlive their clients
  tagline: Replicated control state, independent pull workers, and immutable artifacts on filesystem or S3.
  actions:
    - theme: brand
      text: Quickstart
      link: /quickstart
    - theme: alt
      text: Read the architecture
      link: /DESIGN

features:
  - title: Detachable heads
    details: Submit one turn-opening step and leave. Workers continue the chain without a client connection.
  - title: Fenced pull work
    details: Workers claim compatible steps and renew leases; expired attempts cannot commit stale results.
  - title: Small replicated core
    details: Raft stores control metadata. Immutable artifact bytes stay in filesystem or S3-compatible storage.
---

## Experimental developer preview

Beignet v0.1 is deliberately narrow. It demonstrates durable, unattended
agent turns on infrastructure you control. It is not an agent platform,
workflow engine, or hosted service.

::: danger Private networks only
The HTTP and Raft services have no authentication or encryption. Bind them to
loopback or a trusted private network, never the public internet.
:::

The execution guarantee is **at least once**. Attempt fencing protects the
replicated ledger, but tools can repeat external side effects if a worker dies
after acting and before its result commits.
