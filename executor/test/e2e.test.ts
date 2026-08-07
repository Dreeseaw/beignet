// OBSOLETE — safe to delete this file along with ../../shim/.
//
// This tested the head-driven design: a pi head whose loop routed each tool
// call and provider call through the cluster, with the head sequencing the
// turn. v1 replaced that — the cluster chains turns itself and the head is a
// disposable client (zombie/head/head.ts).
//
// Superseded by: test/headless.test.ts (the v1 gate) and test/chain.test.ts.
import { test } from "node:test";

test("superseded by headless.test.ts", { skip: "head-driven design removed in v1" }, () => {});
