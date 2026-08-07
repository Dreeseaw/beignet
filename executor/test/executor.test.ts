import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { mkdtempSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { launch, waitHealthy, freePort } from "./helpers.ts";
import { rebuildContext } from "../context.ts";

const port = freePort();
const url = `http://127.0.0.1:${port}`;
let child: ReturnType<typeof launch>;

before(async () => {
	child = launch("executor.ts", { BEIGNET_EXECUTOR_PORT: String(port) });
	await waitHealthy(url);
});

after(() => child.kill("SIGKILL"));

async function executeStep(kind: string, spec: any) {
	const res = await fetch(`${url}/v1/execute`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify({ kind, spec }),
	});
	return { status: res.status, body: await res.json() };
}

test("tool step: bash runs and captures output", async () => {
	const { status, body } = await executeStep("tool", {
		tool: "bash",
		args: { command: "echo hello-from-executor-$((21 * 2))" },
		cwd: os.tmpdir(),
	});
	assert.equal(status, 200);
	const text = JSON.stringify(body.result.content);
	assert.match(text, /hello-from-executor-42/);
});

test("tool step: read returns file contents", async () => {
	const dir = mkdtempSync(path.join(os.tmpdir(), "beignet-"));
	writeFileSync(path.join(dir, "note.txt"), "zombie payload 123\n");
	const { status, body } = await executeStep("tool", {
		tool: "read",
		args: { path: path.join(dir, "note.txt") },
		cwd: dir,
	});
	assert.equal(status, 200);
	assert.match(JSON.stringify(body.result.content), /zombie payload 123/);
});

test("tool step: unknown tool → 500 with error", async () => {
	const { status, body } = await executeStep("tool", { tool: "nope", args: {}, cwd: "/tmp" });
	assert.equal(status, 500);
	assert.match(body.error, /unknown tool/);
});

test("rebuildContext resolves refs from blobs", async () => {
	const store = new Map<string, string>([
		["h-sp", JSON.stringify("You are helpful.")],
		["h-m1", JSON.stringify({ role: "user", content: [{ type: "text", text: "hi" }] })],
		["h-m2", JSON.stringify({ role: "assistant", content: [{ type: "text", text: "hello" }] })],
		["h-tools", JSON.stringify([{ name: "bash", description: "run", parameters: {} }])],
	]);
	const getBlob = async (h: string) => {
		const blob = store.get(h);
		if (blob === undefined) throw new Error(`missing ${h}`);
		return blob;
	};
	const context = await rebuildContext(
		{ systemPrompt: "h-sp", messages: ["h-m1", "h-m2"], tools: "h-tools" },
		getBlob,
	);
	assert.equal(context.systemPrompt, "You are helpful.");
	assert.equal(context.messages.length, 2);
	assert.equal(context.messages[1].content[0].text, "hello");
	assert.equal(context.tools[0].name, "bash");
});

test("rebuildContext fails loudly on missing blob or inline context", async () => {
	const getBlob = async () => { throw new Error("missing blob"); };
	await assert.rejects(
		rebuildContext({ systemPrompt: null, messages: ["h-gone"], tools: null }, getBlob),
		/missing blob/,
	);
	await assert.rejects(
		rebuildContext({ messages: [{ role: "user" }] } as any, getBlob),
		/hash string/,
	);
});

test("bash failure is a committed toolError, not a transport error", async () => {
	const { status, body } = await executeStep("tool", {
		tool: "bash",
		args: { command: "exit 3" },
		cwd: os.tmpdir(),
	});
	assert.equal(status, 200);
	assert.match(body.result.toolError, /code 3/);
});
