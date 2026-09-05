// THE v1 GATE: submit one llm step, let the client exit, and the cluster
// finishes a multi-step turn by itself. Zero tokens (scripted llm results),
// real bash execution, real chaining.
import { test, before, after } from "node:test";
import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { launch, waitHealthy, freePort, executorDir } from "./helpers.ts";

const sidecarPort = freePort();
const sidecarUrl = `http://127.0.0.1:${sidecarPort}`;
const workDir = mkdtempSync(path.join(os.tmpdir(), "beignet-headless-"));
const headPath = path.join(path.dirname(executorDir), "head", "head.ts");
const session = `test-${Date.now().toString(36)}`;

function assistant(content: any[], stopReason: string) {
	return {
		role: "assistant",
		content,
		api: "anthropic-messages",
		provider: "test",
		model: "scripted",
		usage: { input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 } },
		stopReason,
		timestamp: 1,
	};
}

// Turn shape: llm → tool,tool → llm → tool → llm(stop) = 6 steps.
const script = [
	assistant([
		{ type: "text", text: "Making two files." },
		// Deliberately slow: guarantees the turn is still running when the
		// client exits, so completion proves unattended progress.
		{ type: "toolCall", id: "c1", name: "bash", arguments: { command: `sleep 3; echo one > ${workDir}/one.txt` } },
		{ type: "toolCall", id: "c2", name: "bash", arguments: { command: `echo two > ${workDir}/two.txt` } },
	], "toolUse"),
	assistant([
		{ type: "toolCall", id: "c3", name: "bash", arguments: { command: `cat ${workDir}/one.txt ${workDir}/two.txt > ${workDir}/both.txt` } },
	], "toolUse"),
	assistant([{ type: "text", text: "HEADLESS_TURN_COMPLETE" }], "stop"),
];

let worker: ReturnType<typeof launch>;
let fakecar: ReturnType<typeof launch>;

before(async () => {
	const scriptPath = path.join(workDir, "llm-script.json");
	writeFileSync(scriptPath, JSON.stringify(script));
	fakecar = launch("fakecar.ts", {
		BEIGNET_SIDECAR_PORT: String(sidecarPort),
	});
	worker = launch("worker.ts", {
		BEIGNET_SIDECAR_URL: sidecarUrl,
		BEIGNET_FAKE_LLM: scriptPath,
		BEIGNET_WORKER_ID: "headless-worker",
		BEIGNET_WORKER_LABELS: JSON.stringify({ pool: "test", zone: "east" }),
		BEIGNET_RENEW_INTERVAL_MS: "100",
	});
	await waitHealthy(sidecarUrl);
});

after(() => {
	worker.kill("SIGKILL");
	fakecar.kill("SIGKILL");
});

function runHead(args: string[]): Promise<{ code: number | null; stdout: string; stderr: string }> {
	return new Promise((resolve) => {
		const child = spawn("node", [headPath, ...args], {
			cwd: workDir,
			stdio: ["ignore", "pipe", "pipe"],
			env: { ...process.env, BEIGNET_SIDECAR_URL: sidecarUrl },
		});
		let stdout = "";
		let stderr = "";
		child.stdout.on("data", (d) => (stdout += d));
		child.stderr.on("data", (d) => (stderr += d));
		child.on("close", (code) => resolve({ code, stdout, stderr }));
	});
}

async function steps(): Promise<any[]> {
	const res = await fetch(`${sidecarUrl}/v1/session/${session}/steps`);
	return ((await res.json()) as any).steps ?? [];
}

test("a turn completes after the client exits", { timeout: 90_000 }, async () => {
	const head = await runHead([
		"start", "--session", session, "--cwd", workDir,
		"--require", "pool=test", "--require", "zone=east", "do the thing",
	]);
	assert.equal(head.code, 0, `head failed: ${head.stderr}`);
	assert.match(head.stdout, /detaching/);

	// The client is GONE. Nothing is attached to the cluster from here on.
	const atDetach = await steps();
	assert.ok(
		!atDetach.some((s) => s.result?.stopReason === "stop"),
		"turn was already finished at detach; the test proves nothing",
	);
	assert.ok(atDetach.length < 6, `turn already complete at detach (${atDetach.length} steps)`);

	const deadline = Date.now() + 60_000;
	let final: any[] = [];
	while (Date.now() < deadline) {
		final = await steps();
		const last = final[final.length - 1];
		if (last?.kind === "llm" && last.state === "done" && last.result?.stopReason === "stop") break;
		await new Promise((r) => setTimeout(r, 200));
	}

	const kinds = final.map((s) => `${s.kind}${s.kind === "tool" ? ":" + s.spec.tool : ""}`);
	assert.deepEqual(
		kinds,
		["llm", "tool:bash", "tool:bash", "llm", "tool:bash", "llm"],
		"the cluster chained the whole turn unattended",
	);
	assert.ok(final.every((s) => s.state === "done"), "every step committed");
	assert.ok(
		final.every((s) => s.requirements?.pool === "test" && s.requirements?.zone === "east"),
		"routing requirements follow the turn",
	);

	// Tool steps really ran, in order, on the machine.
	assert.ok(existsSync(`${workDir}/one.txt`) && existsSync(`${workDir}/two.txt`));
	assert.equal(readFileSync(`${workDir}/both.txt`, "utf8"), "one\ntwo\n");

	// The final assistant message closed the turn.
	assert.match(JSON.stringify(final.at(-1).result.content), /HEADLESS_TURN_COMPLETE/);
	const stats = await (await fetch(`${sidecarUrl}/v1/debug/stats`)).json() as any;
	assert.ok(stats.renewals > 0, "the worker renewed its lease during the slow tool");
});

test("context accumulates: the last llm step sees the whole turn", async () => {
	const all = await steps();
	const first = all.find((s) => s.kind === "llm");
	const last = all.at(-1);
	assert.equal(first.spec.context.messages.length, 1, "turn opened with just the user message");
	assert.equal(
		last.spec.context.messages.length,
		6,
		"user + assistant + 2 tool results + assistant + 1 tool result",
	);
	// Blob refs, never inlined copies.
	assert.ok(last.spec.context.messages.every((m: any) => typeof m === "string" && m.length === 64));
});

test("a second turn inherits model, system prompt and tools from the first", async () => {
	const before = (await steps()).length;
	const head = await runHead(["start", "--session", session, "--cwd", workDir, "and again"]);
	assert.equal(head.code, 0, head.stderr);

	const deadline = Date.now() + 30_000;
	let after: any[] = [];
	while (Date.now() < deadline) {
		after = await steps();
		if (after.length > before && after.at(-1)?.state === "done") break;
		await new Promise((r) => setTimeout(r, 200));
	}
	const turn2 = after[before];
	const turn1 = after.find((s) => s.kind === "llm");
	assert.equal(turn2.kind, "llm");
	assert.equal(turn2.spec.context.systemPrompt, turn1.spec.context.systemPrompt);
	assert.equal(turn2.spec.context.tools, turn1.spec.context.tools);
	assert.deepEqual(turn2.spec.model, turn1.spec.model);
	assert.ok(
		turn2.spec.context.messages.length > turn1.spec.context.messages.length,
		"turn 2 carries turn 1's history",
	);
});

test("worker claims only steps matching its labels", { timeout: 10_000 }, async () => {
	const submit = async (step_id: string, requirement: string, output: string) => {
		const res = await fetch(`${sidecarUrl}/v1/step?wait=false`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({
				step_id,
				session: `${session}-labels`,
				kind: "tool",
				spec: { tool: "bash", args: { command: `echo ${output}` }, cwd: workDir },
				requirements: { pool: requirement },
			}),
		});
		assert.equal(res.status, 202);
	};
	await submit("label-mismatch", "other", "wrong");
	await submit("label-match", "test", "right");

	let listed: any[] = [];
	const deadline = Date.now() + 5_000;
	while (Date.now() < deadline) {
		listed = await fetch(`${sidecarUrl}/v1/session/${session}-labels/steps`)
			.then((res) => res.json()).then((body: any) => body.steps);
		if (listed.find((step) => step.step_id === "label-match")?.state === "done") break;
		await new Promise((resolve) => setTimeout(resolve, 100));
	}
	assert.equal(listed.find((step) => step.step_id === "label-mismatch")?.state, "pending");
	assert.equal(listed.find((step) => step.step_id === "label-match")?.state, "done");
});
