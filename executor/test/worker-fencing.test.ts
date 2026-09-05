import { test } from "node:test";
import assert from "node:assert/strict";
import { spawn, spawnSync, type ChildProcess } from "node:child_process";
import { existsSync, mkdtempSync, readFileSync, rmSync } from "node:fs";
import os from "node:os";
import path from "node:path";
import { executorDir, freePort } from "./helpers.ts";

const repoDir = path.dirname(executorDir);

function capture(child: ChildProcess) {
	let output = "";
	child.stdout?.on("data", (data) => (output += data));
	child.stderr?.on("data", (data) => (output += data));
	return () => output;
}

async function waitFor(description: string, predicate: () => boolean | Promise<boolean>, timeoutMs = 15_000) {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		if (await predicate()) return;
		await new Promise((resolve) => setTimeout(resolve, 50));
	}
	throw new Error(`timed out waiting for ${description}`);
}

test("an expired worker attempt cannot overwrite its replacement", { timeout: 30_000 }, async (t) => {
	const runDir = mkdtempSync(path.join(os.tmpdir(), "beignet-fence-"));
	const binary = path.join(runDir, "beignet");
	const history = path.join(runDir, "executions.log");
	const httpPort = freePort();
	let raftPort = freePort();
	while (raftPort === httpPort) raftPort = freePort();
	const sidecar = `http://127.0.0.1:${httpPort}`;
	const children: ChildProcess[] = [];
	let workerA: ChildProcess | undefined;

	t.after(() => {
		if (workerA?.pid) process.kill(workerA.pid, "SIGCONT");
		for (const child of children) child.kill("SIGKILL");
		rmSync(runDir, { recursive: true, force: true });
	});

	const build = spawnSync("go", ["build", "-o", binary, "."], {
		cwd: repoDir,
		encoding: "utf8",
	});
	assert.equal(build.status, 0, build.stderr);

	const server = spawn(binary, [
		"--id", "fence-node",
		"--http", `127.0.0.1:${httpPort}`,
		"--raft", `127.0.0.1:${raftPort}`,
		"--artifact-dir", path.join(runDir, "artifacts"),
	], { cwd: runDir, stdio: ["ignore", "pipe", "pipe"] });
	children.push(server);
	const serverLog = capture(server);
	await waitFor("the real sidecar to become ready", async () => {
		try { return (await fetch(`${sidecar}/readyz`)).ok; } catch { return false; }
	});

	function startWorker(id: string, renewIntervalMs = "50") {
		const child = spawn("node", [path.join(executorDir, "worker.ts")], {
			cwd: runDir,
			stdio: ["ignore", "pipe", "pipe"],
			env: {
				...process.env,
				BEIGNET_SIDECAR_URL: sidecar,
				BEIGNET_WORKER_ID: id,
				BEIGNET_WORKER_LABELS: JSON.stringify({ pool: "fence" }),
				BEIGNET_RENEW_INTERVAL_MS: renewIntervalMs,
			},
		});
		children.push(child);
		return { child, log: capture(child) };
	}

	// Keep A's renewal asleep for the whole fault window so its stale result
	// must reach CommitResult and be fenced by the server after SIGCONT.
	const first = startWorker("worker-a", "60000");
	workerA = first.child;
	const command = [
		`printf 'start:%s\\n' \"$$\" >> ${history}`,
		"sleep 2",
		`printf 'finish:%s\\n' \"$$\" >> ${history}`,
		"printf 'result:%s\\n' \"$$\"",
	].join("; ");
	const submitted = await fetch(`${sidecar}/v1/step?wait=false`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify({
			step_id: "fenced-step",
			session: "fenced-session",
			kind: "tool",
			spec: { tool: "bash", args: { command }, cwd: runDir },
			requirements: { pool: "fence" },
		}),
	});
	assert.equal(submitted.status, 202);

	const executions = () => existsSync(history)
		? readFileSync(history, "utf8").split("\n").filter((line) => line.startsWith("start:"))
		: [];
	await waitFor("worker A to start the command", () => executions().length === 1);
	assert.ok(workerA.pid);
	process.kill(workerA.pid, "SIGSTOP");
	await waitFor("worker A to enter the stopped state", () => {
		return /^State:\s+T/m.test(readFileSync(`/proc/${workerA!.pid}/status`, "utf8"));
	});

	for (let tick = 0; tick <= 30; tick++) {
		const advanced = await fetch(`${sidecar}/v1/internal/apply`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ type: "Tick", data: {} }),
		});
		assert.equal(advanced.status, 200, `logical tick ${tick} failed: ${await advanced.text()}`);
	}
	const readStep = async () => {
		const res = await fetch(`${sidecar}/v1/session/fenced-session/steps`);
		return ((await res.json()) as any).steps[0];
	};
	await waitFor("worker A's lease to expire", async () => (await readStep()).state === "pending");

	const second = startWorker("worker-b");
	await waitFor("worker B to re-execute the step", () => executions().length === 2);
	const [firstPID, secondPID] = executions().map((line) => line.slice("start:".length));
	assert.notEqual(firstPID, secondPID, "the replacement execution must be distinct");
	process.kill(workerA.pid, "SIGCONT");

	await waitFor("worker B's result to commit", async () => (await readStep()).state === "done");
	const accepted = JSON.stringify((await readStep()).result);
	assert.match(accepted, new RegExp(`result:${secondPID}\\\\n`), `accepted result was not worker B's: ${accepted}`);
	assert.doesNotMatch(accepted, new RegExp(`result:${firstPID}\\\\n`));
	await waitFor("worker A's stale commit to be fenced", () => /commit fenced/.test(first.log()));
	assert.doesNotMatch(first.log(), /lease lost/);
	assert.match(second.log(), /claimed tool fenced-step attempt 1/);
	assert.doesNotMatch(serverLog(), /panic|fatal error/i);
});
