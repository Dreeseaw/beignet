// Sidecar conformance suite. Your Go sidecar passes this or it isn't done.
//
// Prereqs (three terminals, or backgrounded):
//   1. node executor.ts                      # :4701
//   2. <your sidecar>, pointed at :4701      # :4700
//   3. node conformance.ts [sidecar-url]
//
// Exits 0 iff every check passes. Reference implementation: fakecar.ts.
import { createHash } from "node:crypto";
import { mkdtempSync, readFileSync } from "node:fs";
import os from "node:os";
import path from "node:path";

const SIDECAR = process.argv[2] ?? "http://127.0.0.1:4700";
const EXECUTOR = process.env.BEIGNET_EXECUTOR_URL ?? "http://127.0.0.1:4701";
const run = `conf-${Date.now().toString(36)}`; // fresh ids: durable ledgers must not dedup across runs
const dir = mkdtempSync(path.join(os.tmpdir(), "beignet-conf-"));

let failures = 0;
function check(name: string, ok: boolean, detail = "") {
	console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok || !detail ? "" : `  — ${detail}`}`);
	if (!ok) failures++;
}

function toolStep(id: string, command: string) {
	return {
		step_id: `${run}:${id}`,
		session: run,
		kind: "tool",
		spec: { tool: "bash", args: { command }, cwd: dir },
	};
}

async function post(body: unknown): Promise<{ status: number; body: any; text: string }> {
	const res = await fetch(`${SIDECAR}/v1/step`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: typeof body === "string" ? body : JSON.stringify(body),
	});
	const text = await res.text();
	let parsed: any = undefined;
	try { parsed = JSON.parse(text); } catch {}
	return { status: res.status, body: parsed, text };
}

async function healthy(url: string): Promise<boolean> {
	try { return (await fetch(`${url}/healthz`)).ok; } catch { return false; }
}

// 1. both processes reachable
check("executor /healthz", await healthy(EXECUTOR), `is executor.ts running on ${EXECUTOR}?`);
check("sidecar /healthz", await healthy(SIDECAR), `is your sidecar running on ${SIDECAR}?`);
if (failures) process.exit(1);

// 2. happy path: tool step executes, result carries output
{
	const { status, body } = await post(toolStep("happy", "echo conf-$((6 * 7))"));
	const text = JSON.stringify(body?.result?.content ?? "");
	check("tool step returns 200 {result}", status === 200 && body?.result != null, `got ${status}: ${JSON.stringify(body)}`);
	check("result contains command output", /conf-42/.test(text), text.slice(0, 200));
}

// 3. dedup of a DONE step: same step_id → same body, zero re-execution
{
	const step = toolStep("dedup", `echo x >> ${dir}/dedup.txt; wc -l < ${dir}/dedup.txt`);
	const first = await post(step);
	const second = await post(step);
	check("done-dedup: identical response", first.status === 200 && first.text === second.text,
		`first=${first.text.slice(0, 120)} second=${second.text.slice(0, 120)}`);
	const lines = readFileSync(`${dir}/dedup.txt`, "utf8").trim();
	check("done-dedup: executed exactly once", lines === "x", `file content: ${JSON.stringify(lines)}`);
}

// 4. dedup of a PENDING step: concurrent same-id posts share one execution
{
	const step = toolStep("pending", `sleep 2; echo y >> ${dir}/pending.txt; wc -l < ${dir}/pending.txt`);
	const [a, b] = await Promise.all([post(step), post(step)]);
	check("pending-dedup: both callers get 200", a.status === 200 && b.status === 200, `${a.status}/${b.status}`);
	check("pending-dedup: identical response", a.text === b.text,
		`a=${a.text.slice(0, 120)} b=${b.text.slice(0, 120)}`);
	const lines = readFileSync(`${dir}/pending.txt`, "utf8").trim();
	check("pending-dedup: executed exactly once", lines === "y", `file content: ${JSON.stringify(lines)}`);
}

// 5. toolError is a COMMITTED 200, and dedup applies to it too
{
	const step = toolStep("toolerr", "exit 3");
	const first = await post(step);
	check("tool failure → 200 with result.toolError", first.status === 200 && /code 3/.test(first.body?.result?.toolError ?? ""),
		`got ${first.status}: ${first.text.slice(0, 200)}`);
	const second = await post(step);
	check("toolError is committed (deduped)", second.text === first.text);
}

// 6. infra failure (executor 5xx) surfaces as sidecar 5xx {error}
{
	const { status, body } = await post({
		step_id: `${run}:infra`, session: run, kind: "tool",
		spec: { tool: "not-a-real-tool", args: {}, cwd: dir },
	});
	check("infra failure → 5xx {error}", status >= 500 && typeof body?.error === "string", `got ${status}: ${JSON.stringify(body)}`);
}

// 7. malformed requests are rejected and the server survives
{
	const bad1 = await post("this is not json");
	const bad2 = await post({ session: run, kind: "tool", spec: {} }); // missing step_id
	const bad3 = await post({ step_id: `${run}:badkind`, session: run, kind: "dance", spec: {} });
	const bad4 = await post({ step_id: `${run}:nosession`, kind: "tool", spec: {} });
	check("malformed body → 4xx", bad1.status >= 400 && bad1.status < 500, `got ${bad1.status}`);
	check("missing step_id → 4xx", bad2.status >= 400 && bad2.status < 500, `got ${bad2.status}`);
	check("unknown kind → 4xx", bad3.status >= 400 && bad3.status < 500, `got ${bad3.status}`);
	check("missing session → 4xx", bad4.status >= 400 && bad4.status < 500, `got ${bad4.status}`);
	check("sidecar alive after garbage", await healthy(SIDECAR));
}

// 8. blob store: put/get roundtrip, hash verification, missing-check
{
	const payload = JSON.stringify({ conformance: run, n: 42 });
	const hash = createHash("sha256").update(payload).digest("hex");
	const unknown = createHash("sha256").update(`${run}-never-stored`).digest("hex");

	const put = await fetch(`${SIDECAR}/v1/blob/${hash}`, {
		method: "PUT",
		headers: { "content-type": "application/octet-stream" },
		body: payload,
	});
	check("blob PUT → 200", put.status === 200, `got ${put.status}`);

	const rePut = await fetch(`${SIDECAR}/v1/blob/${hash}`, { method: "PUT", body: payload });
	check("blob re-PUT is idempotent 200", rePut.status === 200, `got ${rePut.status}`);

	const get = await fetch(`${SIDECAR}/v1/blob/${hash}`);
	const roundTrip = await get.text();
	check("blob GET returns exact bytes", get.status === 200 && roundTrip === payload,
		`got ${get.status}: ${roundTrip.slice(0, 120)}`);

	const notFound = await fetch(`${SIDECAR}/v1/blob/${unknown}`);
	check("unknown blob → 404", notFound.status === 404, `got ${notFound.status}`);

	const liar = await fetch(`${SIDECAR}/v1/blob/${unknown}`, { method: "PUT", body: payload });
	check("blob PUT with wrong hash → 400", liar.status === 400, `got ${liar.status}`);

	const missing = await fetch(`${SIDECAR}/v1/blobs/missing`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify({ hashes: [hash, unknown] }),
	});
	const body = await missing.json().catch(() => undefined);
	check("blobs/missing splits correctly",
		missing.status === 200 && JSON.stringify(body?.missing) === JSON.stringify([unknown]),
		`got ${missing.status}: ${JSON.stringify(body)}`);
}

// 9. fire-and-forget submission: ?wait=false returns immediately
{
	const step = toolStep("nowait", `echo nowait >> ${dir}/nowait.txt`);
	const res = await fetch(`${SIDECAR}/v1/step?wait=false`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(step),
	});
	const body = await res.json().catch(() => undefined);
	check("wait=false → 202 {step_id}", res.status === 202 && body?.step_id === step.step_id,
		`got ${res.status}: ${JSON.stringify(body)}`);

	// The work still happens with nobody waiting.
	const deadline = Date.now() + 15000;
	let ran = false;
	while (Date.now() < deadline && !ran) {
		const again = await post(step); // long-poll dedups into the same execution
		ran = again.status === 200;
		if (!ran) await new Promise((r) => setTimeout(r, 200));
	}
	check("wait=false work completes unattended", ran);
}

// 10. session read route: ordered, contains what we submitted
{
	const res = await fetch(`${SIDECAR}/v1/session/${encodeURIComponent(run)}/steps`);
	const body = await res.json().catch(() => undefined);
	const list = body?.steps;
	check("session route → 200 {steps: [...]}", res.status === 200 && Array.isArray(list),
		`got ${res.status}: ${JSON.stringify(body)?.slice(0, 160)}`);
	if (Array.isArray(list)) {
		check("session steps carry index/step_id/kind/state",
			list.length > 0 && list.every((s: any) =>
				typeof s.index === "number" && typeof s.step_id === "string" &&
				typeof s.kind === "string" && typeof s.state === "string"),
			JSON.stringify(list[0])?.slice(0, 160));
		check("session steps are in submission order",
			list.every((s: any, i: number) => s.index === i));

		const sinceRes = await fetch(`${SIDECAR}/v1/session/${encodeURIComponent(run)}/steps?since=1`);
		const sinceBody = await sinceRes.json().catch(() => undefined);
		check("?since=N skips the first N", sinceBody?.steps?.length === list.length - 1,
			`full=${list.length} since1=${sinceBody?.steps?.length}`);
	}
	const empty = await fetch(`${SIDECAR}/v1/session/no-such-session-${run}/steps`);
	const emptyBody = await empty.json().catch(() => undefined);
	check("unknown session → 200 with empty list",
		empty.status === 200 && Array.isArray(emptyBody?.steps) && emptyBody.steps.length === 0,
		`got ${empty.status}: ${JSON.stringify(emptyBody)}`);
}

console.log(failures ? `\n${failures} FAILED` : "\nALL PASS");
process.exit(failures ? 1 : 0);
