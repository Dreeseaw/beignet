// Sidecar wire conformance for the v0.1 pull protocol.
//
// Start a sidecar with no workers, then run:
//   node executor/conformance.ts [sidecar-url]
import { createHash, randomUUID } from "node:crypto";

const SIDECAR = process.argv[2] ?? "http://127.0.0.1:4700";
const run = `conf-${randomUUID()}`;
let failures = 0;

function check(name: string, ok: boolean, detail = "") {
	console.log(`${ok ? "PASS" : "FAIL"}  ${name}${ok || !detail ? "" : ` — ${detail}`}`);
	if (!ok) failures++;
}

async function request(method: string, path: string, body?: unknown, contentType = "application/json") {
	const res = await fetch(`${SIDECAR}${path}`, {
		method,
		headers: body === undefined ? undefined : { "content-type": contentType },
		body: body === undefined ? undefined : typeof body === "string" ? body : JSON.stringify(body),
	});
	const text = await res.text();
	let json: any;
	try { json = JSON.parse(text); } catch {}
	return { status: res.status, text, json };
}

const post = (path: string, body: unknown) => request("POST", path, body);

try {
	const health = await request("GET", "/healthz");
	check("health endpoint", health.status === 200, `HTTP ${health.status}`);
} catch (error: any) {
	console.error(`FAIL  sidecar unavailable at ${SIDECAR}: ${error?.message ?? error}`);
	process.exit(1);
}

// Immutable artifact contract.
const artifact = JSON.stringify({ run, value: 42 });
const hash = createHash("sha256").update(artifact).digest("hex");
const missingHash = createHash("sha256").update(`${run}-missing`).digest("hex");
{
	const missing = await request("GET", `/v1/blob/${missingHash}`);
	check("unknown artifact is 404", missing.status === 404, `HTTP ${missing.status}`);

	const wrong = await request("PUT", `/v1/blob/${missingHash}`, artifact, "application/octet-stream");
	check("artifact hash mismatch is rejected", wrong.status === 400, `HTTP ${wrong.status}`);

	const put = await request("PUT", `/v1/blob/${hash}`, artifact, "application/octet-stream");
	const repeat = await request("PUT", `/v1/blob/${hash}`, artifact, "application/octet-stream");
	check("artifact PUT succeeds", put.status === 200, `HTTP ${put.status}`);
	check("artifact PUT is idempotent", repeat.status === 200, `HTTP ${repeat.status}`);

	const get = await request("GET", `/v1/blob/${hash}`);
	check("artifact GET returns exact bytes", get.status === 200 && get.text === artifact,
		`HTTP ${get.status}: ${get.text.slice(0, 100)}`);

	const batch = await post("/v1/blobs/missing", { hashes: [hash, missingHash] });
	check("artifact missing check returns only absent hashes",
		batch.status === 200 && JSON.stringify(batch.json?.missing) === JSON.stringify([missingHash]),
		`HTTP ${batch.status}: ${batch.text}`);
}

// Request validation leaves no work behind.
{
	const invalidJSON = await post("/v1/step", "not json");
	const missingID = await post("/v1/step", { session: run, kind: "tool", spec: {} });
	const badKind = await post("/v1/step", { step_id: `${run}-bad`, session: run, kind: "dance", spec: {} });
	const missingWorker = await post("/v1/work/claim", { labels: {} });
	check("invalid JSON is rejected", invalidJSON.status === 400, `HTTP ${invalidJSON.status}`);
	check("missing step id is rejected", missingID.status === 400, `HTTP ${missingID.status}`);
	check("unknown step kind is rejected", badKind.status === 400, `HTTP ${badKind.status}`);
	check("missing worker id is rejected", missingWorker.status === 400, `HTTP ${missingWorker.status}`);
}

// Claim, renew, fence, atomic successor insertion, and requirement inheritance.
const firstID = `${run}-first`;
const nextID = `${run}-next`;
const reservedNextID = `${run}-reserved-next`;
const first = {
	step_id: firstID,
	session: run,
	kind: "tool",
	spec: { tool: "bash", args: { command: "echo conformance" }, cwd: "/tmp" },
	requirements: { pool: "gpu" },
};
const claimRequest = { worker_id: "worker-a", labels: { pool: "gpu", zone: "test" } };
let claim: any;
{
	const submit = await post("/v1/step?wait=false", first);
	check("detached submission is accepted", submit.status === 202 && submit.json?.step_id === firstID,
		`HTTP ${submit.status}: ${submit.text}`);

	const mismatch = await post("/v1/work/claim", { worker_id: "worker-b", labels: { pool: "cpu" } });
	check("ineligible worker receives no work", mismatch.status === 204, `HTTP ${mismatch.status}`);

	const won = await post("/v1/work/claim", claimRequest);
	claim = won.json;
	check("eligible worker claims the step",
		won.status === 200 && claim?.step_id === firstID && claim?.attempt === 0,
		`HTTP ${won.status}: ${won.text}`);
	check("claim preserves opaque spec and requirements",
		JSON.stringify(claim?.spec) === JSON.stringify(first.spec) && claim?.requirements?.pool === "gpu");

	const retry = await post("/v1/work/claim", claimRequest);
	check("ambiguous claim retry returns the same attempt",
		retry.status === 200 && retry.json?.step_id === firstID && retry.json?.attempt === claim.attempt,
		`HTTP ${retry.status}: ${retry.text}`);

	const staleRenew = await post("/v1/work/renew", {
		worker_id: "worker-a", step_id: firstID, attempt: claim.attempt + 1,
	});
	const renew = await post("/v1/work/renew", {
		worker_id: "worker-a", step_id: firstID, attempt: claim.attempt,
	});
	check("stale renewal is fenced", staleRenew.status === 409 && staleRenew.json?.renewed === false,
		`HTTP ${staleRenew.status}: ${staleRenew.text}`);
	check("current owner can renew", renew.status === 200 && renew.json?.renewed === true,
		`HTTP ${renew.status}: ${renew.text}`);

	const staleCommit = await post("/v1/work/commit", {
		worker_id: "worker-b", step_id: firstID, attempt: claim.attempt,
		result: { value: "stale" },
	});
	check("non-owner commit is fenced", staleCommit.status === 409 && staleCommit.json?.committed === false,
		`HTTP ${staleCommit.status}: ${staleCommit.text}`);

	const reserve = await post("/v1/step?wait=false", {
		step_id: reservedNextID,
		session: `${run}-reserved`,
		kind: "tool",
		spec: { tool: "bash", args: { command: "true" }, cwd: "/tmp" },
		requirements: { pool: "reserved" },
	});
	check("successor collision fixture is accepted", reserve.status === 202,
		`HTTP ${reserve.status}: ${reserve.text}`);
	const collision = await post("/v1/work/commit", {
		worker_id: "worker-a",
		step_id: firstID,
		attempt: claim.attempt,
		result: { value: "must-not-land" },
		next: { step_id: reservedNextID, session: run, kind: "llm", spec: { model: "opaque" } },
	});
	check("existing successor id rejects the whole commit",
		collision.status === 409 && collision.json?.committed === false &&
			collision.json?.reason === "next step exists",
		`HTTP ${collision.status}: ${collision.text}`);
	const afterCollision = await post("/v1/work/renew", {
		worker_id: "worker-a", step_id: firstID, attempt: claim.attempt,
	});
	check("rejected successor collision leaves the claim live",
		afterCollision.status === 200 && afterCollision.json?.renewed === true,
		`HTTP ${afterCollision.status}: ${afterCollision.text}`);

	const commit = await post("/v1/work/commit", {
		worker_id: "worker-a",
		step_id: firstID,
		attempt: claim.attempt,
		result: { value: "accepted" },
		next: { step_id: nextID, session: run, kind: "llm", spec: { model: "opaque" } },
	});
	check("current owner commits result and successor", commit.status === 200 && commit.json?.committed === true,
		`HTTP ${commit.status}: ${commit.text}`);

	const dedup = await post("/v1/step", first);
	check("completed step deduplicates without execution",
		dedup.status === 200 && dedup.json?.result?.value === "accepted",
		`HTTP ${dedup.status}: ${dedup.text}`);
}

{
	const mismatch = await post("/v1/work/claim", { worker_id: "worker-b", labels: { pool: "cpu" } });
	check("successor inherits routing requirements", mismatch.status === 204, `HTTP ${mismatch.status}`);

	const next = await post("/v1/work/claim", { worker_id: "worker-b", labels: { pool: "gpu" } });
	check("eligible worker claims atomic successor",
		next.status === 200 && next.json?.step_id === nextID && next.json?.requirements?.pool === "gpu",
		`HTTP ${next.status}: ${next.text}`);
	const finish = await post("/v1/work/commit", {
		worker_id: "worker-b", step_id: nextID, attempt: next.json?.attempt,
		result: { stopReason: "stop" },
	});
	check("successor result commits", finish.status === 200 && finish.json?.committed === true,
		`HTTP ${finish.status}: ${finish.text}`);
}

// Session reads expose committed history in insertion order.
{
	const session = await request("GET", `/v1/session/${encodeURIComponent(run)}/steps`);
	const listed = session.json?.steps;
	check("session route returns both steps", session.status === 200 && listed?.length === 2,
		`HTTP ${session.status}: ${session.text}`);
	check("session order and results are stable",
		listed?.[0]?.step_id === firstID && listed?.[0]?.result?.value === "accepted" &&
		listed?.[1]?.step_id === nextID && listed?.[1]?.result?.stopReason === "stop");

	const since = await request("GET", `/v1/session/${encodeURIComponent(run)}/steps?since=1`);
	check("session cursor skips earlier steps",
		since.status === 200 && since.json?.steps?.length === 1 && since.json.steps[0].index === 1,
		`HTTP ${since.status}: ${since.text}`);

	const empty = await request("GET", `/v1/session/no-such-${run}/steps`);
	check("unknown session is an empty list", empty.status === 200 && empty.json?.steps?.length === 0,
		`HTTP ${empty.status}: ${empty.text}`);
}

console.log(failures ? `\n${failures} FAILED` : "\nALL PASS");
process.exit(failures ? 1 : 0);
