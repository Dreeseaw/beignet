// Fake sidecar: the beignet wire contract with an in-memory ledger.
// Stand-in for the Go sidecar during development, and the test double for CI.
// Mirrors the FSM's atomic commit + successor insertion and fenced pull work.
import http from "node:http";
import { createHash } from "node:crypto";

const PORT = Number(process.env.BEIGNET_SIDECAR_PORT ?? 4700);

type Step = {
	id: string;
	session: string;
	kind: string;
	spec: any;
	requirements: Record<string, string>;
	state: "pending" | "claimed" | "done";
	owner?: string;
	attempt: number;
	payload?: any;
	waiters: Array<(step: Step) => void>;
};

const steps = new Map<string, Step>();
const order = new Map<string, string[]>(); // session -> step ids, submission order
const blobs = new Map<string, string>();
let renewals = 0;

function submit(
	id: string,
	session: string,
	kind: string,
	spec: any,
	requirements: Record<string, string> = {},
): Step {
	const existing = steps.get(id);
	if (existing) return existing; // dedup: SubmitStep is a no-op if id exists
	const step: Step = {
		id, session, kind, spec, requirements: { ...requirements },
		state: "pending", attempt: 0, waiters: [],
	};
	steps.set(id, step);
	if (!order.has(session)) order.set(session, []);
	order.get(session)!.push(id);
	console.error(
		`[fakecar] ${id.slice(0, 12)} submit ${kind}${kind === "tool" ? `:${spec.tool}` : ""} (session ${session.slice(0, 8)})`,
	);
	return step;
}

/** Apply(CommitResult): mark Done and insert `next` in one transition. */
function commit(step: Step, payload: any, next?: any): void {
	step.state = "done";
	step.payload = payload;
	if (next) submit(next.step_id, next.session, next.kind, next.spec, step.requirements);
	console.error(`[fakecar] ${step.id.slice(0, 12)} committed${next ? " +next" : ""}`);
	for (const waiter of step.waiters) waiter(step);
	step.waiters = [];
}

function labelsMatch(requirements: Record<string, string>, labels: Record<string, string>): boolean {
	return Object.entries(requirements).every(([key, value]) => labels[key] === value);
}

function findWork(worker: string, labels: Record<string, string>): Step | undefined {
	let pending: Step | undefined;
	for (const step of steps.values()) {
		if (!labelsMatch(step.requirements, labels)) continue;
		if (step.state === "claimed" && step.owner === worker) return step;
		if (!pending && step.state === "pending") pending = step;
	}
	return pending;
}

function readBody(req: http.IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		let data = "";
		req.on("data", (c) => (data += c));
		req.on("end", () => resolve(data));
		req.on("error", reject);
	});
}

function replyStep(res: http.ServerResponse, step: Step) {
	res.writeHead(200, { "content-type": "application/json" });
	res.end(JSON.stringify({ result: step.payload }));
}

const server = http.createServer(async (req, res) => {
	const url = new URL(req.url ?? "/", "http://localhost");

	if (req.method === "GET" && url.pathname === "/healthz") {
		res.writeHead(200).end("ok");
		return;
	}
	if (req.method === "GET" && url.pathname === "/v1/debug/stats") {
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ steps: steps.size, blobs: blobs.size, sessions: order.size, renewals }));
		return;
	}

	// ---- pull workers ----
	if (req.method === "POST" && url.pathname === "/v1/work/claim") {
		try {
			const { worker_id, labels = {} } = JSON.parse(await readBody(req));
			if (typeof worker_id !== "string" || !worker_id) throw new Error("missing worker_id");
			const step = findWork(worker_id, labels);
			if (!step) {
				res.writeHead(204).end();
				return;
			}
			step.state = "claimed";
			step.owner = worker_id;
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({
				step_id: step.id, session: step.session, kind: step.kind, spec: step.spec,
				requirements: step.requirements, attempt: step.attempt,
			}));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/renew") {
		try {
			const { worker_id, step_id, attempt } = JSON.parse(await readBody(req));
			const step = steps.get(step_id);
			const renewed = step?.state === "claimed" && step.owner === worker_id && step.attempt === attempt;
			res.writeHead(renewed ? 200 : 409, { "content-type": "application/json" });
			res.end(JSON.stringify(renewed ? { renewed: true } : { renewed: false, reason: "fenced" }));
			if (renewed) renewals++;
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/commit") {
		try {
			const { worker_id, step_id, attempt, result, next } = JSON.parse(await readBody(req));
			const step = steps.get(step_id);
			const accepted = step?.state === "claimed" && step.owner === worker_id && step.attempt === attempt;
			if (!accepted) {
				res.writeHead(409, { "content-type": "application/json" });
				res.end(JSON.stringify({ committed: false, reason: "fenced" }));
				return;
			}
			commit(step, result, next);
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ committed: true }));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}

	// ---- blobs ----
	const blobMatch = url.pathname.match(/^\/v1\/blob\/([0-9a-f]{64})$/);
	if (blobMatch) {
		const hash = blobMatch[1];
		if (req.method === "PUT") {
			const body = await readBody(req);
			if (createHash("sha256").update(body).digest("hex") !== hash) {
				res.writeHead(400, { "content-type": "application/json" });
				res.end(JSON.stringify({ error: "hash mismatch" }));
				return;
			}
			blobs.set(hash, body);
			res.writeHead(200).end();
			return;
		}
		if (req.method === "GET") {
			const blob = blobs.get(hash);
			if (blob === undefined) {
				res.writeHead(404).end();
				return;
			}
			res.writeHead(200, { "content-type": "application/octet-stream" });
			res.end(blob);
			return;
		}
	}
	if (req.method === "POST" && url.pathname === "/v1/blobs/missing") {
		try {
			const { hashes } = JSON.parse(await readBody(req));
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ missing: (hashes as string[]).filter((h) => !blobs.has(h)) }));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}

	// ---- session read route ----
	const sessionMatch = url.pathname.match(/^\/v1\/session\/([^/]+)\/steps$/);
	if (req.method === "GET" && sessionMatch) {
		const since = Number(url.searchParams.get("since") ?? 0);
		const ids = order.get(decodeURIComponent(sessionMatch[1])) ?? [];
		const out = ids.slice(since).map((id, i) => {
			const s = steps.get(id)!;
			return {
				index: since + i,
				step_id: s.id,
				kind: s.kind,
				state: s.state,
				spec: s.spec,
				requirements: s.requirements,
				result: s.state === "done" ? s.payload : undefined,
			};
		});
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ steps: out }));
		return;
	}

	// ---- step submission ----
	if (req.method === "POST" && url.pathname === "/v1/step") {
		let step_id: string, session: string, kind: string, spec: any;
		let requirements: Record<string, string>;
		try {
			({ step_id, session, kind, spec, requirements = {} } = JSON.parse(await readBody(req)));
			if (typeof step_id !== "string" || !step_id) throw new Error("missing step_id");
			if (typeof session !== "string" || !session) throw new Error("missing session");
			if (kind !== "llm" && kind !== "tool") throw new Error(`bad kind: ${kind}`);
			if (spec === undefined) throw new Error("missing spec");
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
			return;
		}

		const step = submit(step_id, session, kind, spec, requirements);
		const wait = url.searchParams.get("wait") !== "false";

		if (!wait) {
			res.writeHead(202, { "content-type": "application/json" });
			res.end(JSON.stringify({ step_id }));
			return;
		}
		if (step.state === "done") {
			replyStep(res, step);
			return;
		}
		step.waiters.push((s) => replyStep(res, s));
		return;
	}

	res.writeHead(404).end();
});

server.listen(PORT, "127.0.0.1", () => {
	console.error(`[fakecar] listening on 127.0.0.1:${PORT}`);
});
