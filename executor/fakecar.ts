// Fake sidecar: the beignet wire contract with an in-memory ledger.
// Stand-in for the Go sidecar during development, and the test double for CI.
// Mirrors the FSM's atomic commit + successor insertion and fenced pull work.
import http from "node:http";
import { createHash } from "node:crypto";

const PORT = Number(process.env.BEIGNET_SIDECAR_PORT ?? 4700);
const MAX_BATCH = 256;

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
const blobs = new Map<string, Buffer>();
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
		`[fakecar] ${id.slice(0, 12)} submit ${kind}${kind === "tool" ? `:${spec?.tool}` : ""} (session ${session.slice(0, 8)})`,
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

function stringMap(value: any): value is Record<string, string> {
	return value !== null && !Array.isArray(value) && typeof value === "object" &&
		Object.values(value).every((entry) => typeof entry === "string");
}

function validNext(next: any): boolean {
	return next == null || (typeof next.step_id === "string" && next.step_id &&
		typeof next.session === "string" && next.session &&
		(next.kind === "llm" || next.kind === "tool") && next.spec !== undefined);
}

function boundedItems(value: any, field: string): any[] {
	const items = value?.[field];
	if (!Array.isArray(items) || items.length === 0 || items.length > MAX_BATCH) {
		throw new Error(`${field} must contain 1-${MAX_BATCH} items`);
	}
	return items;
}

function parseStep(value: any) {
	const { step_id, session, kind, spec, requirements = {} } = value ?? {};
	if (typeof step_id !== "string" || !step_id) throw new Error("missing step_id");
	if (typeof session !== "string" || !session) throw new Error("missing session");
	if (kind !== "llm" && kind !== "tool") throw new Error(`bad kind: ${kind}`);
	if (spec === undefined) throw new Error("missing spec");
	if (!stringMap(requirements)) throw new Error("requirements must be a string map");
	return { step_id, session, kind, spec, requirements };
}

function parseClaim(value: any) {
	const { worker_id, labels = {} } = value ?? {};
	if (typeof worker_id !== "string" || !worker_id) throw new Error("missing worker_id");
	if (!stringMap(labels)) throw new Error("labels must be a string map");
	return { worker_id, labels };
}

function parseRenewal(value: any) {
	const { worker_id, step_id, attempt } = value ?? {};
	if (typeof worker_id !== "string" || !worker_id || typeof step_id !== "string" || !step_id) {
		throw new Error("missing worker_id or step_id");
	}
	return { worker_id, step_id, attempt };
}

function parseCommit(value: any) {
	const { worker_id, step_id, attempt, result, next } = value ?? {};
	if (typeof worker_id !== "string" || !worker_id || typeof step_id !== "string" || !step_id || result === undefined) {
		throw new Error("missing worker_id, step_id or result");
	}
	if (!validNext(next)) throw new Error("invalid next step");
	return { worker_id, step_id, attempt, result, next };
}

function findWork(worker: string, labels: Record<string, string>, excluded = new Set<string>()): Step | undefined {
	let pending: Step | undefined;
	for (const step of steps.values()) {
		if (excluded.has(step.id)) continue;
		if (!labelsMatch(step.requirements, labels)) continue;
		if (step.state === "claimed" && step.owner === worker) return step;
		if (!pending && step.state === "pending") pending = step;
	}
	return pending;
}

function claim(worker: string, labels: Record<string, string>, excluded = new Set<string>()) {
	const step = findWork(worker, labels, excluded);
	if (!step) return undefined;
	step.state = "claimed";
	step.owner = worker;
	return {
		worker_id: worker, step_id: step.id, session: step.session, kind: step.kind, spec: step.spec,
		requirements: step.requirements, attempt: step.attempt,
	};
}

function renew({ worker_id, step_id, attempt }: ReturnType<typeof parseRenewal>) {
	const step = steps.get(step_id);
	const renewed = step?.state === "claimed" && step.owner === worker_id && step.attempt === attempt;
	if (renewed) renewals++;
	return renewed ? { renewed: true } : { renewed: false, reason: "fenced" };
}

function commitResult({ worker_id, step_id, attempt, result, next }: ReturnType<typeof parseCommit>) {
	const step = steps.get(step_id);
	if (!step) return { committed: false, reason: "unknown step" };
	if (step.state === "done") return { committed: false, reason: "duplicate" };
	if (step.state !== "claimed" || step.owner !== worker_id || step.attempt !== attempt) {
		return { committed: false, reason: "fenced" };
	}
	if (next && steps.has(next.step_id)) return { committed: false, reason: "next step exists" };
	commit(step, result, next);
	return { committed: true };
}

function readBody(req: http.IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		let data = "";
		req.on("data", (c) => (data += c));
		req.on("end", () => resolve(data));
		req.on("error", reject);
	});
}

function readBytes(req: http.IncomingMessage): Promise<Buffer> {
	return new Promise((resolve, reject) => {
		const chunks: Buffer[] = [];
		req.on("data", (chunk) => chunks.push(Buffer.from(chunk)));
		req.on("end", () => resolve(Buffer.concat(chunks)));
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
			const request = parseClaim(JSON.parse(await readBody(req)));
			const claimed = claim(request.worker_id, request.labels);
			if (!claimed) {
				res.writeHead(204).end();
				return;
			}
			res.writeHead(200, { "content-type": "application/json" });
			const { worker_id: _, ...response } = claimed;
			res.end(JSON.stringify(response));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/claim-batch") {
		try {
			const requests = boundedItems(JSON.parse(await readBody(req)), "claims").map(parseClaim);
			if (new Set(requests.map((request) => request.worker_id)).size !== requests.length) {
				throw new Error("worker_id must be unique within a batch");
			}
			const excluded = new Set<string>();
			const claims = requests.flatMap((request) => {
				const claimed = claim(request.worker_id, request.labels, excluded);
				if (!claimed) return [];
				excluded.add(claimed.step_id);
				return [claimed];
			});
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ claims }));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/renew") {
		try {
			const result = renew(parseRenewal(JSON.parse(await readBody(req))));
			res.writeHead(result.renewed ? 200 : 409, { "content-type": "application/json" });
			res.end(JSON.stringify(result));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/renew-batch") {
		try {
			const requests = boundedItems(JSON.parse(await readBody(req)), "renewals").map(parseRenewal);
			const results = requests.map((request) => ({ step_id: request.step_id, ...renew(request) }));
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ results }));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/commit") {
		try {
			const result = commitResult(parseCommit(JSON.parse(await readBody(req))));
			res.writeHead(result.committed ? 200 : 409, { "content-type": "application/json" });
			res.end(JSON.stringify(result));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/work/commit-batch") {
		try {
			const requests = boundedItems(JSON.parse(await readBody(req)), "commits").map(parseCommit);
			const results = requests.map((request) => ({ step_id: request.step_id, ...commitResult(request) }));
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify({ results }));
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
			const body = await readBytes(req);
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

	// ---- session read routes ----
	const summaryMatch = url.pathname.match(/^\/v1\/session\/([^/]+)\/summary$/);
	if (req.method === "GET" && summaryMatch) {
		const ids = order.get(decodeURIComponent(summaryMatch[1])) ?? [];
		const summary = { observed: ids.length, pending: 0, claimed: 0, done: 0 };
		for (const id of ids) summary[steps.get(id)!.state]++;
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify(summary));
		return;
	}
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
	if (req.method === "POST" && url.pathname === "/v1/steps") {
		try {
			const requests = boundedItems(JSON.parse(await readBody(req)), "steps").map(parseStep);
			for (const request of requests) {
				submit(request.step_id, request.session, request.kind, request.spec, request.requirements);
			}
			res.writeHead(202, { "content-type": "application/json" });
			res.end(JSON.stringify({ accepted: requests.length }));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}
	if (req.method === "POST" && url.pathname === "/v1/step") {
		try {
			const request = parseStep(JSON.parse(await readBody(req)));
			const step = submit(request.step_id, request.session, request.kind, request.spec, request.requirements);
			const wait = url.searchParams.get("wait") !== "false";

			if (!wait) {
				res.writeHead(202, { "content-type": "application/json" });
				res.end(JSON.stringify({ step_id: request.step_id }));
				return;
			}
			if (step.state === "done") {
				replyStep(res, step);
				return;
			}
			step.waiters.push((finished) => replyStep(res, finished));
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
		}
		return;
	}

	res.writeHead(404).end();
});

server.listen(PORT, "127.0.0.1", () => {
	console.error(`[fakecar] listening on 127.0.0.1:${PORT}`);
});
