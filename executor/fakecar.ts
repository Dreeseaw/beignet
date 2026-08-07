// Fake sidecar: the beignet wire contract with an in-memory ledger.
// Stand-in for the Go sidecar during development, and the test double for CI.
// Mirrors what the FSM must do — notably: a step's result and its `next`
// commit ATOMICALLY, and the node that commits immediately picks up `next`
// (turn affinity). Usage: node fakecar.ts
import http from "node:http";
import { createHash } from "node:crypto";

const PORT = Number(process.env.BEIGNET_SIDECAR_PORT ?? 4700);
const EXECUTOR = process.env.BEIGNET_EXECUTOR_URL ?? "http://127.0.0.1:4701";

type Step = {
	id: string;
	session: string;
	kind: string;
	spec: any;
	state: "pending" | "done";
	ok?: boolean;
	payload?: any;
	waiters: Array<(step: Step) => void>;
};

const steps = new Map<string, Step>();
const order = new Map<string, string[]>(); // session -> step ids, submission order
const blobs = new Map<string, string>();
const inFlight = new Set<string>();

function submit(id: string, session: string, kind: string, spec: any): Step {
	const existing = steps.get(id);
	if (existing) return existing; // dedup: SubmitStep is a no-op if id exists
	const step: Step = { id, session, kind, spec, state: "pending", waiters: [] };
	steps.set(id, step);
	if (!order.has(session)) order.set(session, []);
	order.get(session)!.push(id);
	console.error(
		`[fakecar] ${id.slice(0, 12)} submit ${kind}${kind === "tool" ? `:${spec.tool}` : ""} (session ${session.slice(0, 8)})`,
	);
	return step;
}

/** Apply(CommitResult): mark Done and insert `next` in one transition. */
function commit(step: Step, ok: boolean, payload: any, next?: any): Step | undefined {
	if (step.state === "done") return undefined; // Duplicate
	step.state = "done";
	step.ok = ok;
	step.payload = payload;
	let successor: Step | undefined;
	if (ok && next) successor = submit(next.step_id, next.session ?? step.session, next.kind, next.spec);
	console.error(`[fakecar] ${step.id.slice(0, 12)} committed ok=${ok}${next ? " +next" : ""}`);
	for (const waiter of step.waiters) waiter(step);
	step.waiters = [];
	return successor;
}

/** Worker: execute a pending step, commit, then follow the chain (affinity). */
async function work(id: string): Promise<void> {
	let current: string | undefined = id;
	while (current) {
		const step = steps.get(current);
		if (!step || step.state === "done" || inFlight.has(step.id)) return;
		inFlight.add(step.id);
		let successor: Step | undefined;
		try {
			const res = await fetch(`${EXECUTOR}/v1/execute`, {
				method: "POST",
				headers: { "content-type": "application/json" },
				body: JSON.stringify({ kind: step.kind, spec: step.spec, session: step.session }),
			});
			const body: any = await res.json().catch(() => ({}));
			if (!res.ok) {
				// Infra failure: nothing commits, step stays pending, callers may retry.
				console.error(`[fakecar] ${step.id.slice(0, 12)} execute failed: ${body.error}`);
				for (const waiter of step.waiters) waiter({ ...step, ok: false, payload: body.error });
				step.waiters = [];
				return;
			}
			successor = commit(step, true, body.result, body.next);
		} catch (e: any) {
			console.error(`[fakecar] ${step.id.slice(0, 12)} execute error: ${e?.message ?? e}`);
			for (const waiter of step.waiters) waiter({ ...step, ok: false, payload: e?.message ?? String(e) });
			step.waiters = [];
			return;
		} finally {
			inFlight.delete(step.id);
		}
		current = successor?.id;
	}
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
	if (step.ok) {
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ result: step.payload }));
	} else {
		res.writeHead(502, { "content-type": "application/json" });
		res.end(JSON.stringify({ error: step.payload }));
	}
}

const server = http.createServer(async (req, res) => {
	const url = new URL(req.url ?? "/", "http://localhost");

	if (req.method === "GET" && url.pathname === "/healthz") {
		res.writeHead(200).end("ok");
		return;
	}
	if (req.method === "GET" && url.pathname === "/v1/debug/stats") {
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ steps: steps.size, blobs: blobs.size, sessions: order.size }));
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
				result: s.state === "done" && s.ok ? s.payload : undefined,
			};
		});
		res.writeHead(200, { "content-type": "application/json" });
		res.end(JSON.stringify({ steps: out }));
		return;
	}

	// ---- step submission ----
	if (req.method === "POST" && url.pathname === "/v1/step") {
		let step_id: string, session: string, kind: string, spec: any;
		try {
			({ step_id, session, kind, spec } = JSON.parse(await readBody(req)));
			if (typeof step_id !== "string" || !step_id) throw new Error("missing step_id");
			if (typeof session !== "string" || !session) throw new Error("missing session");
			if (kind !== "llm" && kind !== "tool") throw new Error(`bad kind: ${kind}`);
			if (spec === undefined) throw new Error("missing spec");
		} catch (e: any) {
			res.writeHead(400, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: `bad request: ${e?.message ?? e}` }));
			return;
		}

		const step = submit(step_id, session, kind, spec);
		const wait = url.searchParams.get("wait") !== "false";

		if (!wait) {
			res.writeHead(202, { "content-type": "application/json" });
			res.end(JSON.stringify({ step_id }));
			void work(step_id); // turn advances with no client attached
			return;
		}
		if (step.state === "done") {
			replyStep(res, step);
			return;
		}
		step.waiters.push((s) => replyStep(res, s));
		void work(step_id);
		return;
	}

	res.writeHead(404).end();
});

server.listen(PORT, "127.0.0.1", () => {
	console.error(`[fakecar] listening on 127.0.0.1:${PORT} (executor ${EXECUTOR})`);
});
