// beignet executor: runs one step and returns {result, next}.
// Pure function from the sidecar's perspective — all agent semantics live
// here (providers, tools, turn chaining); none live in Go.
import http from "node:http";
import { readFileSync } from "node:fs";
import { pathToFileURL } from "node:url";
import {
	createBashTool,
	createFindTool,
	createGrepTool,
	createLsTool,
	createReadTool,
	ModelRuntime,
} from "@earendil-works/pi-coding-agent";
import { BlobClient } from "./blobs.ts";
import { computeNext } from "./chain.ts";
import { rebuildContext } from "./context.ts";

const PORT = Number(process.env.BEIGNET_EXECUTOR_PORT ?? 4701);
const SIDECAR = process.env.BEIGNET_SIDECAR_URL ?? "http://127.0.0.1:4700";
const LLM_TIMEOUT_MS = 600_000;
const TOOL_TIMEOUT_MS = 300_000;

function timedSignal(timeoutMs: number, signal?: AbortSignal): AbortSignal {
	const timeout = AbortSignal.timeout(timeoutMs);
	return signal ? AbortSignal.any([signal, timeout]) : timeout;
}

const blobs = new BlobClient(SIDECAR);

// pi's model runtime owns auth: stored API keys AND OAuth subscription
// logins, with refresh. Reusing it is the whole point of executing in TS.
const models = await ModelRuntime.create();

// Test mode: canned assistant messages instead of provider calls, so the real
// chainer can be exercised end to end without spending tokens.
const fakeLlm: any[] = process.env.BEIGNET_FAKE_LLM
	? JSON.parse(readFileSync(process.env.BEIGNET_FAKE_LLM, "utf8"))
	: [];

const toolFactories: Record<string, (cwd: string) => any> = {
	bash: createBashTool,
	read: createReadTool,
	grep: createGrepTool,
	find: createFindTool,
	ls: createLsTool,
};

async function runStep(kind: string, spec: any, signal?: AbortSignal): Promise<any> {
	signal?.throwIfAborted();
	if (kind === "llm") {
		const { model, context: refs, options } = spec;
		const context = await rebuildContext(refs, (hash) => blobs.get(hash, signal));
		if (fakeLlm.length > 0) {
			const priorReplies = context.messages.filter((message: any) => message?.role === "assistant").length;
			return fakeLlm[priorReplies % fakeLlm.length];
		}
		// Prefer the node's configured model (correct baseUrl + auth wiring);
		// the spec's embedded copy is the fallback for unknown providers.
		const resolved = models.getModel(model.provider, model.id) ?? model;
		return await models.completeSimple(resolved, context, {
			...(options ?? {}),
			signal: timedSignal(LLM_TIMEOUT_MS, signal),
		});
	}
	if (kind === "tool") {
		const factory = toolFactories[spec.tool];
		if (!factory) throw new Error(`unknown tool: ${spec.tool}`);
		const tool = factory(spec.cwd ?? process.cwd());
		try {
			return await tool.execute(
				`beignet-${crypto.randomUUID().slice(0, 8)}`,
				spec.args,
				timedSignal(TOOL_TIMEOUT_MS, signal),
			);
		} catch (e: any) {
			if (signal?.aborted) signal.throwIfAborted();
			// pi tools throw on failure (e.g. nonzero exit); that's a committed
			// outcome, not an infra error — 5xx stays reserved for retryable.
			return { toolError: e?.message ?? String(e) };
		}
	}
	throw new Error(`unknown step kind: ${kind}`);
}

export async function executeStep(kind: string, spec: any, session = "", signal?: AbortSignal) {
	const result = await runStep(kind, spec, signal);
	signal?.throwIfAborted();
	// Referenced blobs land before the control plane can commit the successor.
	const next = await computeNext(session, kind, spec, result, blobs, signal);
	signal?.throwIfAborted();
	console.error(
		`[executor] ${kind}${kind === "tool" ? `:${spec.tool}` : ""} ok` +
			(next ? ` → next ${next.kind} ${next.step_id.slice(0, 12)}` : " → turn end"),
	);
	return next ? { result, next } : { result };
}

function readBody(req: http.IncomingMessage): Promise<string> {
	return new Promise((resolve, reject) => {
		let data = "";
		req.on("data", (c) => (data += c));
		req.on("end", () => resolve(data));
		req.on("error", reject);
	});
}

const server = http.createServer(async (req, res) => {
	if (req.method === "GET" && req.url === "/healthz") {
		res.writeHead(200).end("ok");
		return;
	}
	if (req.method === "POST" && req.url === "/v1/execute") {
		try {
			const { kind, spec, session } = JSON.parse(await readBody(req));
			const output = await executeStep(kind, spec, session);
			res.writeHead(200, { "content-type": "application/json" });
			res.end(JSON.stringify(output));
		} catch (e: any) {
			console.error(`[executor] error: ${e?.message ?? e}`);
			res.writeHead(500, { "content-type": "application/json" });
			res.end(JSON.stringify({ error: e?.message ?? String(e) }));
		}
		return;
	}
	res.writeHead(404).end();
});

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	server.listen(PORT, "127.0.0.1", () => {
		console.error(`[executor] listening on 127.0.0.1:${PORT} (sidecar ${SIDECAR})`);
	});
}
