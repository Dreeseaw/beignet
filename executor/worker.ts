// Pull worker: claims one step at a time, executes it locally, and submits a
// fenced result. It never participates in Raft.
import { hostname } from "node:os";
import { pathToFileURL } from "node:url";
import { executeStep } from "./executor.ts";

const SIDECAR = process.env.BEIGNET_SIDECAR_URL ?? "http://127.0.0.1:4700";
const WORKER_ID = process.env.BEIGNET_WORKER_ID ?? `${hostname()}-${crypto.randomUUID().slice(0, 8)}`;
const POLL_INTERVAL_MS = 250;
const RETRY_INTERVAL_MS = 500;
const REQUEST_TIMEOUT_MS = 5_000;
const RENEW_INTERVAL_MS = Number(process.env.BEIGNET_RENEW_INTERVAL_MS ?? 10_000);

type Claim = {
	step_id: string;
	session: string;
	kind: string;
	spec: any;
	attempt: number;
};

function workerLabels(): Record<string, string> {
	const value = JSON.parse(process.env.BEIGNET_WORKER_LABELS ?? "{}");
	if (value === null || Array.isArray(value) || typeof value !== "object") {
		throw new Error("BEIGNET_WORKER_LABELS must be a JSON object");
	}
	if (Object.values(value).some((label) => typeof label !== "string")) {
		throw new Error("BEIGNET_WORKER_LABELS values must be strings");
	}
	return value as Record<string, string>;
}

const labels = workerLabels();

function delay(ms: number, signal?: AbortSignal): Promise<void> {
	return new Promise((resolve) => {
		if (signal?.aborted) return resolve();
		const timer = setTimeout(done, ms);
		function done() {
			clearTimeout(timer);
			signal?.removeEventListener("abort", done);
			resolve();
		}
		signal?.addEventListener("abort", done, { once: true });
	});
}

async function post(path: string, body: unknown, signal?: AbortSignal): Promise<Response> {
	const timeout = AbortSignal.timeout(REQUEST_TIMEOUT_MS);
	return await fetch(`${SIDECAR}${path}`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify(body),
		signal: signal ? AbortSignal.any([signal, timeout]) : timeout,
	});
}

async function claim(signal: AbortSignal): Promise<Claim | null> {
	const res = await post("/v1/work/claim", { worker_id: WORKER_ID, labels }, signal);
	if (res.status === 204) return null;
	if (!res.ok) throw new Error(`claim HTTP ${res.status}: ${await res.text()}`);
	return await res.json() as Claim;
}

async function renewUntilStopped(work: Claim, signal: AbortSignal): Promise<boolean> {
	while (!signal.aborted) {
		await delay(RENEW_INTERVAL_MS, signal);
		if (signal.aborted) break;
		try {
			const res = await post("/v1/work/renew", {
				worker_id: WORKER_ID,
				step_id: work.step_id,
				attempt: work.attempt,
			}, signal);
			if (res.status === 409) return false;
			if (!res.ok) throw new Error(`HTTP ${res.status}: ${await res.text()}`);
		} catch (error: any) {
			if (signal.aborted) break;
			console.error(`[worker ${WORKER_ID}] renew ${work.step_id.slice(0, 12)} failed: ${error?.message ?? error}`);
		}
	}
	return true;
}

async function runClaim(work: Claim, signal: AbortSignal): Promise<void> {
	console.error(`[worker ${WORKER_ID}] claimed ${work.kind} ${work.step_id.slice(0, 12)} attempt ${work.attempt}`);
	const renewals = new AbortController();
	const stopRenewing = () => renewals.abort(signal.reason);
	if (signal.aborted) stopRenewing();
	else signal.addEventListener("abort", stopRenewing, { once: true });
	const stillOwned = renewUntilStopped(work, renewals.signal);
	try {
		let output: Awaited<ReturnType<typeof executeStep>>;
		try {
			output = await executeStep(work.kind, work.spec, work.session, signal);
		} catch (error: any) {
			if (signal.aborted) return;
			throw new Error(`execute ${work.step_id.slice(0, 12)} failed: ${error?.message ?? error}`);
		}
		if (signal.aborted) return;

		renewals.abort();
		if (!await stillOwned) {
			console.error(`[worker ${WORKER_ID}] lease lost for ${work.step_id.slice(0, 12)}; discarding result`);
			return;
		}
		const res = await post("/v1/work/commit", {
			worker_id: WORKER_ID,
			step_id: work.step_id,
			attempt: work.attempt,
			result: output.result,
			next: output.next,
		}, signal);
		if (res.status === 409) {
			console.error(`[worker ${WORKER_ID}] commit fenced for ${work.step_id.slice(0, 12)}`);
			return;
		}
		if (!res.ok) throw new Error(`commit HTTP ${res.status}: ${await res.text()}`);
	} finally {
		signal.removeEventListener("abort", stopRenewing);
		renewals.abort();
		await stillOwned;
	}
}

export async function runWorker(signal: AbortSignal): Promise<void> {
	console.error(`[worker ${WORKER_ID}] polling ${SIDECAR} labels=${JSON.stringify(labels)}`);
	while (!signal.aborted) {
		try {
			const work = await claim(signal);
			if (work) await runClaim(work, signal);
			else await delay(POLL_INTERVAL_MS, signal);
		} catch (error: any) {
			if (signal.aborted) break;
			console.error(`[worker ${WORKER_ID}] ${error?.message ?? error}`);
			await delay(RETRY_INTERVAL_MS, signal);
		}
	}
}

if (process.argv[1] && import.meta.url === pathToFileURL(process.argv[1]).href) {
	if (!Number.isFinite(RENEW_INTERVAL_MS) || RENEW_INTERVAL_MS <= 0) {
		throw new Error("BEIGNET_RENEW_INTERVAL_MS must be a positive number");
	}
	const stop = new AbortController();
	process.once("SIGINT", () => stop.abort());
	process.once("SIGTERM", () => stop.abort());
	await runWorker(stop.signal);
}
