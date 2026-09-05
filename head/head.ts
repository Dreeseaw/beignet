// beignet head: a disposable client.
//
//   head.ts start [--session ID] [--model prov/id] [--cwd DIR]
//                 [--require KEY=VALUE] "message"
//       Submits ONE llm step (?wait=false) and exits. The cluster runs the
//       rest of the turn whether or not anything is watching.
//
//   head.ts watch --session ID [--follow]
//       Renders a session from the ledger. Read-only, no lease, any node.
//
// Turn 2+ inherits model/systemPrompt/tools from the session's last llm step,
// so an attaching head needs no out-of-band configuration.
import { getModel } from "@earendil-works/pi-ai/compat";
import {
	createBashToolDefinition,
	createFindToolDefinition,
	createGrepToolDefinition,
	createLsToolDefinition,
	createReadToolDefinition,
} from "@earendil-works/pi-coding-agent";
import { BlobClient, newStepId } from "../executor/blobs.ts";
import { buildLlmSpec, stage, type ContextRefs } from "../executor/chain.ts";

const SIDECAR = process.env.BEIGNET_SIDECAR_URL ?? "http://127.0.0.1:4700";
const blobs = new BlobClient(SIDECAR);

const SYSTEM_PROMPT = [
	"You are a coding agent running on a distributed cluster.",
	"Use the provided tools to inspect and act on the machine.",
	"Be concise. When the task is done, reply with a short summary and no tool calls.",
].join(" ");

const VALUE_FLAGS = new Set(["session", "model", "cwd"]);

/** argv minus the subcommand, split into flags and positionals. */
function parseArgs(argv: string[]) {
	const flags: Record<string, string> = {};
	const positional: string[] = [];
	const requirementFlags: string[] = [];
	for (let i = 0; i < argv.length; i++) {
		const arg = argv[i];
		if (arg.startsWith("--")) {
			const name = arg.slice(2);
			if (name === "require") requirementFlags.push(argv[++i] ?? "");
			else if (VALUE_FLAGS.has(name)) flags[name] = argv[++i] ?? "";
			else flags[name] = "true";
		} else positional.push(arg);
	}
	return { flags, positional, requirementFlags };
}

const { flags, positional, requirementFlags } = parseArgs(process.argv.slice(3));

function flag(name: string, fallback?: string): string | undefined {
	return flags[name] ?? fallback;
}

function toolDefs(cwd: string) {
	return [
		createBashToolDefinition(cwd),
		createReadToolDefinition(cwd),
		createGrepToolDefinition(cwd),
		createFindToolDefinition(cwd),
		createLsToolDefinition(cwd),
	].map((d: any) => ({ name: d.name, description: d.description, parameters: d.parameters }));
}

function requirements(): Record<string, string> {
	return Object.fromEntries(requirementFlags.map((requirement) => {
		const separator = requirement.indexOf("=");
		if (separator < 1) throw new Error(`invalid --require ${JSON.stringify(requirement)}; expected KEY=VALUE`);
		return [requirement.slice(0, separator), requirement.slice(separator + 1)];
	}));
}

async function fetchSteps(session: string, since = 0): Promise<any[]> {
	const res = await fetch(
		`${SIDECAR}/v1/session/${encodeURIComponent(session)}/steps?since=${since}`,
	);
	if (!res.ok) throw new Error(`session read HTTP ${res.status}`);
	return ((await res.json()) as any).steps ?? [];
}

async function start() {
	const message = positional.join(" ").trim();
	if (!message) {
		console.error('usage: head.ts start [--session ID] [--model prov/id] [--cwd DIR] [--require KEY=VALUE] "message"');
		process.exit(2);
	}
	let required: Record<string, string>;
	try {
		required = requirements();
	} catch (error: any) {
		console.error(error?.message ?? error);
		process.exit(2);
	}
	const session = flag("session") ?? `sess-${crypto.randomUUID().slice(0, 8)}`;
	const cwd = flag("cwd", process.cwd())!;
	const staged = new Map<string, unknown>();
	const userMessage = { role: "user", content: [{ type: "text", text: message }], timestamp: Date.now() };

	// Inherit from the session's most recent llm step, if any.
	const prior = await fetchSteps(session).catch(() => []);
	const lastLlm = [...prior].reverse().find((s) => s.kind === "llm" && s.result);

	let model: any;
	let context: ContextRefs;
	let options: any;
	if (lastLlm) {
		model = lastLlm.spec.model;
		options = lastLlm.spec.options ?? {};
		context = {
			...lastLlm.spec.context,
			messages: [
				...lastLlm.spec.context.messages,
				stage(staged, lastLlm.result), // the turn's final assistant message
				stage(staged, userMessage),
			],
		};
	} else {
		const modelRef = flag("model", process.env.BEIGNET_MODEL ?? "anthropic/claude-haiku-4-5")!;
		const slash = modelRef.indexOf("/");
		model = getModel(modelRef.slice(0, slash) as any, modelRef.slice(slash + 1) as any);
		options = {};
		context = {
			systemPrompt: stage(staged, SYSTEM_PROMPT),
			messages: [stage(staged, userMessage)],
			tools: stage(staged, toolDefs(cwd)),
		};
	}

	await blobs.putMissing(staged);
	const spec = buildLlmSpec(model, context, options, cwd);
	const stepId = newStepId();
	const res = await fetch(`${SIDECAR}/v1/step?wait=false`, {
		method: "POST",
		headers: { "content-type": "application/json" },
		body: JSON.stringify({ step_id: stepId, session, kind: "llm", spec, requirements: required }),
	});
	if (res.status !== 202 && res.status !== 200) {
		console.error(`submit failed: HTTP ${res.status} ${await res.text()}`);
		process.exit(1);
	}
	console.log(`session ${session}`);
	console.log(`step    ${stepId.slice(0, 12)} submitted — detaching; the cluster owns the turn`);
	console.log(`watch:  node head.ts watch --session ${session} --follow`);
}

function render(step: any) {
	const tag = `[${String(step.index).padStart(2, "0")}] ${step.kind}`;
	if (step.kind === "tool") {
		const args = step.spec.args?.command ?? step.spec.args?.path ?? JSON.stringify(step.spec.args ?? {});
		console.log(`${tag}:${step.spec.tool} ${String(args).slice(0, 100)}`);
		if (step.result?.toolError) console.log(`     ! ${step.result.toolError}`);
		else if (step.result?.content) {
			for (const c of step.result.content) {
				if (c.type === "text") console.log(`     ${c.text.trim().split("\n").join("\n     ").slice(0, 400)}`);
			}
		} else console.log(`     (${step.state})`);
		return;
	}
	if (!step.result) {
		console.log(`${tag}: (${step.state})`);
		return;
	}
	for (const c of step.result.content ?? []) {
		if (c.type === "text" && c.text.trim()) console.log(`${tag}: ${c.text.trim()}`);
		if (c.type === "toolCall") console.log(`${tag}: → calls ${c.name}`);
	}
	if (step.result.stopReason && step.result.stopReason !== "toolUse") {
		console.log(`     (turn ended: ${step.result.stopReason})`);
	}
}

async function watch() {
	const session = flag("session");
	if (!session) {
		console.error("usage: head.ts watch --session ID [--follow]");
		process.exit(2);
	}
	const follow = flags.follow === "true";
	let since = 0;
	for (;;) {
		const steps = await fetchSteps(session, since);
		for (const step of steps) {
			if (step.state === "done" || !follow) render(step);
		}
		const done = steps.filter((s: any) => s.state === "done");
		since += done.length;
		if (!follow) return;
		const last = steps[steps.length - 1];
		const ended =
			last?.state === "done" &&
			last.kind === "llm" &&
			last.result?.stopReason !== "toolUse" &&
			since === steps.length + (since - done.length);
		if (ended) return;
		await new Promise((r) => setTimeout(r, 500));
	}
}

const command = process.argv[2];
if (command === "start") await start();
else if (command === "watch") await watch();
else {
	console.error("usage: head.ts <start|watch> …");
	process.exit(2);
}
