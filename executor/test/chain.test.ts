import { test } from "node:test";
import assert from "node:assert/strict";
import { computeNext, buildLlmSpec, type ContextRefs } from "../chain.ts";
import { hashOf } from "../blobs.ts";

// Stub blob store: records what chaining uploads, serves it back.
function stubBlobs() {
	const store = new Map<string, unknown>();
	return {
		store,
		client: {
			async put(value: unknown) {
				const h = hashOf(value);
				store.set(h, value);
				return h;
			},
			async get(hash: string) {
				return JSON.stringify(store.get(hash));
			},
		} as any,
	};
}

const context: ContextRefs = { systemPrompt: "h-sys", messages: ["h-user"], tools: "h-tools" };
const llmSpec = { ...buildLlmSpec({ id: "m" }, context, {}), cwd: "/work" };

function assistant(content: any[], stopReason = "toolUse") {
	return { role: "assistant", content, stopReason, timestamp: 1 };
}

test("llm result with no tool calls ends the turn", async () => {
	const { client } = stubBlobs();
	const next = await computeNext(
		"s1", "llm", llmSpec,
		assistant([{ type: "text", text: "all done" }], "stop"),
		client,
	);
	assert.equal(next, null);
});

test("llm result with tool calls chains to the first tool step", async () => {
	const { client, store } = stubBlobs();
	const msg = assistant([
		{ type: "text", text: "working" },
		{ type: "toolCall", id: "c1", name: "bash", arguments: { command: "echo a" } },
		{ type: "toolCall", id: "c2", name: "bash", arguments: { command: "echo b" } },
	]);
	const next = await computeNext("s1", "llm", llmSpec, msg, client);

	assert.equal(next?.kind, "tool");
	assert.equal(next?.session, "s1");
	assert.equal(next?.spec.tool, "bash");
	assert.equal(next?.spec.args.command, "echo a");
	assert.equal(next?.spec.cwd, "/work");
	assert.equal(next?.spec.chain.index, 0);
	assert.equal(next?.spec.chain.calls.length, 2, "both sibling calls carried");
	assert.deepEqual(next?.spec.chain.results, []);
	// The assistant message was uploaded so the next llm step can reference it.
	assert.equal(store.get(next!.spec.chain.assistant), msg);
});

test("tool step chains to its sibling, then to the next llm step", async () => {
	const { client } = stubBlobs();
	const msg = assistant([
		{ type: "toolCall", id: "c1", name: "bash", arguments: { command: "echo a" } },
		{ type: "toolCall", id: "c2", name: "bash", arguments: { command: "echo b" } },
	]);
	const first = await computeNext("s1", "llm", llmSpec, msg, client);

	const second = await computeNext(
		"s1", "tool", first!.spec,
		{ content: [{ type: "text", text: "a\n" }] },
		client,
	);
	assert.equal(second?.kind, "tool", "second sibling runs before any llm step");
	assert.equal(second?.spec.args.command, "echo b");
	assert.equal(second?.spec.chain.index, 1);
	assert.equal(second?.spec.chain.results.length, 1, "first result accumulated");

	const third = await computeNext(
		"s1", "tool", second!.spec,
		{ content: [{ type: "text", text: "b\n" }] },
		client,
	);
	assert.equal(third?.kind, "llm", "last sibling chains to the next llm step");
	assert.deepEqual(third?.spec.model, { id: "m" });
	assert.equal(third?.spec.cwd, "/work", "the next model call keeps the turn's workspace");
	assert.equal(third?.spec.context.systemPrompt, "h-sys");
	assert.equal(third?.spec.context.tools, "h-tools");
	assert.equal(
		third?.spec.context.messages.length,
		4,
		"original user msg + assistant + 2 tool results",
	);
	assert.equal(third?.spec.context.messages[1], first!.spec.chain.assistant);
});

test("tool failure still chains, recorded as an error result message", async () => {
	const { client, store } = stubBlobs();
	const msg = assistant([{ type: "toolCall", id: "c1", name: "bash", arguments: { command: "exit 3" } }]);
	const toolStep = await computeNext("s1", "llm", llmSpec, msg, client);
	const next = await computeNext(
		"s1", "tool", toolStep!.spec,
		{ toolError: "Command exited with code 3" },
		client,
	);
	assert.equal(next?.kind, "llm", "the model gets to see the failure and react");
	const resultRef = next!.spec.context.messages.at(-1) as string;
	const resultMsg = store.get(resultRef) as any;
	assert.equal(resultMsg.isError, true);
	assert.equal(resultMsg.toolCallId, "c1");
	assert.match(resultMsg.content[0].text, /code 3/);
});

test("standalone tool step (no chain metadata) ends the chain", async () => {
	const { client } = stubBlobs();
	const next = await computeNext(
		"s1", "tool",
		{ tool: "bash", args: { command: "echo hi" }, cwd: "/tmp" },
		{ content: [{ type: "text", text: "hi\n" }] },
		client,
	);
	assert.equal(next, null);
});

test("step ids identify logical invocations, not identical input content", async () => {
	const { client } = stubBlobs();
	const msg = assistant([{ type: "toolCall", id: "c1", name: "bash", arguments: { command: "echo a" } }]);
	const a = await computeNext("s1", "llm", llmSpec, msg, client);
	const b = await computeNext("s1", "llm", llmSpec, msg, client);
	assert.notEqual(a?.step_id, b?.step_id, "two requested samples must remain independent");
});
