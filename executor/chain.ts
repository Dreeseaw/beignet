// Turn chaining. The executor is a pure function step -> {result, next};
// this file computes `next`. At most ONE successor: a turn is a linked list.
//
//   llm result with toolCalls  -> first tool step
//   tool result, more siblings -> next tool step
//   tool result, last sibling  -> next llm step (context grown by the
//                                 assistant message + all tool results)
//   llm result, stopReason stop/length/error -> null (turn over)
import { hashOf, newStepId, type BlobClient } from "./blobs.ts";

export interface ContextRefs {
	systemPrompt: string | null;
	messages: string[];
	tools: string | null;
}

export interface ToolCallSpec {
	toolCallId: string;
	tool: string;
	args: any;
}

/** Everything a tool step needs to continue the turn after it commits. */
export interface ChainMeta {
	model: any;
	options: any;
	context: ContextRefs;
	assistant: string;
	calls: ToolCallSpec[];
	index: number;
	results: string[];
	cwd: string;
}

export interface StepRef {
	step_id: string;
	session: string;
	kind: "llm" | "tool";
	spec: any;
}

/** Single builder so head-built and chain-built llm specs hash identically. */
export function buildLlmSpec(
	model: any,
	context: ContextRefs,
	options: any = {},
	cwd?: string,
) {
	return cwd === undefined ? { model, context, options } : { model, context, options, cwd };
}

export function buildToolSpec(chain: ChainMeta) {
	const call = chain.calls[chain.index];
	return {
		tool: call.tool,
		args: call.args,
		cwd: chain.cwd,
		toolCallId: call.toolCallId,
		chain,
	};
}

function stepRef(session: string, kind: "llm" | "tool", spec: any): StepRef {
	return { step_id: newStepId(), session, kind, spec };
}

function toolCallsOf(assistant: any): ToolCallSpec[] {
	return (assistant?.content ?? [])
		.filter((c: any) => c?.type === "toolCall")
		.map((c: any) => ({ toolCallId: c.id, tool: c.name, args: c.arguments ?? {} }));
}

/**
 * Blobs referenced by `next` are uploaded here, BEFORE the caller commits —
 * so any committed step's refs are already replicated (CONTRACT §1 ordering).
 * Re-execution recomputes different hashes (timestamps, resampling); harmless,
 * because commit+next is atomic and only one execution's pair ever lands.
 */
export async function computeNext(
	session: string,
	kind: string,
	spec: any,
	result: any,
	blobs: BlobClient,
): Promise<StepRef | null> {
	if (kind === "llm") return await nextAfterLlm(session, spec, result, blobs);
	if (kind === "tool") return await nextAfterTool(session, spec, result, blobs);
	return null;
}

async function nextAfterLlm(
	session: string,
	spec: any,
	assistant: any,
	blobs: BlobClient,
): Promise<StepRef | null> {
	const calls = toolCallsOf(assistant);
	if (calls.length === 0) return null; // turn over: plain reply, length cap, or error
	const assistantRef = await blobs.put(assistant);
	const chain: ChainMeta = {
		model: spec.model,
		options: spec.options ?? {},
		context: spec.context,
		assistant: assistantRef,
		calls,
		index: 0,
		results: [],
		cwd: spec.cwd ?? process.cwd(),
	};
	return stepRef(session, "tool", buildToolSpec(chain));
}

async function nextAfterTool(
	session: string,
	spec: any,
	result: any,
	blobs: BlobClient,
): Promise<StepRef | null> {
	const chain: ChainMeta | undefined = spec.chain;
	if (!chain) return null; // standalone tool step (tests, manual pokes): nothing to chain

	const call = chain.calls[chain.index];
	const isError = result?.toolError != null;
	const resultRef = await blobs.put({
		role: "toolResult",
		toolCallId: call.toolCallId,
		toolName: call.tool,
		content: isError ? [{ type: "text", text: String(result.toolError) }] : (result.content ?? []),
		isError,
		timestamp: Date.now(),
	});
	const results = [...chain.results, resultRef];

	if (chain.index + 1 < chain.calls.length) {
		return stepRef(session, "tool", buildToolSpec({ ...chain, index: chain.index + 1, results }));
	}

	const context: ContextRefs = {
		...chain.context,
		messages: [...chain.context.messages, chain.assistant, ...results],
	};
	return stepRef(session, "llm", buildLlmSpec(chain.model, context, chain.options, chain.cwd));
}

/** Head-side helper: hash a value and stage it for upload. */
export function stage(blobs: Map<string, unknown>, value: unknown): string {
	const hash = hashOf(value);
	blobs.set(hash, value);
	return hash;
}
