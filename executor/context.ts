// Rebuild a pi-ai Context from content-addressed blob refs.
export interface ContextRefs {
	systemPrompt: string | null;
	messages: string[];
	tools: string | null;
}

export async function rebuildContext(
	refs: ContextRefs,
	getBlob: (hash: string) => Promise<string>,
): Promise<any> {
	if (!refs || !Array.isArray(refs.messages)) {
		throw new Error("llm spec.context must be blob refs: {systemPrompt, messages[], tools}");
	}
	const parse = async (hash: string) => {
		if (typeof hash !== "string") throw new Error(`blob ref must be a hash string, got ${typeof hash}`);
		return JSON.parse(await getBlob(hash));
	};
	const context: any = { messages: await Promise.all(refs.messages.map(parse)) };
	if (refs.systemPrompt) context.systemPrompt = await parse(refs.systemPrompt);
	if (refs.tools) context.tools = await parse(refs.tools);
	return context;
}
