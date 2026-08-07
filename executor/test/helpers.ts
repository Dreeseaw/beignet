import { spawn, type ChildProcess } from "node:child_process";
import path from "node:path";
import { fileURLToPath } from "node:url";

export const executorDir = path.dirname(path.dirname(fileURLToPath(import.meta.url)));

export function freePort(): number {
	return 20000 + Math.floor(Math.random() * 20000);
}

export function launch(script: string, env: Record<string, string>, args: string[] = []): ChildProcess {
	const child = spawn("node", [path.join(executorDir, script), ...args], {
		env: { ...process.env, ...env },
		stdio: ["ignore", "pipe", "pipe"],
	});
	child.stderr!.on("data", (d) => process.stderr.write(`  ${d}`));
	return child;
}

export async function waitHealthy(url: string, timeoutMs = 15000): Promise<void> {
	const deadline = Date.now() + timeoutMs;
	while (Date.now() < deadline) {
		try {
			const res = await fetch(`${url}/healthz`);
			if (res.ok) return;
		} catch {}
		await new Promise((r) => setTimeout(r, 150));
	}
	throw new Error(`${url} not healthy after ${timeoutMs}ms`);
}

export function assistantMessage(content: any[], stopReason: string): any {
	return {
		role: "assistant",
		content,
		api: "anthropic-messages",
		provider: "beignet",
		model: "agent",
		usage: {
			input: 1, output: 1, cacheRead: 0, cacheWrite: 0, totalTokens: 2,
			cost: { input: 0, output: 0, cacheRead: 0, cacheWrite: 0, total: 0 },
		},
		stopReason,
		timestamp: Date.now(),
	};
}
