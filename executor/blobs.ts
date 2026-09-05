import { createHash, randomUUID } from "node:crypto";

export function sha256(data: string): string {
	return createHash("sha256").update(data).digest("hex");
}

export function hashOf(value: unknown): string {
	return sha256(JSON.stringify(value));
}

// Random IDs let identical inputs represent distinct sampling requests.
export function newStepId(): string {
	return randomUUID();
}

export class BlobClient {
	sidecar: string;

	constructor(sidecar: string) {
		this.sidecar = sidecar;
	}

	async get(hash: string, signal?: AbortSignal): Promise<string> {
		const res = await fetch(`${this.sidecar}/v1/blob/${hash}`, { signal });
		if (!res.ok) throw new Error(`blob ${hash.slice(0, 12)}… unavailable (HTTP ${res.status})`);
		return await res.text();
	}

	async getJson(hash: string): Promise<any> {
		return JSON.parse(await this.get(hash));
	}

	async putMissing(blobs: Map<string, unknown>, signal?: AbortSignal): Promise<void> {
		if (blobs.size === 0) return;
		const res = await fetch(`${this.sidecar}/v1/blobs/missing`, {
			method: "POST",
			headers: { "content-type": "application/json" },
			body: JSON.stringify({ hashes: [...blobs.keys()] }),
			signal,
		});
		if (!res.ok) throw new Error(`blobs/missing HTTP ${res.status}`);
		const { missing } = (await res.json()) as { missing: string[] };
		await Promise.all(missing.map((hash) => this.putOne(hash, blobs.get(hash), signal)));
	}

	// Uploads precede step submission so Raft never references missing bytes.
	async put(value: unknown, signal?: AbortSignal): Promise<string> {
		const hash = hashOf(value);
		await this.putOne(hash, value, signal);
		return hash;
	}

	private async putOne(hash: string, value: unknown, signal?: AbortSignal): Promise<void> {
		const res = await fetch(`${this.sidecar}/v1/blob/${hash}`, {
			method: "PUT",
			headers: { "content-type": "application/octet-stream" },
			body: JSON.stringify(value),
			signal,
		});
		if (!res.ok) throw new Error(`blob PUT ${hash.slice(0, 12)}… HTTP ${res.status}`);
	}
}
