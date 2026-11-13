import { describe, expect, it } from "bun:test";
import { fetchServerHealth, summarizeCoverage } from "../src/commands/status";

describe("summarizeCoverage", () => {
	it("lists every source seen on either side, sorted", () => {
		const { rows } = summarizeCoverage(
			{ opencode: 10, omp: 4 },
			{ opencode: 7, mcp: 2 },
		);
		expect(rows.map((r) => r.source)).toEqual(["mcp", "omp", "opencode"]);
		expect(rows.find((r) => r.source === "opencode")).toEqual({
			source: "opencode",
			captured: 10,
			injected: 7,
		});
	});

	it("warns when a source is captured but never injected", () => {
		const { warnings } = summarizeCoverage({ omp: 1528 }, {});
		expect(warnings.length).toBe(1);
		expect(warnings[0]).toContain("omp");
		expect(warnings[0]).toContain("1528");
	});

	it("does not warn for sources that were injected or never captured", () => {
		const { warnings } = summarizeCoverage(
			{ opencode: 5 },
			{ opencode: 3, mcp: 9 },
		);
		expect(warnings).toEqual([]);
	});
});

describe("summarizeCoverage client tags", () => {
	it("matches the oh-my-pi capture source with the omp client tag", () => {
		const { rows, warnings } = summarizeCoverage(
			{ "oh-my-pi": 40 },
			{ omp: 12 },
		);
		expect(rows).toEqual([{ source: "oh-my-pi", captured: 40, injected: 12 }]);
		expect(warnings).toEqual([]);
	});

	it("still warns for oh-my-pi when nothing was injected under omp", () => {
		const { warnings } = summarizeCoverage({ "oh-my-pi": 40 }, {});
		expect(warnings.length).toBe(1);
		expect(warnings[0]).toContain("oh-my-pi");
	});
});

describe("fetchServerHealth", () => {
	it("reads health and reasons from a running server", async () => {
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: () =>
				Response.json({
					status: "ok",
					health: "degraded",
					reasons: ["store:sqlite failing"],
				}),
		});
		try {
			expect(
				await fetchServerHealth(`http://127.0.0.1:${server.port}`),
			).toEqual({ health: "degraded", reasons: ["store:sqlite failing"] });
		} finally {
			server.stop(true);
		}
	});

	it("returns null when nothing answers", async () => {
		const server = Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: () => new Response(""),
		});
		const port = server.port;
		server.stop(true);
		expect(await fetchServerHealth(`http://127.0.0.1:${port}`, 500)).toBeNull();
	});
});

describe("fetchServerHealth with a protected /status", () => {
	const TOKEN = "stable-admin-token-0123456789";
	/** A server that answers /status only to the admin bearer, as with KNOWLEDGE_PROTECT_READS. */
	function protectedServer() {
		return Bun.serve({
			hostname: "127.0.0.1",
			port: 0,
			fetch: (req) =>
				req.headers.get("authorization") === `Bearer ${TOKEN}`
					? Response.json({ status: "ok", health: "ok", reasons: [] })
					: Response.json({ error: "Unauthorized" }, { status: 401 }),
		});
	}

	it("sends the admin token as a bearer and reads the health", async () => {
		const server = protectedServer();
		try {
			expect(
				await fetchServerHealth(`http://127.0.0.1:${server.port}`, 2000, TOKEN),
			).toEqual({ health: "ok", reasons: [] });
		} finally {
			server.stop(true);
		}
	});

	it("without the token, a protected server reads as not answering", async () => {
		const server = protectedServer();
		try {
			expect(
				await fetchServerHealth(`http://127.0.0.1:${server.port}`),
			).toBeNull();
		} finally {
			server.stop(true);
		}
	});
});
