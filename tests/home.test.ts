import { afterEach, beforeEach, describe, expect, it } from "bun:test";
import { homedir } from "node:os";
import { homeDir } from "../src/home";

describe("homeDir", () => {
	let savedHome: string | undefined;

	beforeEach(() => {
		savedHome = process.env.HOME;
	});

	afterEach(() => {
		if (savedHome === undefined) Reflect.deleteProperty(process.env, "HOME");
		else process.env.HOME = savedHome;
	});

	it("reads HOME at call time, so a runtime change is honoured", () => {
		process.env.HOME = "/tmp/ks-home-probe";
		expect(homeDir()).toBe("/tmp/ks-home-probe");
	});

	it("falls back to the account home when HOME is empty", () => {
		process.env.HOME = "";
		expect(homeDir()).toBe(homedir());
	});
});
