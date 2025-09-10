#!/usr/bin/env -S tsx

/**
 * freyr-set-spotify-token-rr.ts
 *
 * Usage:
 *  tsx scripts/freyr-set-spotify-token-rr.ts          # run round-robin every 30s
 *  tsx scripts/freyr-set-spotify-token-rr.ts --interval 10   # interval in seconds
 *  tsx scripts/freyr-set-spotify-token-rr.ts --once         # run one cycle and exit
 *
 * Behavior:
 *  - Rotates through SPOTIFY_AUTH entries in round-robin order and requests
 *    a Client Credentials token for each entry.
 *  - Computes expiry = Date.now() + expires_in * 1000 (ms).
 *  - Writes services.spotify = { expiry, accessToken, refreshToken } into Freyr config json.
 *  - Prints the JSON object written to stdout each cycle.
 *
 * Notes:
 *  - Client Credentials does not return a refresh token. If you have refresh tokens
 *    stored in your SPOTIFY_AUTH entries (field `refresh_token`), that value will be used.
 *  - Configure FREYR_CONFIG_PATH to override default path.
 */

import dotenv from "dotenv";
import fs from "fs/promises";
import os from "os";
import path from "path";
import process from "process";
import { Buffer } from "buffer";

dotenv.config();

type AuthEntry = {
	name?: string;
	client_id: string;
	client_secret: string;
	refresh_token?: string | null;
};

function die(msg: string, code = 1): never {
	console.error(msg);
	process.exit(code);
}

function parseArgs(): { intervalSec: number; once: boolean } {
	const argv = process.argv.slice(2);
	let intervalSec = 30;
	let once = false;
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === "--interval" || a === "-t") {
			const v = argv[++i];
			if (!v) die("Missing value for --interval");
			const n = Number(v);
			if (!Number.isFinite(n) || n <= 0) die("Invalid interval value");
			intervalSec = Math.floor(n);
		} else if (a === "--once") {
			once = true;
		} else {
			die(`Unknown arg ${a}`);
		}
	}
	return { intervalSec, once };
}

function loadAuthEntries(): AuthEntry[] {
	const raw = process.env.SPOTIFY_AUTH;
	if (!raw) die("SPOTIFY_AUTH not set in environment (put in .env).");
	let parsed: any;
	try {
		parsed = JSON.parse(raw);
	} catch (e) {
		die("Failed to parse SPOTIFY_AUTH: invalid JSON.");
	}
	if (!Array.isArray(parsed)) die("SPOTIFY_AUTH must be a JSON array.");
	return parsed.map((p: any, idx: number) => {
		if (!p || !p.client_id || !p.client_secret) {
			die(`SPOTIFY_AUTH[${idx}] missing client_id or client_secret`);
		}
		return {
			name: p.name,
			client_id: String(p.client_id),
			client_secret: String(p.client_secret),
			refresh_token: p.refresh_token ?? null
		} as AuthEntry;
	});
}

async function requestClientCredentialsToken(clientId: string, clientSecret: string) {
	const tokenUrl = "https://accounts.spotify.com/api/token";
	const body = new URLSearchParams({ grant_type: "client_credentials" }).toString();
	const auth = Buffer.from(`${clientId}:${clientSecret}`).toString("base64");

	const res = await fetch(tokenUrl, {
		method: "POST",
		headers: {
			"Authorization": `Basic ${auth}`,
			"Content-Type": "application/x-www-form-urlencoded",
			"Accept": "application/json"
		},
		body
	});

	const json = await res.json().catch(() => ({}));
	if (!res.ok) {
		const msg = json && (json.error_description || json.error) ? json.error_description || json.error : JSON.stringify(json);
		throw new Error(`Spotify token request failed ${res.status}: ${msg}`);
	}
	return json as { access_token: string; expires_in: number; token_type?: string; scope?: string };
}

function expandHome(p: string) {
	if (!p) return p;
	if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
	return p;
}

async function readConfig(configPath: string) {
	try {
		const txt = await fs.readFile(configPath, "utf8");
		return JSON.parse(txt);
	} catch (err: any) {
		if (err?.code === "ENOENT") return {};
		throw err;
	}
}

async function writeConfig(configPath: string, obj: any) {
	const dir = path.dirname(configPath);
	await fs.mkdir(dir, { recursive: true });
	const pretty = JSON.stringify(obj, null, 2);
	await fs.writeFile(configPath, pretty, "utf8");
}

(async function main() {
	const { intervalSec, once } = parseArgs();
	const entries = loadAuthEntries();
	if (entries.length === 0) die("SPOTIFY_AUTH has no entries.");

	// config path
	const defaultPath = path.join(os.homedir(), ".config", "FreyrCLI", "d3fault.x4p");
	const cfgPathEnv = process.env.FREYR_CONFIG_PATH;
	const cfgPath = expandHome(cfgPathEnv ? cfgPathEnv : defaultPath);

	let idx = 0;
	let stopped = false;
	let cycleCount = 0;
	let runningPromise: Promise<void> | null = null;

	async function doCycle() {
		const entry = entries[idx]!;
		const entryName = entry.name ?? `#${idx}`;
		console.log(`[${new Date().toISOString()}] Starting cycle ${cycleCount + 1}: using entry ${entryName}`);

		try {
			const tokenResp = await requestClientCredentialsToken(entry.client_id, entry.client_secret);
			const expiry = Date.now() + (Number(tokenResp.expires_in) || 3600) * 1000;
			const outObj = {
				expiry,
				accessToken: tokenResp.access_token,
				refreshToken: entry.refresh_token ?? ""
			};

			// read existing config, update services.spotify
			let cfg: any = {};
			try {
				cfg = await readConfig(cfgPath);
			} catch (err: any) {
				console.error(`Failed to read freyr config at ${cfgPath}: ${err.message || err}`);
				// don't die; continue and try to write base config
				cfg = {};
			}

			if (!cfg.services || typeof cfg.services !== "object") cfg.services = {};
			cfg.services.spotify = {
				expiry: outObj.expiry,
				accessToken: outObj.accessToken,
				refreshToken: outObj.refreshToken
			};

			try {
				await writeConfig(cfgPath, cfg);
			} catch (err: any) {
				console.error(`Failed to write freyr config at ${cfgPath}: ${err.message || err}`);
				// continue; we don't exit the loop on write errors
			}

			console.log(JSON.stringify(outObj, null, 2));
		} catch (err: any) {
			console.error(`[${new Date().toISOString()}] Error during token refresh for entry ${entryName}: ${(err && err.message) || err}`);
			// continue to next entry rather than exiting
		} finally {
			// advance round-robin index
			idx = (idx + 1) % entries.length;
			cycleCount++;
		}
	}

	// run first cycle immediately
	runningPromise = (async () => {
		await doCycle();
	})();

	if (once) {
		// wait for the first (and only) cycle to finish then exit
		await runningPromise;
		console.log("Completed single run (--once). Exiting.");
		process.exit(0);
	}

	// schedule next cycles
	const intervalMs = intervalSec * 1000;
	const handle = setInterval(() => {
		// ensure we don't overlap cycles if one takes longer than interval
		if (runningPromise) {
			// if previous still running, skip this tick and log
			// (this prevents concurrent writes for slow network)
			console.warn(`[${new Date().toISOString()}] Previous cycle still running; skipping this tick.`);
			return;
		}
		runningPromise = (async () => {
			try {
				await doCycle();
			} finally {
				runningPromise = null;
			}
		})();
	}, intervalMs);

	function shutdown(signal: string) {
		if (stopped) return;
		stopped = true;
		console.log(`\nReceived ${signal}, shutting down...`);
		clearInterval(handle);
		// wait for in-flight run to complete (but don't wait forever)
		const waitFor = runningPromise
			? Promise.race([
					runningPromise,
					new Promise((res) => setTimeout(res, 5000)) // at most 5s
				])
			: Promise.resolve();
		waitFor.then(() => {
			console.log("Shutdown complete.");
			process.exit(0);
		});
	}

	process.on("SIGINT", () => shutdown("SIGINT"));
	process.on("SIGTERM", () => shutdown("SIGTERM"));

	// keep process alive
})();
