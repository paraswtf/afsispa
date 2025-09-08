#!/usr/bin/env -S tsx

/**
 * freyr-set-spotify-token.ts
 *
 * Usage:
 *  # by index (0-based)
 *  tsx scripts/freyr-set-spotify-token.ts --index 0
 *
 *  # by name (case-insensitive)
 *  tsx scripts/freyr-set-spotify-token.ts --name "Research 3"
 *
 *  # override freyr config path:
 *  FREYR_CONFIG_PATH=~/.config/FreyrCLI/d3fault.x4p tsx scripts/freyr-set-spotify-token.ts --index 0
 *
 * Behavior:
 *  - Uses Client Credentials flow to obtain access_token and expires_in.
 *  - compute expiry = Date.now() + expires_in * 1000 (ms).
 *  - writes services.spotify = { expiry, accessToken, refreshToken } into Freyr config json.
 *  - prints the written object to stdout.
 *
 * Notes:
 *  - Client Credentials does not return a refresh token. If you have refresh tokens
 *    stored in your SPOTIFY_AUTH entries (field `refresh_token`), the script will
 *    write that value into the config. Otherwise refreshToken will be "" (empty).
 *  - Make sure SPOTIFY_AUTH is present in .env as a JSON array.
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

function parseArgs(): { index?: number; name?: string } {
	const argv = process.argv.slice(2);
	const out: { index?: number; name?: string } = {};
	for (let i = 0; i < argv.length; i++) {
		const a = argv[i];
		if (a === "--index" || a === "-i") {
			const v = argv[++i];
			if (!v) die("Missing value for --index");
			const n = Number(v);
			if (!Number.isFinite(n) || n < 0) die("Invalid index value");
			out.index = n;
		} else if (a === "--name" || a === "-n") {
			const v = argv[++i];
			if (!v) die("Missing value for --name");
			out.name = v;
		} else {
			die(`Unknown arg ${a}`);
		}
	}
	if (out.index === undefined && !out.name) die('Pass --index N or --name "..."');
	return out;
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
		const msg = json && json.error_description ? json.error_description : JSON.stringify(json);
		throw new Error(`Spotify token request failed ${res.status}: ${msg}`);
	}
	return json as { access_token: string; expires_in: number; token_type?: string; scope?: string };
}

function expandHome(p: string) {
	if (p.startsWith("~/")) return path.join(os.homedir(), p.slice(2));
	return p;
}

async function readConfig(configPath: string) {
	try {
		const txt = await fs.readFile(configPath, "utf8");
		return JSON.parse(txt);
	} catch (err: any) {
		// if file missing, return an empty base config
		if (err.code === "ENOENT") return {};
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
	const { index, name } = parseArgs();
	const entries = loadAuthEntries();

	let entry: AuthEntry | undefined;
	if (index !== undefined) {
		if (index < 0 || index >= entries.length) die(`Index ${index} out of range (0..${entries.length - 1})`);
		entry = entries[index];
	} else if (name) {
		const nlow = name.toLowerCase();
		entry = entries.find((e) => (e.name ?? "").toLowerCase() === nlow);
		if (!entry) die(`No entry found with name "${name}"`);
	}

	if (!entry) die("Selected entry not found");

	// fetch token (Client Credentials)
	let tokenResp;
	try {
		tokenResp = await requestClientCredentialsToken(entry.client_id, entry.client_secret);
	} catch (err: any) {
		die(`Failed to request token: ${(err && err.message) || err}`);
	}

	const expiry = Date.now() + (Number(tokenResp.expires_in) || 3600) * 1000;
	const outObj = {
		expiry,
		accessToken: tokenResp.access_token,
		refreshToken: entry.refresh_token ?? "" // if you stored one in SPOTIFY_AUTH, it'll be used; else empty string
	};

	// config path
	const defaultPath = path.join(os.homedir(), ".config", "FreyrCLI", "d3fault.x4p");
	const cfgPathEnv = process.env.FREYR_CONFIG_PATH;
	const cfgPath = expandHome(cfgPathEnv ? cfgPathEnv : defaultPath);

	// read existing config, update services.spotify
	let cfg: any = {};
	try {
		cfg = await readConfig(cfgPath);
	} catch (err: any) {
		die(`Failed to read freyr config at ${cfgPath}: ${err.message || err}`);
	}

	// ensure structure
	if (!cfg.services || typeof cfg.services !== "object") cfg.services = {};
	cfg.services.spotify = {
		expiry: outObj.expiry,
		accessToken: outObj.accessToken,
		refreshToken: outObj.refreshToken
	};

	// write back
	try {
		await writeConfig(cfgPath, cfg);
	} catch (err: any) {
		die(`Failed to write freyr config at ${cfgPath}: ${err.message || err}`);
	}

	// print the JSON object to stdout (exact shape requested)
	console.log(JSON.stringify(outObj, null, 2));
})();
