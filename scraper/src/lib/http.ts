// src/lib/http.ts
import { getUrlCache, setUrlCache } from "../db/helpers";

type SpotifyAuthEntry = {
	name: string;
	client_id: string;
	client_secret: string;
};

let globalBackoffUntil = 0; // kept for global fallback behavior (unused for token-specific limits)

/**
 * ---------- Token manager ----------
 * Loads SPOTIFY_AUTH JSON from env (array of {name, client_id, client_secret})
 * Exchanges client credentials for bearer tokens and caches them with expiry.
 * Tracks per-token rateLimitUntil and allows marking tokens rate-limited.
 * Also tracks lastUsed timestamp to prefer least-recently-used tokens (rotation).
 */
class SpotifyTokenManager {
	private clients: SpotifyAuthEntry[] = [];
	// cached tokens: { token, expiresAtMs }
	private tokens: Array<{ token?: string; expiresAt?: number }> = [];
	// per-token rate-limit windows (ms since epoch)
	private rateLimitUntil: number[] = [];
	// per-token last used timestamp (ms since epoch)
	private lastUsed: number[] = [];
	private _lastRoundRobin = -1;

	constructor() {
		const raw = process.env.SPOTIFY_AUTH ?? "[]";
		try {
			const parsed = JSON.parse(raw);
			if (!Array.isArray(parsed)) throw new Error("SPOTIFY_AUTH must be an array");
			this.clients = parsed as SpotifyAuthEntry[];
		} catch (err) {
			console.error("Invalid SPOTIFY_AUTH env. Expected JSON array of clients.", err);
			this.clients = [];
		}
		this.tokens = this.clients.map(() => ({}));
		this.rateLimitUntil = this.clients.map(() => 0);
		this.lastUsed = this.clients.map(() => 0);
	}

	/** Public helpers for logging/debugging */
	get count() {
		return this.clients.length;
	}
	getClientName(idx: number): string {
		return this.clients[idx]?.name ?? `client_${idx}`;
	}
	getRateLimitUntil(idx: number): number {
		return this.rateLimitUntil[idx] ?? 0;
	}
	getLastUsed(idx: number): number {
		return this.lastUsed[idx] ?? 0;
	}

	/**
	 * Pick an available client index.
	 * Strategy:
	 *  - Filter out rate-limited clients
	 *  - From remaining, pick the one with smallest lastUsed (least recently used)
	 *  - Use round-robin starting point to avoid always picking index 0
	 */
	getAvailableClientIndex(): number | null {
		if (this.clients.length === 0) return null;
		const now = Date.now();
		const avail: number[] = [];
		for (let i = 0; i < this.clients.length; i++) {
			if ((this.rateLimitUntil[i] ?? 0) <= now) avail.push(i);
		}
		if (avail.length === 0) return null;

		// prefer least recently used among available
		avail.sort((a, b) => (this.lastUsed[a] ?? 0) - (this.lastUsed[b] ?? 0));

		// rotate starting point slightly for fairness
		const startOffset = (this._lastRoundRobin + 1) % avail.length;
		const chosen = avail[(startOffset + 0) % avail.length];
		this._lastRoundRobin = (avail.indexOf(chosen!) >= 0 ? chosen : this._lastRoundRobin)!;
		// mark lastUsed immediately (optimistic) to avoid thundering the same token
		this.lastUsed[chosen!] = Date.now();
		return chosen!;
	}

	// mark a specific client as rate-limited for seconds (or until timestamp)
	markRateLimited(idx: number, secondsOrUntil?: number) {
		if (idx < 0 || idx >= this.rateLimitUntil.length) return;
		const now = Date.now();
		let untilMs: number;
		if (!secondsOrUntil) {
			untilMs = now + 3000; // default 3s
		} else if (secondsOrUntil > 1_000_000_000) {
			// looks like a timestamp (ms)
			untilMs = secondsOrUntil;
		} else {
			// treat as seconds
			untilMs = now + Math.floor(secondsOrUntil) * 1000;
		}
		this.rateLimitUntil[idx] = untilMs;

		// Log which app got rate-limited and when it resumes (human readable)
		const name = this.getClientName(idx);
		const resumeAt = new Date(untilMs).toISOString();
		console.warn(`Token (app: ${name}) rate-limited. Paused until ${resumeAt} (${Math.ceil((untilMs - now) / 1000)}s).`);
	}

	// fetch or return cached token for index
	async getToken(idx: number): Promise<string> {
		if (idx < 0 || idx >= this.clients.length) throw new Error("invalid client index");
		const cached = this.tokens[idx];
		const now = Date.now();
		if (cached?.token && (cached.expiresAt ?? 0) - 60000 > now) {
			// token still valid (with 60s safety margin)
			return cached.token!;
		}
		// fetch a new token
		const client = this.clients[idx]!;
		const resp = await fetch("https://accounts.spotify.com/api/token", {
			method: "POST",
			headers: {
				"Content-Type": "application/x-www-form-urlencoded",
				"Authorization": `Basic ${Buffer.from(`${client.client_id}:${client.client_secret}`).toString("base64")}`
			},
			body: new URLSearchParams({ grant_type: "client_credentials" }).toString()
		});
		if (!resp.ok) {
			const txt = await safeText(resp);
			throw new Error(`Failed to obtain token for client ${client.name}: ${resp.status} ${resp.statusText} - ${txt}`);
		}
		const json: {
			access_token?: string;
			expires_in?: number;
		} = (await resp.json().catch(() => ({}))) as any;
		if (!json?.access_token || !json?.expires_in) {
			throw new Error(`Invalid token response for client ${client.name}`);
		}
		this.tokens[idx] = {
			token: json.access_token,
			expiresAt: Date.now() + json.expires_in * 1000
		};
		// update lastUsed when token is freshly fetched so rotation accounts for this
		this.lastUsed[idx] = Date.now();
		return json.access_token;
	}
}

const tokenManager = new SpotifyTokenManager();

/**
 * Core fetch with:
 *  - cache read/write
 *  - 429 global backoff window (kept for generic fallback)
 *  - infinite retries for transient errors (429, 5xx, network)
 *  - fast exponential backoff with jitter
 *
 * Permanent client errors (4xx except 401/429) THROW immediately.
 */
export async function fetchWithRetry(
	url: string,
	options?: RequestInit,
	{
		cache = true,
		initialBackoffMs = 2000,
		maxBackoffMs = 30000,
		responseIsError = (json: any) => Boolean(json?.error) // customize if needed
	}: {
		cache?: boolean;
		initialBackoffMs?: number;
		maxBackoffMs?: number;
		responseIsError?: (json: any) => boolean;
	} = {}
): Promise<any> {
	// 0) Cache check
	if (cache) {
		try {
			const cached = await getUrlCache(url);
			if (cached) return cached.data ?? cached; // support both shapes
		} catch {
			// cache read errors are non-fatal
		}
	}

	let backoff = initialBackoffMs;

	// Infinite retry loop for transient errors
	for (;;) {
		// Respect any active global rate-limit window
		const now = Date.now();
		if (globalBackoffUntil > now) {
			const waitMs = globalBackoffUntil - now;
			// eslint-disable-next-line no-console
			console.warn(`Global backoff active. Waiting ${Math.ceil(waitMs / 1000)}s...`);
			await wait(waitMs);
		}

		try {
			const res = await fetch(url, options);

			// Handle 429 rate limit: honor Retry-After and retry
			if (res.status === 429) {
				const retryAfter = res.headers.get("retry-after");
				const waitSeconds = retryAfter ? parseInt(retryAfter, 10) : 3;
				globalBackoffUntil = Date.now() + waitSeconds * 1000;
				// eslint-disable-next-line no-console
				console.warn(`Rate limited. Pausing all requests for ${waitSeconds}s...`);
				continue;
			}

			// Transient server errors → retry forever with backoff
			if (res.status >= 500) {
				console.warn(`Server error: ${res.status} ${res.statusText}. Retrying...`);
				await backoffWait(backoff, maxBackoffMs);
				backoff = nextBackoff(backoff, maxBackoffMs);
				continue;
			}

			// Client errors (except 401 handled upstream) → throw now (permanent)
			if (res.status >= 400 && res.status !== 401) {
				const text = await safeText(res);
				throw new Error(`HTTP ${res.status} ${res.statusText}: ${text}`.trim());
			}

			// Success (2xx) or 401 handled elsewhere
			const data = await res.json().catch(() => ({}));

			// Some Spotify endpoints return JSON with an `error` object—treat as failure.
			if (responseIsError(data)) {
				// If API signals error without proper status code, treat as transient server hiccup
				console.warn(`API error: ${JSON.stringify((data as any).error)}`);
				await backoffWait(backoff, maxBackoffMs);
				backoff = nextBackoff(backoff, maxBackoffMs);
				continue;
			}

			// Write to cache (best-effort)
			if (cache && !(data && typeof data === "object" && "error" in data)) {
				try {
					await setUrlCache(url, data);
				} catch {
					// ignore cache write errors
				}
			}

			return data;
		} catch (err: any) {
			// Network/transport error → retry with backoff
			// eslint-disable-next-line no-console
			console.warn(`Network error: ${err?.message ?? err}. Retrying...`);
			await backoffWait(backoff, maxBackoffMs);
			backoff = nextBackoff(backoff, maxBackoffMs);
			continue;
		}
	}
}

/**
 * Spotify-aware JSON fetcher using token rotation:
 *  - obtains tokens from tokenManager (client credentials)
 *  - picks a non-rate-limited token and tries
 *  - if 401, refreshes that token and retries once
 *  - if 429 on a token, mark that token rate-limited and try another token
 *  - if all tokens exhausted, fall back to fetchWithRetry with last token (which will handle global retry/backoff)
 */
export async function fetchSpotifyJSON(url: string, init?: Omit<RequestInit, "headers"> & { headers?: Record<string, string> }, opts?: Parameters<typeof fetchWithRetry>[2]): Promise<any> {
	// If no configured clients, fallback to existing behavior: use upstream getSpotifyToken if present
	if (tokenManager.count === 0) {
		// fallback to whatever existing getSpotifyToken provides (if present). Try to import lazily.
		try {
			// eslint-disable-next-line @typescript-eslint/no-var-requires
			const { getSpotifyToken } = require("../spotify/client");
			const singleToken = await getSpotifyToken();
			return fetchWithRetry(url, withAuth(init, singleToken), opts);
		} catch (_e) {
			throw new Error("No SPOTIFY_AUTH configured and no getSpotifyToken available.");
		}
	}

	let lastError: any = null;
	const triedIndices = new Set<number>();

	// Attempt tokens until one succeeds or all exhausted
	for (;;) {
		const idx = tokenManager.getAvailableClientIndex();
		if (idx === null) {
			// all tokens rate-limited currently -> pick the token with earliest release and wait no more than a short time
			// Find earliest unlimit time
			const now = Date.now();
			let earliest = Infinity;
			for (let i = 0; i < tokenManager.count; i++) {
				const t = tokenManager.getRateLimitUntil(i);
				if (t > now && t < earliest) earliest = t;
			}
			// If earliest is soon, wait until then; otherwise break and let fetchWithRetry handle broader retries.
			if (earliest !== Infinity && earliest - now <= 5000) {
				const waitMs = earliest - now;
				// eslint-disable-next-line no-console
				console.warn(`All tokens temporarily rate-limited. Waiting ${Math.ceil(waitMs / 1000)}s for next available token...`);
				await wait(waitMs);
				continue;
			}
			// All tokens are rate-limited for longer or none available -> fallback to fetchWithRetry with a fresh token from index 0
			try {
				const token = await tokenManager.getToken(0);
				return fetchWithRetry(url, withAuth(init, token), opts);
			} catch (e) {
				// If token fetch failed, throw last error
				throw e;
			}
		}

		// Prevent infinite loop: if we've tried this idx already and it failed, mark and continue
		if (triedIndices.has(idx) && triedIndices.size >= tokenManager.count) break;
		triedIndices.add(idx);

		// obtain token for this idx
		let token: string | undefined;
		try {
			token = await tokenManager.getToken(idx);
		} catch (err) {
			// couldn't obtain token for this client -> move to next
			lastError = err;
			// mark rate-limited briefly so we don't repeatedly try the same broken client
			tokenManager.markRateLimited(idx, 3);
			continue;
		}

		// try once with token
		let res = await tryOnce(url, withAuth(init, token));
		// if network error (tryOnce returns undefined), let fetchWithRetry handle it with that token
		if (!res) {
			// fallback to robust retrier using this token
			return fetchWithRetry(url, withAuth(init, token), opts);
		}

		// 401 -> refresh token and retry once
		if (res.status === 401) {
			try {
				// refresh token for this client and retry one more time
				await tokenManager.getToken(idx); // forces refresh if expired
			} catch (err) {
				lastError = err;
				// mark this client briefly and try next client
				tokenManager.markRateLimited(idx, 2);
				continue;
			}
			// re-get token and retry
			try {
				token = await tokenManager.getToken(idx);
			} catch (err) {
				lastError = err;
				tokenManager.markRateLimited(idx, 2);
				continue;
			}
			res = await tryOnce(url, withAuth(init, token));
			if (!res) return fetchWithRetry(url, withAuth(init, token), opts);
		}

		// 429 -> mark this token as rate-limited and try next token
		if (res.status === 429) {
			const retryAfter = res.headers.get("retry-after");
			const waitSeconds = retryAfter ? parseInt(retryAfter, 10) : 3;
			// mark token-specific rate-limit and log app name (markRateLimited logs the name + resume time)
			tokenManager.markRateLimited(idx, waitSeconds);
			// continue to next token in loop
			continue;
		}

		// 5xx -> let fetchWithRetry handle backoff/retries (using this token)
		if (res.status >= 500) {
			return fetchWithRetry(url, withAuth(init, token), opts);
		}

		// other 4xx (except handled 401/429 above) -> throw
		if (res.status >= 400 && res.status !== 401 && res.status !== 429) {
			const text = await safeText(res);
			throw new Error(`HTTP ${res.status} ${res.statusText}: ${text}`.trim());
		}

		// success -> parse json, cache, return
		if (res.ok) {
			const data = await res.json().catch(() => ({}));
			// treat API-level error shapes as failures (let fetchWithRetry logic be similar)
			if (data && typeof data === "object" && "error" in data) {
				// If the server responded with a JSON error but 2xx, treat as transient and let fetchWithRetry handle it
				return fetchWithRetry(url, withAuth(init, token), opts);
			}
			// cache best-effort
			try {
				if (!(data && typeof data === "object" && "error" in data)) await setUrlCache(url, data);
			} catch {}
			return data;
		}

		// If we reach here, something unexpected happened — record error and continue trying other tokens
		lastError = new Error(`Unexpected response status: ${res.status}`);
	}

	// If we exhausted the loop, throw the last error or a generic one
	throw lastError ?? new Error("Failed to fetch from Spotify with any configured token.");
}

function withAuth(init: Omit<RequestInit, "headers"> & { headers?: Record<string, string> } = {}, token: string): RequestInit {
	const headers = {
		Authorization: `Bearer ${token}`,
		...(init.headers ?? {})
	};
	return { ...init, headers };
}

async function tryOnce(url: string, options?: RequestInit) {
	try {
		return await fetch(url, options);
	} catch {
		// network error → indicate failure; fetchWithRetry will take over afterwards
		return undefined as any;
	}
}

async function safeText(res: Response) {
	try {
		return (await res.text()) || "";
	} catch {
		return "";
	}
}

/* Helper utilities (kept from your original file) */
function wait(ms: number) {
	return new Promise((r) => setTimeout(r, ms));
}
function jitter(ms: number) {
	// +/- 20% jitter
	const delta = ms * 0.2;
	const min = ms - delta;
	const max = ms + delta;
	return Math.floor(Math.random() * (max - min + 1) + min);
}
function nextBackoff(current: number, cap: number) {
	return Math.min(current * 2, cap);
}
async function backoffWait(current: number, cap: number) {
	const ms = jitter(Math.min(current, cap));
	// eslint-disable-next-line no-console
	console.warn(`Retrying in ${Math.ceil(ms / 1000)}s...`);
	await wait(ms);
}
