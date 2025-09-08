// download-tracks-freyr-with-art-separate.ts
import { PrismaClient } from "@prisma/client";
import { spawn } from "child_process";
import { promises as fs } from "fs";
import path from "path";
import os from "os";
import * as mm from "music-metadata";
import pLimit from "p-limit";

const prisma = new PrismaClient();

/**
 * CONFIG
 */
const BATCH_SIZE = 40;
const CONCURRENCY = process.env.DL_CONCURRENCY ? parseInt(process.env.DL_CONCURRENCY) : 2;
if (!process.env.FREYR_CMD) throw new Error("Missing required environment variables");
if (!process.env.TRACKS_OUT_DIR || !process.env.ART_OUT_DIR) throw new Error("Missing required environment variables");
const FREYR_CMD = process.env.FREYR_CMD;
const OUTPUT_DIR = path.resolve(process.env.TRACKS_OUT_DIR);
const ART_DIR = path.resolve(process.env.ART_OUT_DIR);
const PLAYLIST_NAME = "freyr_playlist.m3u";
const TARGET_BITRATE = 320; // kbps - passed to freyr and used as human threshold

const SUPPORTED_PREFIXES = ["spotify:", "apple_music:", "deezer:"];

/**
 * Helpers
 */
/**
 * Move a file, falling back to copy+unlink if across filesystems (EXDEV).
 * Returns the final destination path on success.
 */
async function moveFile(src: string, dest: string): Promise<string> {
	// ensure parent dir exists for dest
	await fs.mkdir(path.dirname(dest), { recursive: true });

	try {
		await fs.rename(src, dest);
		return dest;
	} catch (err: any) {
		// If it's not EXDEV, rethrow
		if (err && err.code !== "EXDEV") {
			throw err;
		}

		// EXDEV: copy then unlink
		// Use fs.copyFile for efficiency
		try {
			await fs.copyFile(src, dest);
		} catch (copyErr) {
			// if copy failed, ensure no half-file left
			try {
				await fs.unlink(dest);
			} catch (_) {
				/* ignore */
			}
			throw copyErr;
		}

		// remove original
		try {
			await fs.unlink(src);
		} catch (unlinkErr) {
			// if we couldn't remove the original, that's bad but file was copied;
			// warn and return dest nonetheless.
			console.warn(`Warning: copied file to ${dest} but failed to remove original ${src}: ${(unlinkErr as Error).message}`);
		}

		return dest;
	}
}

async function ensureDir(dir: string) {
	await fs.mkdir(dir, { recursive: true });
}
async function safeRmDir(dir: string) {
	try {
		await fs.rm(dir, { recursive: true, force: true });
	} catch {
		// ignore
	}
}

function isSupportedUri(uri?: string): boolean {
	if (!uri) return false;
	const lower = uri.toLowerCase();
	return SUPPORTED_PREFIXES.some((p) => lower.startsWith(p));
}

function buildFreyrUri(doc: any): string | null {
	if (!doc) return null;

	if (doc.spotifyId && typeof doc.spotifyId === "string" && doc.spotifyId.trim()) {
		return `spotify:track:${doc.spotifyId.trim()}`;
	}

	if (doc.uri && typeof doc.uri === "string" && isSupportedUri(doc.uri)) {
		return doc.uri;
	}

	if (typeof doc.uri === "string") {
		const parts = doc.uri.split(/[:/]/);
		const last = parts[parts.length - 1] ?? "";
		if (/^[A-Za-z0-9]{22}$/.test(last)) {
			return `spotify:track:${last}`;
		}
		const possible = parts.slice(-2)[0] ?? "";
		if (/^[A-Za-z0-9]{22}$/.test(possible)) {
			return `spotify:track:${possible}`;
		}
	}

	return null;
}

function runFreyrCapture(uri: string, tmpDir: string): Promise<{ code: number | null; stdout: string; stderr: string }> {
	return new Promise((resolve, reject) => {
		const args = [uri, "-d", tmpDir, "-p", PLAYLIST_NAME, "-b", String(TARGET_BITRATE), "--no-logo", "--no-header"];
		const p = spawn(FREYR_CMD, args, { stdio: ["ignore", "pipe", "pipe"] });

		let stdout = "";
		let stderr = "";
		p.stdout.on("data", (b) => (stdout += b.toString()));
		p.stderr.on("data", (b) => (stderr += b.toString()));

		p.on("error", (err) => reject(err));
		p.on("close", (code) => resolve({ code, stdout, stderr }));
	});
}

async function readPlaylistFirstTrack(playlistPath: string): Promise<string | null> {
	try {
		const text = await fs.readFile(playlistPath, "utf8");
		for (const ln of text.split(/\r?\n/)) {
			const l = ln.trim();
			if (!l || l.startsWith("#")) continue;
			return l;
		}
	} catch {
		// ignore
	}
	return null;
}

async function findFirstAudioFile(dir: string): Promise<string | null> {
	const entries = await fs.readdir(dir, { withFileTypes: true });
	for (const e of entries) {
		const full = path.join(dir, e.name);
		if (e.isFile()) {
			if (/\.(m4a|mp3|flac|aac|wav)$/i.test(e.name)) return full;
		} else if (e.isDirectory()) {
			const nested = await findFirstAudioFile(full);
			if (nested) return nested;
		}
	}
	return null;
}

async function findCoverFile(tmpDir: string): Promise<string | null> {
	const candidates = ["cover.png", "cover.jpg", "cover.jpeg", "cover.webp", "artwork.png", "album.jpg", "cover.bmp"];
	try {
		const entries = await fs.readdir(tmpDir);
		for (const c of candidates) {
			if (entries.includes(c)) return path.join(tmpDir, c);
		}
		for (const e of entries) {
			if (/\.(png|jpe?g|webp|bmp)$/i.test(e)) return path.join(tmpDir, e);
		}
	} catch {
		// ignore
	}
	return null;
}

async function writePictureToFile(picture: mm.IPicture, destPathNoExt: string): Promise<string> {
	const mime = (picture.format || "").toLowerCase();
	let ext = ".jpg";
	if (mime.includes("png")) ext = ".png";
	else if (mime.includes("webp")) ext = ".webp";
	else if (mime.includes("bmp")) ext = ".bmp";
	else if (mime.includes("gif")) ext = ".gif";

	const final = `${destPathNoExt}${ext}`;
	await fs.writeFile(final, picture.data);
	return final;
}

/**
 * Save album art to ART_DIR (separate from audio). Returns saved path or null.
 */
async function saveAlbumArtToDir(tmpDir: string, metadata: mm.IAudioMetadata | null, id: string): Promise<string | null> {
	// 1) prefer freyr cover file
	const coverFile = await findCoverFile(tmpDir);
	if (coverFile) {
		const ext = path.extname(coverFile) || ".png";
		let dest = path.join(ART_DIR, `${id}${ext}`);
		try {
			await fs.access(dest);
			const ts = Date.now();
			dest = path.join(ART_DIR, `${id}-${ts}${ext}`);
		} catch {
			// dest not exist
		}
		try {
			await fs.copyFile(coverFile, dest);
			return dest;
		} catch (e) {
			console.warn(`Failed to copy cover file for ${id}: ${(e as Error).message}`);
		}
	}

	// 2) fallback to embedded picture from metadata
	try {
		const pics = metadata?.common?.picture;
		if (Array.isArray(pics) && pics.length > 0) {
			const pic = pics[0]!;
			const destNoExt = path.join(ART_DIR, id);
			const final = await writePictureToFile(pic, destNoExt);
			return final;
		}
	} catch (e) {
		console.warn(`Failed to extract picture from metadata for ${id}: ${(e as Error).message}`);
	}

	return null;
}

/**
 * Process single document: run freyr, move audio to OUTPUT_DIR, save art to ART_DIR, update DB duration.
 */
async function processDoc(doc: any) {
	const id = doc._id && (doc._id.$oid || doc._id.toString) ? (doc._id.$oid ?? doc._id.toString()) : (doc.id ?? doc._id?.toString?.() ?? null);
	if (!id) {
		console.warn("Skipping doc with no id-like field:", JSON.stringify(doc).slice(0, 200));
		return;
	}

	const freyrUri = buildFreyrUri(doc);
	if (!freyrUri) {
		console.warn(`Skipping ${id}: cannot construct a supported freyr URI. stored uri=${doc.uri ?? "(none)"} spotifyId=${doc.spotifyId ?? "(none)"}`);
		return;
	}

	console.log(`→ [${id}] using freyr URI: ${freyrUri}`);

	const tmpDir = await fs.mkdtemp(path.join(os.tmpdir(), "freyr-"));
	try {
		const { code, stdout, stderr } = await runFreyrCapture(freyrUri, tmpDir);

		if (code !== 0) {
			console.error(`freyr failed for ${id} (code=${code}). stderr:\n${stderr}\nstdout:\n${stdout}`);
			const lower = (stderr + "\n" + stdout).toLowerCase();
			if (lower.includes("invalid query") || lower.includes("invalid uri") || lower.includes("invalid")) {
				console.warn(`freyr reported invalid query for ${id}; skipping.`);
				return;
			}
			console.warn(`freyr returned non-zero code for ${id}; skipping.`);
			return;
		}

		// locate audio
		const playlistPath = path.join(tmpDir, PLAYLIST_NAME);
		let filePath = await readPlaylistFirstTrack(playlistPath);
		if (!filePath) filePath = await findFirstAudioFile(tmpDir);
		if (!filePath) {
			console.error(`No output file found for ${id}. freyr stdout:\n${stdout}\nfreyr stderr:\n${stderr}`);
			return;
		}

		if (!path.isAbsolute(filePath)) filePath = path.resolve(tmpDir, filePath);
		try {
			await fs.access(filePath);
		} catch {
			console.error(`Declared file path does not exist for ${id}: ${filePath}`);
			return;
		}

		// parse metadata
		let metadata: mm.IAudioMetadata | null = null;
		let durationSec: number | null = null;
		try {
			metadata = await mm.parseFile(filePath, { duration: true });
			const dur = metadata.format.duration ?? null;
			if (typeof dur === "number" && !Number.isNaN(dur)) durationSec = Math.round(dur);
		} catch (e) {
			console.warn(`Could not parse metadata for ${id}: ${(e as Error).message}`);
		}

		const bitrateBps = metadata?.format?.bitrate ?? null;
		if (bitrateBps) {
			const bitrateKbps = Math.round(bitrateBps / 1000);
			console.log(`Detected bitrate for ${id}: approx ${bitrateKbps} kbps`);
			if (bitrateKbps < Math.max(192, TARGET_BITRATE - 32)) {
				console.warn(`Warning: source bitrate for ${id} is low (${bitrateKbps} kbps). Consider retrying with different sources.`);
			}
		}

		// move audio to OUTPUT_DIR
		await ensureDir(OUTPUT_DIR);
		const ext = path.extname(filePath) || ".m4a";
		let dest = path.join(OUTPUT_DIR, `${id}${ext}`);
		try {
			await fs.access(dest);
			const ts = Date.now();
			dest = path.join(OUTPUT_DIR, `${id}-${ts}${ext}`);
		} catch {
			// dest not exist
		}
		const finalDest = await moveFile(filePath, dest);
		console.log(`Saved audio for ${id} -> ${finalDest}`);

		// save album art to ART_DIR
		await ensureDir(ART_DIR);
		const artPath = await saveAlbumArtToDir(tmpDir, metadata, id);
		if (artPath) {
			console.log(`Saved album art for ${id} -> ${artPath}`);
		} else {
			console.log(`No album art found for ${id}.`);
		}

		// update DB duration if available
		if (durationSec) {
			try {
				await prisma.track.update({
					where: { id },
					data: { duration: durationSec }
				});
				console.log(`Updated DB for ${id}: duration=${durationSec}s`);
			} catch (dbErr) {
				console.error(`Failed to update DB for ${id}: ${(dbErr as Error).message}`);
			}
		} else {
			console.log(`Skipped DB update for ${id} because duration couldn't be read.`);
		}
	} catch (err) {
		console.error(`Unhandled error processing ${id}: ${(err as Error).message}`);
	} finally {
		await safeRmDir(tmpDir);
	}
}

/**
 * Paging + processing loop.
 */
async function pageAndProcess() {
	const missingFilter = {
		$or: [{ duration: { $exists: false } }, { duration: -1 }, { duration: "-1" }, { duration: null }]
	};

	let skip = 0;
	const limit = pLimit(CONCURRENCY);

	while (true) {
		console.log(`raw find Track: skip=${skip} limit=${BATCH_SIZE} filter=${JSON.stringify(missingFilter)}`);
		const cmd: any = { find: "Track", filter: missingFilter, limit: BATCH_SIZE, sort: { _id: 1 }, skip };

		let res: any;
		try {
			res = await prisma.$runCommandRaw(cmd);
		} catch (e) {
			console.error("Raw mongo find failed:", (e as Error).message);
			break;
		}

		const docs: any[] = res?.cursor?.firstBatch ?? res?.documents ?? (Array.isArray(res) ? res : []);
		if (!docs || docs.length === 0) {
			console.log("No matching documents found. Exiting paging loop.");
			break;
		}

		console.log(`Fetched ${docs.length} docs (skip=${skip}). Processing up to ${CONCURRENCY} concurrently.`);
		await Promise.all(docs.map((d) => limit(() => processDoc(d))));

		skip += docs.length;
		if (docs.length < BATCH_SIZE) {
			console.log("Last page processed.");
			break;
		}
	}
}

/**
 * Entrypoint
 */
async function main() {
	try {
		await ensureDir(OUTPUT_DIR);
		await ensureDir(ART_DIR);
		console.log("Starting downloader pipeline (freyr) with separate album art dir.");
		await pageAndProcess();
		console.log("Pipeline finished.");
	} catch (err) {
		console.error("Fatal:", (err as Error).message);
	} finally {
		await prisma.$disconnect();
	}
}

main();
