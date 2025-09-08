// get-tracks.js
import fs from "fs";
import fetch from "node-fetch"; // if Node <18, install: npm install node-fetch
import dotenv from "dotenv";
import client, {getUrlCache,saveArtistIfDoesNotExist,setUrlCache} from './database.js';

// For env from .env
dotenv.config();

let trackCount = 0;

// === CONFIG ===
const clientId = process.env.CLIENT_ID;
const clientSecret = process.env.CLIENT_SECRET;

console.log("Client ID:", clientId ? clientId : "Missing");
console.log("Client Secret:", clientSecret ? clientSecret : "Missing");

if (!clientId || !clientSecret) {
	process.exit(1);
}

import artists from "./artists.json" with { type: "json" };

// === AUTHENTICATE ===
async function getAccessToken() {
	const response = await fetch("https://accounts.spotify.com/api/token", {
		method: "POST",
		headers: {
			Authorization:
				"Basic " + Buffer.from(clientId + ":" + clientSecret).toString("base64"),
			"Content-Type": "application/x-www-form-urlencoded",
		},
		body: "grant_type=client_credentials",
	});

	const data = await response.json();
	return data.access_token;
}

// === GLOBAL RATE LIMIT STATE ===
let rateLimitUntil = 0;

// === FETCH WITH RETRY (rate-limit aware) ===
async function fetchWithRetry(url, options, retries = 5, backoff = 2000) {

	const cached = await getUrlCache(url);

	if (cached) {
		return cached.data;
	}

	let attempt = 0;
	while (true) {
		// If we're still inside a rate limit window, wait it out
		const now = Date.now();
		if (rateLimitUntil > now) {
			const waitMs = rateLimitUntil - now;
			console.warn(
				`Global rate-limit active. Waiting ${Math.ceil(waitMs / 1000)}s...`
			);
			await new Promise((resolve) => setTimeout(resolve, waitMs));
		}

		try {
			const response = await fetch(url, options);

			// Handle rate limiting
			if (response.status === 429) {
				const retryAfter = response.headers.get("retry-after");
				const waitSeconds = retryAfter ? parseInt(retryAfter, 10) : 3;
				rateLimitUntil = Date.now() + waitSeconds * 1000; // block globally
				console.warn(
					`Rate limited. All requests paused for ${waitSeconds}s...`
				);
				continue; // retry same request after wait
			}

			// Retry for transient errors
			if (!response.ok) {
				attempt++;
				if (attempt > retries) {
					throw new Error(
						`Failed after ${retries} retries: ${response.status} ${response.statusText}`
					);
				}
				console.warn(
					`Error ${response.status}. Retrying in ${
						backoff / 1000
					}s (attempt ${attempt}/${retries})...`
				);
				await new Promise((resolve) => setTimeout(resolve, backoff));
				backoff *= 2;
				continue;
			}

			// Success
			const responseData = await response.json();
			if (!responseData.error) {
				await setUrlCache(url, responseData);
			}
			return responseData;
		} catch (err) {
			// Handle network errors
			attempt++;
			if (attempt > retries) {
				throw new Error(
					`Network error after ${retries} retries: ${err.message}`
				);
			}
			console.warn(
				`Network error: ${err.message}. Retrying in ${
					backoff / 1000
				}s (attempt ${attempt}/${retries})...`
			);
			await new Promise((resolve) => setTimeout(resolve, backoff));
			backoff *= 2;
		}
	}
}

// === GET ARTIST TRACKS ===
async function getArtistTracks(token, artistId) {
	let tracks = [];

	// 1. Get all albums
	let url = `https://api.spotify.com/v1/artists/${artistId}/albums?include_groups=album,single&limit=50`;
	while (url) {
		const data = await fetchWithRetry(url, {
			headers: { Authorization: "Bearer " + token },
		});

		if (Array.isArray(data.items)) {
			for (const album of data.items) {
				// 2. For each album, get tracks
				let albumUrl = `https://api.spotify.com/v1/albums/${album.id}/tracks?limit=50`;
				while (albumUrl) {
					const albumData = await fetchWithRetry(albumUrl, {
						headers: { Authorization: "Bearer " + token },
					});

					if (Array.isArray(albumData.items)) {
						albumData.items.forEach((track) => {
							console.log(`Found track ${trackCount}: ${track.name}`);
							tracks.push(`https://open.spotify.com/track/${track.id}`);
							trackCount++;
						});
					} else {
						console.warn("No items found for album:", album.id, albumData);
					}

					albumUrl = albumData.next; // paginate
				}
			}
		} else {
			console.warn("No albums found for artist:", artistId, data);
		}

		url = data.next; // paginate
	}

	return [...new Set(tracks)]; // remove duplicates
}

// === MAIN ===
(async () => {
	try {
		const token = await getAccessToken();
		console.log("Fetched access token ✅");

		for (const [artistName, artistUrl] of Object.entries(artists)) {
			const artistId = artistUrl.split("/artist/")[1].split("?")[0];
			const tracks = await getArtistTracks(token, artistId);
			console.log(`Fetched ${tracks.length} tracks for ${artistName} ✅`);

			for (const track of tracks) {
				//Save artist data
				for(const artist of track.artists){
					await saveArtistIfDoesNotExist(artist.id, artist);
				}

				
			}

			const chunkSize = 50;
			const safeArtistName = artistName.replace(/\s+/g, "_");

			for (let i = 0; i < tracks.length; i += chunkSize) {
				const chunk = tracks.slice(i, i + chunkSize);
				const fileName = `Assets/${safeArtistName}-segment${
					i / chunkSize + 1
				}.txt`;
				fs.writeFileSync(fileName, chunk.join("\n"), "utf8");
				console.log(`Saved ${chunk.length} tracks to ${fileName}`);
			}
		}

		console.log("All track batch files saved ✅");
	} catch (err) {
		console.error("Error:", err);
	}
})();