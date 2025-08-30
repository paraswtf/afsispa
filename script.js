// get-tracks.js
import fs from "fs";
import fetch from "node-fetch"; // if Node <18, install: npm install node-fetch
import { exec } from "child_process";

// === CONFIG ===
const clientId = "c6f9e1f89bf44ab0888ced219203d6bd";
const clientSecret = "f1751cf250d54a78881e5892d8be3883";

const artists = {
	"Lana Del Ray": "https://open.spotify.com/artist/00FQb4jTyendYWaN8pK0wa",
	"Robert Grace": "https://open.spotify.com/artist/6W8rk6H6C3Mcj0lALuLVg1",
	"Justin Bieber": "https://open.spotify.com/artist/1uNFoZAHBGtllmzznpCI3s",
	"Alessia Cara": "https://open.spotify.com/artist/2wUjUUtkb5lvLKcGKsKqsR",
	"Alec Benjamin": "https://open.spotify.com/artist/5IH6FPUwQTxPSXurCrcIov",
	"keshi": "https://open.spotify.com/artist/3pc0bOVB5whxmD50W79wwO",
	"Christian French": "https://open.spotify.com/artist/7naAJDAh7AZnf18YYfQruM",
	"Ritviz": "https://open.spotify.com/artist/72beYOeW2sb2yfcS4JsRvb",
	"Billie Eilish": "https://open.spotify.com/artist/6qqNVTkY8uBg9cP3Jd7DAH",
	"Salena Gomez": "https://open.spotify.com/artist/0C8ZW7ezQVs4URX5aX7Kqx",
	"Dua Lipa": "https://open.spotify.com/artist/6M2wZ9GZgrQXHCFfjv46we",
	"The Chainsmokers": "https://open.spotify.com/artist/69GGBxA162lTqCwzJG5jLp",
	"Ariana Grande": "https://open.spotify.com/artist/66CXWjxzNUsdJxJ2JdwvnR",
	"Marshmello": "https://open.spotify.com/artist/64KEffDW9EtZ1y2vBYgq8T",
	"Drake": "https://open.spotify.com/artist/3TVXtAsR1Inumwj472S9r4",
	"Anne-Marie": "https://open.spotify.com/artist/1zNqDE7qDGCsyzJwohVaoX"
};

// === AUTHENTICATE ===
async function getAccessToken() {
	const response = await fetch("https://accounts.spotify.com/api/token", {
		method: "POST",
		headers: {
			"Authorization": "Basic " + Buffer.from(clientId + ":" + clientSecret).toString("base64"),
			"Content-Type": "application/x-www-form-urlencoded"
		},
		body: "grant_type=client_credentials"
	});

	const data = await response.json();
	return data.access_token;
}

// === FETCH WITH RETRY ===
async function fetchWithRetry(url, options) {
	while (true) {
		const response = await fetch(url, options);
		if (response.status !== 429) {
			return response;
		}
		const retryAfter = response.headers.get("retry-after");
		const waitSeconds = retryAfter ? parseInt(retryAfter, 10) : 3;
		console.warn(`Rate limited. Retrying after ${waitSeconds} seconds...`);
		await new Promise((resolve) => setTimeout(resolve, waitSeconds * 1000));
	}
}

// === GET ARTIST TRACKS ===
async function getArtistTracks(token, artistId) {
	let tracks = [];

	// 1. Get all albums
	let url = `https://api.spotify.com/v1/artists/${artistId}/albums?include_groups=album,single&limit=50`;
	while (url) {
		const res = await fetchWithRetry(url, {
			headers: { Authorization: "Bearer " + token }
		});
		const data = await res.json();

		if (Array.isArray(data.items)) {
			for (const album of data.items) {
				// 2. For each album, get tracks
				let albumUrl = `https://api.spotify.com/v1/albums/${album.id}/tracks?limit=50`;
				while (albumUrl) {
					const albumRes = await fetchWithRetry(albumUrl, {
						headers: { Authorization: "Bearer " + token }
					});
					const albumData = await albumRes.json();

					if (Array.isArray(albumData.items)) {
						albumData.items.forEach((track) => {
							console.log(`Found track: ${track.name}`);
							tracks.push(`https://open.spotify.com/track/${track.id}`);
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

		const artistTasks = Object.entries(artists).map(async ([artistName, artistUrl]) => {
			const artistId = artistUrl.split("/artist/")[1].split("?")[0];
			const tracks = await getArtistTracks(token, artistId);
			console.log(`Fetched ${tracks.length} tracks for ${artistName} ✅`);

			const chunkSize = 50;
			const tasks = [];
			const safeArtistName = artistName.replace(/\s+/g, "_");

			for (let i = 0; i < tracks.length; i += chunkSize) {
				const chunk = tracks.slice(i, i + chunkSize);
				const fileName = `Assets/${safeArtistName}-segment${i / chunkSize + 1}.txt`;
				fs.writeFileSync(fileName, chunk.join("\n"), "utf8");
				console.log(`Saved ${chunk.length} tracks to ${fileName}`);

				const outputDir = `./Dataset/${safeArtistName}`;
				fs.mkdirSync(outputDir, { recursive: true });
				const outputFilePath = `${outputDir}/${fileName.split("/").pop()}`;
				if (fs.existsSync(outputFilePath)) {
					console.log(`Skipping ${fileName}, already exists in ${outputDir}`);
					continue;
				}
				const cmd = `freyr -i ${fileName} -d ${outputDir}`;
				tasks.push(
					new Promise((resolve, reject) => {
						const process = exec(cmd, (error, stdout, stderr) => {
							if (error) {
								console.error(`Error with ${fileName}:`, error);
								reject(error);
							} else {
								console.log(`Finished processing ${fileName}`);
								resolve();
							}
						});
					})
				);
			}

			await Promise.all(tasks);
		});

		await Promise.all(artistTasks);
		console.log("All downloads completed for all artists ✅");
	} catch (err) {
		console.error("Error:", err);
	}
})();
