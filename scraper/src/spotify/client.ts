// src/lib/spotify.ts
import { fetchSpotifyJSON } from "../lib/http";

const cachedToken: {
	token: string | null;
	expiry: number;
} = {
	token: null,
	expiry: 0 // epoch ms
};

export async function getSpotifyToken(): Promise<string> {
	const now = Date.now();

	if (cachedToken.token && now < cachedToken.expiry - 60_000) {
		// still valid (with 60s buffer)
		return cachedToken.token;
	}

	const response = await fetch("https://accounts.spotify.com/api/token", {
		method: "POST",
		headers: {
			"Authorization": "Basic " + Buffer.from(`${process.env.SPOTIFY_CLIENT_ID}:${process.env.SPOTIFY_CLIENT_SECRET}`).toString("base64"),
			"Content-Type": "application/x-www-form-urlencoded"
		},
		body: "grant_type=client_credentials"
	});

	if (!response.ok) {
		throw new Error(`Failed to fetch Spotify token: ${response.status} ${response.statusText}`);
	}

	const data: { access_token: string; expires_in: number } = await response.json();

	cachedToken.token = data.access_token;
	cachedToken.expiry = now + data.expires_in * 1000;

	return cachedToken.token;
}

// Reuse your existing types/utilities:
export type SpotifyEntityType = "artist" | "album" | "track" | "playlist" | "show" | "episode";
export type Resolvable = string;
const BASE62_22 = /^[A-Za-z0-9]{22}$/;

export function resolveToId(resolvable: Resolvable, expectedType?: SpotifyEntityType): { id: string; type?: SpotifyEntityType } {
	const raw = String(resolvable).trim();

	if (BASE62_22.test(raw)) return { id: raw, type: expectedType };

	if (raw.startsWith("spotify:")) {
		const [_, type, id] = raw.split(":");
		if (!id || !BASE62_22.test(id)) throw new Error(`Invalid Spotify URI: ${raw}`);
		if (expectedType && type !== expectedType) throw new Error(`Expected ${expectedType} but got ${type}`);
		return { id, type: type as SpotifyEntityType };
	}

	if (raw.startsWith("http://") || raw.startsWith("https://")) {
		const u = new URL(raw);
		if (!/\.?spotify\.com$/.test(u.hostname)) throw new Error(`Unsupported host: ${u.hostname}`);
		const [type, id] = u.pathname.split("/").filter(Boolean);
		if (!id || !BASE62_22.test(id)) throw new Error(`Invalid Spotify URL: ${raw}`);
		if (expectedType && type !== expectedType) throw new Error(`Expected ${expectedType} but got ${type}`);
		return { id, type: type as SpotifyEntityType };
	}

	throw new Error(`Unable to resolve Spotify identifier: ${raw}`);
}

/** Get Artist (typed) */
export async function getArtist(resolvable: Resolvable): Promise<SpotifyApi.ArtistObjectFull> {
	const { id } = resolveToId(resolvable, "artist");
	const url = `https://api.spotify.com/v1/artists/${id}`;
	return fetchSpotifyJSON(url) as Promise<SpotifyApi.ArtistObjectFull>;
}

/** Get Album (typed) */
export async function getAlbum(resolvable: Resolvable): Promise<SpotifyApi.AlbumObjectFull> {
	const { id } = resolveToId(resolvable, "album");
	const url = `https://api.spotify.com/v1/albums/${id}`;
	return fetchSpotifyJSON(url) as Promise<SpotifyApi.AlbumObjectFull>;
}

/** Get All Album Tracks (typed) loop paginated*/
export async function getAlbumTracks(resolvable: Resolvable): Promise<SpotifyApi.TrackObjectSimplified[]> {
	const { id } = resolveToId(resolvable, "album");
	const url = `https://api.spotify.com/v1/albums/${id}/tracks?limit=50`;
	const tracks: SpotifyApi.TrackObjectSimplified[] = [];
	let nextUrl: string | null = url;

	while (nextUrl) {
		const response = (await fetchSpotifyJSON(nextUrl)) as SpotifyApi.AlbumTracksResponse;
		tracks.push(...response.items);
		nextUrl = response.next;
	}

	return tracks;
}

/** Get All Artist Albums (typed) loop paginated*/
export async function getArtistAlbums(resolvable: Resolvable): Promise<SpotifyApi.AlbumObjectSimplified[]> {
	const { id } = resolveToId(resolvable, "artist");
	const url = `https://api.spotify.com/v1/artists/${id}/albums`;
	const albums: SpotifyApi.AlbumObjectSimplified[] = [];
	let nextUrl: string | null = url;

	while (nextUrl) {
		const response = (await fetchSpotifyJSON(nextUrl)) as SpotifyApi.ArtistsAlbumsResponse;
		albums.push(...response.items);
		nextUrl = response.next;
	}

	return albums;
}

/** Get Track (typed) */
export async function getTrack(resolvable: Resolvable): Promise<SpotifyApi.TrackObjectFull> {
	const { id } = resolveToId(resolvable, "track");
	const url = `https://api.spotify.com/v1/tracks/${id}`;
	return fetchSpotifyJSON(url) as Promise<SpotifyApi.TrackObjectFull>;
}

/** (Optional) Get several albums at once (typed) */
export async function getSeveralAlbums(resolvables: Resolvable[]): Promise<SpotifyApi.MultipleAlbumsResponse> {
	const ids = resolvables.map((r) => resolveToId(r, "album").id);
	const url = `https://api.spotify.com/v1/albums?ids=${encodeURIComponent(ids.join(","))}`;
	return fetchSpotifyJSON(url) as Promise<SpotifyApi.MultipleAlbumsResponse>;
}

/** (Optional) Get several tracks at once (typed) */
export async function getSeveralTracks(resolvables: Resolvable[]): Promise<SpotifyApi.MultipleTracksResponse> {
	const ids = resolvables.map((r) => resolveToId(r, "track").id);
	const url = `https://api.spotify.com/v1/tracks?ids=${encodeURIComponent(ids.join(","))}`;
	return fetchSpotifyJSON(url) as Promise<SpotifyApi.MultipleTracksResponse>;
}
