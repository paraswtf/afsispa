import cuid2 from "@paralleldrive/cuid2";
import { getAlbumTracks, getArtist, getArtistAlbums, Resolvable, resolveToId } from "../spotify/client";
import { prisma } from "../db/prisma";

const refetchAfter = 1000 * 60 * 60 * 24; //24 hours

export class ResolvablesQueue {
	queue: Set<Resolvable>;

	constructor() {
		this.queue = new Set<Resolvable>();
	}

	add(resolvable: Resolvable) {
		this.queue.add(resolvable);
	}

	async process() {
		while (this.queue.size > 0) {
			const resolvable = Array.from(this.queue)[0];
			if (!resolvable) break;
			this.queue.delete(resolvable);

			await this.resolve(resolvable);
		}
	}

	async resolve(resolvable: Resolvable) {
		const resolved = resolveToId(resolvable);
		console.log(`Resolving ${resolved.type}: ${resolved.id}`);

		const data = await prisma.artist.findUnique({
			where: { spotifyId: resolved.id }
		});

		if (data && data.updatedAt.getTime() > Date.now() - refetchAfter && data.hasCrawlCompleted) {
			// console.log(`Skipped ${resolved.type}: ${resolved.id}`);
			return; // Skip processing if data is fresh
		}

		//Get artist albums
		console.log(`Getting Albums for ${resolved.type}: ${resolved.id}`);
		const apiAlbums = await getArtistAlbums(resolved.id);
		const artistData = apiAlbums[0]?.artists.find((a) => a.id === resolved.id) ?? (await getArtist(resolved.id));
		const id = cuid2.createId();
		const otherArtists = apiAlbums[0]?.artists.filter((a) => a.id !== resolved.id) || [];
		for (const otherArtist of otherArtists) {
			console.log(`Added other artist: ${otherArtist.id}`);
			this.add(otherArtist.uri);
		}

		console.log(`Upserting ${resolved.type}: ${resolved.id}`);
		// Upsert artist into database
		await prisma.artist.upsert({
			where: { spotifyId: artistData.id },
			update: {
				updatedAt: new Date(Date.now())
			},
			create: {
				id,
				name: artistData.name,
				spotifyId: artistData.id,
				uri: `tuney:artist:${id}`,
				type: artistData.type,
				updatedAt: new Date(Date.now()),
				createdAt: new Date(Date.now())
			}
		});

		// console.log("Artist details:", artistData);

		for (const apiAlbum of apiAlbums) {
			//Check if album exists in database and has same amount of tracks
			const dbAlbum = await prisma.album.findUnique({
				where: { spotifyId: apiAlbum.id }
			});

			if (dbAlbum && dbAlbum.total_tracks === apiAlbum.total_tracks) {
				// console.log(`Album up to date, skipping: ${apiAlbum.name} by ${apiAlbum.artists.map((a) => a.name).join(", ")}`);
				continue; //Album is up to date
			}

			//Process the album
			const albumTracks = await getAlbumTracks(apiAlbum.uri);

			if (!dbAlbum) {
				//Ensure all artists exist in db, create if they don't exist
				for (const artist of apiAlbum.artists) {
					const artistId = cuid2.createId();
					await prisma.artist.upsert({
						where: { spotifyId: artist.id },
						update: {},
						create: {
							id: artistId,
							name: artist.name,
							spotifyId: artist.id,
							uri: `tuney:artist:${artistId}`,
							type: artist.type,
							updatedAt: new Date(Date.now()),
							createdAt: new Date(Date.now())
						}
					});
				}

				const id = cuid2.createId();
				await prisma.album.create({
					data: {
						id,
						spotifyId: apiAlbum.id,
						name: apiAlbum.name,
						uri: `tuney:album:${id}`,
						type: apiAlbum.type,
						total_tracks: apiAlbum.total_tracks,
						available_markets: apiAlbum.available_markets,
						album_type: apiAlbum.album_type,
						album_group: apiAlbum.album_group,
						release_date: apiAlbum.release_date,
						release_date_precision: apiAlbum.release_date_precision,
						images: {
							create: apiAlbum.images.map((image) => ({
								url: image.url,
								height: image.height,
								width: image.width
							}))
						},
						createdAt: new Date(Date.now()),
						updatedAt: new Date(Date.now()),
						tracks: {
							create: albumTracks.map((track) => {
								const id = cuid2.createId();
								return {
									id,
									spotifyId: track.id,
									uri: `tuney:track:${id}`,
									name: track.name,
									createdAt: new Date(Date.now()),
									updatedAt: new Date(Date.now())
								};
							})
						},
						albumRelation: {
							createMany: {
								data: apiAlbum.artists.map((artist) => ({
									id: cuid2.createId(),
									artistId: artist.id,
									createdAt: new Date(Date.now()),
									updatedAt: new Date(Date.now())
								}))
							}
						}
					}
				});
			} else {
				//Album exists but tracks are missing
				//Find what tracks are missing and add
				const existingTracks = await prisma.track.findMany({
					where: { albumId: dbAlbum.id }
				});
				const missingTracks = albumTracks.filter((track) => !existingTracks.some((t) => t.spotifyId === track.id));

				prisma.album.update({
					where: { id: dbAlbum.id },
					data: {
						tracks: {
							createMany: {
								data: missingTracks.map((track) => {
									const id = cuid2.createId();
									return {
										id,
										spotifyId: track.id,
										uri: `tuney:track:${id}`,
										name: track.name,
										album: {
											connect: { spotifyId: apiAlbum.id }
										},
										createdAt: new Date(Date.now()),
										updatedAt: new Date(Date.now())
									};
								})
							}
						}
					}
				});
			}
			// console.log("Album tracks:", albumTracks);
		}

		await prisma.artist.update({
			where: { spotifyId: resolved.id },
			data: {
				hasCrawlCompleted: true
			}
		});
	}

	hasNext() {
		return this.queue.size > 0;
	}
}
