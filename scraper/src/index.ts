/**
 * 1- Get all artists in the given array
 * 2- Get all albums for each artist
 * 3- Get all tracks for each album
 * 4- Upsert artists for each track
 */

import { prisma } from "./db/prisma";
import { ResolvablesQueue } from "./lib/resolvableQueue";

const resolvablesQueue = new ResolvablesQueue();

let count = 0;
while (true) {
	if (resolvablesQueue.hasNext()) {
		try {
			console.log(count + " - Processing next resolvable...");
			await resolvablesQueue.process();
			count++;
		} catch (error) {
			resolvablesQueue.queue.clear(); // Clear the queue to avoid infinite loops on errors
			console.error(`Error processing resolvable: ${error}`);
		}
	} else {
		const unresolvedArtists = await prisma.artist.findMany({
			where: {
				hasCrawlCompleted: false
			}
		});
		for (const artist of unresolvedArtists) {
			resolvablesQueue.add(`spotify:artist:${artist.spotifyId}`);
		}
	}
}
