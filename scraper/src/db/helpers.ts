import { prisma } from "./prisma";

export async function getUrlCache(url: string) {
	const cached = await prisma.urlCache.findUnique({
		where: { url }
	});

	if (!cached) return null;

	// Quick sanity: must be an object with items array to consider pagination merging
	const rootData = (cached.data ?? {}) as any;
	if (!rootData || !rootData.next || !Array.isArray(rootData.items)) {
		return cached;
	}

	// Iteratively walk 'next' pages and merge items (avoid recursion to prevent stack/dup issues)
	// Track visited URLs to prevent infinite loops
	const visited = new Set<string>();
	visited.add(url);

	// Keep a mutable mergedData object we will persist back to DB
	const mergedData = { ...rootData };

	// Use nextUrl variable; it may be absolute or relative depending on Spotify response
	let nextUrl: string | undefined = mergedData.next;

	// Limit iterations to a sensible max to avoid runaway loops / memory usage
	const MAX_PAGES = 200;

	let pagesSeen = 0;
	while (nextUrl && pagesSeen < MAX_PAGES && !visited.has(nextUrl)) {
		pagesSeen++;
		visited.add(nextUrl);

		// Try to read this page from the cache directly (avoid re-invoking getUrlCache to prevent recursion)
		const nextCached = await prisma.urlCache.findUnique({ where: { url: nextUrl } });
		if (!nextCached) break;

		const nextData = nextCached.data as any;
		if (!nextData || !Array.isArray(nextData.items)) break;

		// Append items safely
		mergedData.items = Array.isArray(mergedData.items) ? mergedData.items.concat(nextData.items) : Array.from(nextData.items);
		// Advance the 'next' pointer from the newly read page
		mergedData.next = nextData.next ?? null;
		nextUrl = mergedData.next ?? undefined;

		// Persist a partial update so other callers can see merged progress and to avoid re-merging same pages
		try {
			await prisma.urlCache.update({
				where: { url: cached.url },
				data: { data: mergedData }
			});
		} catch {
			// best-effort: ignore update failures. We still continue merging locally.
		}

		// If the next page is null/empty, loop will exit naturally
	}

	// If we hit MAX_PAGES we stop to avoid unbounded work
	if (pagesSeen >= MAX_PAGES) {
		// Optionally log a warning; do not throw.
		console.warn(`getUrlCache(${url}) reached MAX_PAGES (${MAX_PAGES}) while merging paginated cache.`);
	}

	// Return the merged result (fresh DB state could be returned instead if you prefer)
	return {
		...cached,
		data: mergedData
	};
}

export function setUrlCache(url: string, data: any) {
	return prisma.urlCache.upsert({
		where: { url },
		update: { data },
		create: { url, data }
	});
}
