const fs = require("fs");
const path = require("path");
const asciichart = require("asciichart");

const datasetRoot = "NewDataset";
const intervalMs = 5000;

let prevCount = -1;
let historyCounts = [];
let historyNew = [];
let historyFps = [];

function countTracksRecursive(dir) {
	let trackCount = 0;
	try {
		const entries = fs.readdirSync(dir, { withFileTypes: true });
		for (const entry of entries) {
			const entryPath = path.join(dir, entry.name);
			if (entry.isDirectory()) {
				trackCount += countTracksRecursive(entryPath);
			} else if (entry.isFile()) {
				trackCount++;
			}
		}
	} catch (err) {
		console.error("Error reading directory:", dir, err);
	}
	return trackCount;
}

setInterval(() => {
	const count = countTracksRecursive(datasetRoot);
	if (prevCount === -1) {
		prevCount = count;
		return;
	}
	const newTracks = count - prevCount;
	const fps = newTracks / (intervalMs / 1000);

	// store values
	historyCounts.push(count);
	historyNew.push(newTracks);
	historyFps.push(fps);

	// limit history length
	if (historyCounts.length > 50) {
		historyCounts.shift();
		historyNew.shift();
		historyFps.shift();
	}

	console.clear();
	console.log(`Track count: ${count}`);
	console.log(`New Tracks: ${newTracks}`);
	console.log(`FPS: ${fps.toFixed(2)}\n`);

	console.log("Total Tracks:");
	console.log(asciichart.plot(historyCounts, { height: 10 }));

	console.log("\nNew Tracks per Interval:");
	console.log(asciichart.plot(historyNew, { height: 10 }));

	console.log("\nFPS (new tracks/sec):");
	console.log(asciichart.plot(historyFps, { height: 10 }));

	prevCount = count;
}, intervalMs);
