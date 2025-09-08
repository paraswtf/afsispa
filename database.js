import mongodb from "mongodb";

const connectionString = process.env.MONGODB_URI || "mongodb://localhost:27017";

const client = new mongodb.MongoClient(connectionString, {
	useNewUrlParser: true,
	useUnifiedTopology: true
});

export default client;

//Caching URLS
export function getUrlCache(url) {
	return client.db("tuney").collection("spotifycache").findOne({ url });
}
export function setUrlCache(url, data) {
	return client.db("tuney").collection("spotifycache").updateOne({ url }, { $set: { data } }, { upsert: true });
}

//Saving artists
export function saveArtistIfDoesNotExist(artistId, artistData) {
	return client
		.db("tuney")
		.collection("artists")
		.insertOne({ _id: artistId, ...artistData });
}

export function getArtist(artistId) {
	return client.db("tuney").collection("artists").findOne({ id: artistId });
}

//Save multiple tracks if they don't exist by trackId
export function saveTracksIfDoNotExist(tracks) {
	return client
		.db("tuney")
		.collection("tracks")
		.insertMany(
			tracks.map((track) => ({ _id: track.id, ...track })),
			{ ordered: false }
		);
}
