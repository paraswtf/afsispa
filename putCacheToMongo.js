import db from './database.js';
import cache from './cache.json' with { type: "json" };

//Loop through cache obj with keys and insert to mongodb
const insertDocs = [];
for (const [key, value] of Object.entries(cache)) {
    const doc = { url: key, data: value };
    insertDocs.push({
        updateOne: {
            filter: { url: key },
            update: { $set: doc },
            upsert: true
        }
    });
}

await db.db('tuney').collection('spotifycache').bulkWrite(insertDocs);
console.log(`${insertDocs.length} documents inserted/updated.`);

await db.close();
console.log('All cache data has been inserted/updated to MongoDB.');
process.exit(0);