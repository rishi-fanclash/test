const { MongoClient, ObjectId } = require('mongodb');

async function backfillmatchcommonstatus() {
    // MongoDB connection URLs
    const dbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the first DB

    // Collection names
    const collectionName = 'matchcommon';
    const dbName = 'fanclash-staging';
    const thirdDBName = "fc_cricket";
    const secondDBName = "fc_glacier";

    // Connect to the databases
    const client = new MongoClient(dbUrl);

    try {
        await client.connect();

        const firstDb = client.db(dbName);
        const glacier = client.db(secondDBName);
        const cricket = client.db(thirdDBName);

        const matchcommon = firstDb.collection(collectionName);
        var oneWeekFromNow = Date.now() - 7 * 24 * 60 * 60 * 1000;
        // const matchcommonRecords = await matchcommon.find().toArray();
        // for (const matchcommonRecord of matchcommonRecords) {
        //     const glacierId = 
        // }

        const updatedRecords = await matchcommon.updateMany(
            { startTime: { $lt: oneWeekFromNow }},
            { $set: { matchCommonStatus: "COMPLETED" } }
        );
        console.log(updatedRecords);
        console.log('Update operation completed!');
    } catch (err) {
        console.error('Error during update:', err);
    } finally {
        await client.close();
    }
}

backfillmatchcommonstatus();
