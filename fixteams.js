const { MongoClient, ObjectId } = require('mongodb');

async function fixTeams() {
    // MongoDB connection URLs
    const dbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the first DB

    // Collection names
    const collectionName = 'teams';
    const firstDBName = "fc_cricket";
    const secondDBName = "fc_glacier";
    const thirdDbName = 'fanclash-staging';

    // Connect to the databases
    const firstClient = new MongoClient(dbUrl);

    try {
        await firstClient.connect();

        const firstDb = firstClient.db(firstDBName);
        const secondDb = firstClient.db(secondDBName);
        const thirdDb = firstClient.db(thirdDbName);

        const glacierTeamsCollection = secondDb.collection(collectionName);
        const ccTeamsCollection = thirdDb.collection('squad');
        const cricketPlayersCollection = firstDb.collection('players');
        const glacierPlayersCollection = secondDb.collection('players');
        const ccPlayersCollection = thirdDb.collection('player');

        // Fetch all records from the glacier collection
        const glacierTeamRecords = await glacierTeamsCollection.find().toArray();

        for (const [index,record] of glacierTeamRecords.entries()) {
            console.log(`Processing record ${index} of ${glacierTeamRecords.length} of Glacier`);
            if (!record.playerIds || record.playerIds.length === 0) {
                continue;
            }
            const correctedPlayerIds = [];
            for (const playerId of record.playerIds) {
                const glacierPlayerRecord = await glacierPlayersCollection.findOne({ _id: new ObjectId(playerId) });
                if (glacierPlayerRecord) {
                    correctedPlayerIds.push(playerId);
                } else {
                    await cricketPlayersCollection.deleteOne({ fc_mapping_id: playerId });
                    await ccPlayersCollection.deleteOne({ _id: new ObjectId(playerId) });
                }
            }
            await glacierTeamsCollection.updateOne({ _id: record._id }, {
                $set: {
                    playerIds: correctedPlayerIds
                }
            });
        }

        // Fetch all records from the cc collection
        const ccTeamRecords = await ccTeamsCollection.find().toArray();

        for (const [index, record] of ccTeamRecords.entries()) {
            console.log(`Processing record ${index} of ${ccTeamRecords.length} of CC`);
            if (!record.players || record.players.length === 0) {
                continue;
            }
            const correctedPlayerIds = [];
            for (const playerId of record.players) {
                const ccPlayerRecord = await ccPlayersCollection.findOne({ _id: new ObjectId(playerId) });
                if (ccPlayerRecord) {
                    correctedPlayerIds.push(playerId);
                } else {
                    await cricketPlayersCollection.deleteOne({ fc_mapping_id: playerId });
                    await glacierPlayersCollection.deleteOne({ _id: new ObjectId(playerId) });
                }
            }
            await ccTeamsCollection.updateOne({ _id: record._id }, {
                $set: {
                    players: correctedPlayerIds
                }
            });
        }

        console.log('Update operation completed!');
    } catch (err) {
        console.error('Error during update:', err);
    } finally {
        await firstClient.close();
    }
}

fixTeams();
