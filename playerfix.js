const { MongoClient, ObjectId } = require('mongodb');

async function fixPlayers() {
    // MongoDB connection URLs
    const dbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the first DB

    // Collection names
    const collectionName = 'players';
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

        const cricketPlayersCollection = firstDb.collection(collectionName);
        const glacierPlayersCollection = secondDb.collection(collectionName);
        const ccPlayersCollection = thirdDb.collection('player');
        let messedUpGlacierRecords = 0, messedUpCricketRecords = 0, messedUpCCRecords = 0;
        const sort = {
            'createdAt': -1
        };

        // Fetch all records from the glacier collection
        const glacierTableRecords = await glacierPlayersCollection.find().toArray();

        for (const [index,record] of glacierTableRecords.entries()) {
            const { _id } = record;
            console.log(`Processing record ${index} of ${glacierTableRecords.length} of Glacier`);
            console.log(`Messed up records Glacier: ${messedUpGlacierRecords}, CC: ${messedUpCCRecords}, Cricker: ${messedUpCricketRecords}`);

            const cricketTableRecord = await cricketPlayersCollection.findOne({ fc_mapping_id: _id.toString() });
            const ccPlayerRecord = await ccPlayersCollection.findOne({ _id });
            if (!cricketTableRecord || !ccPlayerRecord) {
                messedUpGlacierRecords++;
                // await cricketPlayersCollection.deleteOne({ fc_mapping_id: _id.toString() });
                // await glacierPlayersCollection.deleteOne({ _id, });
                // await ccPlayersCollection.deleteOne({ _id });
            }
        }

        // Fetch all records from the cc DB
        const ccPlayerRecords = await ccPlayersCollection.find().toArray();

        for (const [index, record] of ccPlayerRecords.entries()) {
            const { _id } = record;
            console.log(`Processing record ${index} of ${ccPlayerRecords.length} of CC`);
            console.log(`Messed up records Glacier: ${messedUpGlacierRecords}, CC: ${messedUpCCRecords}, Cricker: ${messedUpCricketRecords}`);

            const glacierTableRecord = await glacierPlayersCollection.findOne({ _id });
            const cricketTableRecord = await cricketPlayersCollection.findOne({ fc_mapping_id: _id.toString() });
            if (!glacierTableRecord || !cricketTableRecord) {
                messedUpCCRecords++;
                // await ccPlayersCollection.deleteOne({ _id });
                // await glacierPlayersCollection.deleteOne({ _id });
                // await cricketPlayersCollection.deleteOne({ fc_mapping_id: _id.toString() });
            }
        }

        const cricketPlayerRecords = await cricketPlayersCollection.find().toArray();

        for (const [index, record] of cricketPlayerRecords.entries()) {
            const { _id, fc_mapping_id } = record;
            console.log(`Processing record ${index} of ${cricketPlayerRecords.length} of Cricket`);
            console.log(`Messed up records Glacier: ${messedUpGlacierRecords}, CC: ${messedUpCCRecords}, Cricker: ${messedUpCricketRecords}`);

            const glacierTableRecord = await glacierPlayersCollection.findOne({ _id: new ObjectId(fc_mapping_id) });
            const ccPlayerRecord = await ccPlayersCollection.findOne({ _id: new ObjectId(fc_mapping_id) })
            if (!glacierTableRecord || !ccPlayerRecord) {
                messedUpCricketRecords++;
                // await ccPlayersCollection.deleteOne({ _id: new ObjectId(fc_mapping_id) });
                // await glacierPlayersCollection.deleteOne({ _id: new ObjectId(fc_mapping_id) });
                // await cricketPlayersCollection.deleteOne({ _id });
            }
        }

        console.log('Update operation completed!');
        console.log(`Messed up records in glacier: ${messedUpGlacierRecords}`);
        console.log(`Messed up records in cricket: ${messedUpCricketRecords}`);
        console.log(`Messed up records in cc: ${messedUpCCRecords}`);
    } catch (err) {
        console.error('Error during update:', err);
    } finally {
        await firstClient.close();
    }
}

fixPlayers();
