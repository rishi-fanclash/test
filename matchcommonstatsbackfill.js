const { MongoClient, ObjectId } = require('mongodb');

async function matchstatsbackfill() {
    // MongoDB connection URLs
    const dbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the first DB

    // Collection names
    const thirdDbName = 'fanclash-staging';

    // Connect to the databases
    const firstClient = new MongoClient(dbUrl);

    try {
        await firstClient.connect();

        const fanclashStaging = firstClient.db(thirdDbName);

        const squadInMatchStats = fanclashStaging.collection('squadinmatchstats');
        const matchCommon = fanclashStaging.collection('matchcommon');
        const match = fanclashStaging.collection('match');

        const squadInMatchStatsRecords = await squadInMatchStats.find().toArray();

        for (const [index, record] of squadInMatchStatsRecords.entries()) {
            console.log(`Processing record ${index} of ${squadInMatchStatsRecords.length} of squadInMatchStats`);
            const matchId = record.match;
            const squadId = record.squad;
            if(!matchId || !squadId) {
                console.warn(`Match ${matchId} or squad ${squadId} not found`);
                continue;
            }
            const matchRecord = await match.findOne({ _id: new ObjectId(matchId) });
            if(!matchRecord) {
                console.warn(`Match record not found for id: ${matchId}`);
                continue;
            }
            const matchCommonRecords = await matchCommon.find({_id: {$in: matchRecord.matchCommonIds.map(matchCommonId => new ObjectId(matchCommonId))}}).toArray();
            if(!matchCommonRecords || matchCommonRecords.length === 0) {
                console.warn(`Match common record not found for match ${matchId}`);
                continue;
            }
            let updatedCount = 0;
            for (const matchCommonRecord of matchCommonRecords) {
                if(matchCommonRecord.squads.includes(squadId)) {
                    const updatedRecord = await squadInMatchStats.updateOne(
                        { _id: record._id },
                        { $set: { matchCommonId: matchCommonRecord._id.toString() } }
                    );
                    if(updatedRecord.matchedCount!== 1) {
                        console.warn("Record not updated for squadInMatchStats id: "+ record._id.toString());
                    }
                    updatedCount++;
                    break;
                }
            }
            if(updatedCount!==1) {
                console.warn(`not updated record for ${record._id.toString()}`);
            }
        }
    } catch (err) {
        console.error('Error during update:', err);
    } finally {
        await firstClient.close();
    }
}

matchstatsbackfill();
