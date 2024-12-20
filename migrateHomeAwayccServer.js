const { MongoClient, ObjectId } = require('mongodb');

// Connection URIs and database names
const uriFirstDB = "mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true";
const firstDBName = "fc_glacier";
const uriSecondDB = "mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true";
const secondDBName = "fanclash-staging";

// Async function to perform the migration
async function runMigration() {
    const clientFirstDB = new MongoClient(uriFirstDB);
    const clientSecondDB = new MongoClient(uriSecondDB);

    try {
        // Connect to both databases
        await clientFirstDB.connect();
        await clientSecondDB.connect();

        const firstDB = clientFirstDB.db(firstDBName);
        const secondDB = clientSecondDB.db(secondDBName);

        const firstTable = firstDB.collection("matches");
        const secondTable = firstDB.collection("rosters");
        const thirdTable = secondDB.collection("matchcommon");

        // Fetch all records from the first table
        const firstTableRecords = await firstTable.find().toArray();

        for (const record of firstTableRecords) {
            const recordId = record._id;
            const glacierClassId = recordId.toString();  // Replace with the correct field if different
            if (!record.participants || record.participants.length === 0) {
                continue;
            }
            const homeParticipant = record.participants.find(participant => participant.isHomeTeam === true);

            if (homeParticipant) {
                const rosterId = homeParticipant.rosterId;

                // Query third table for teamId based on rosterId
                const secondTableRecord = await secondTable.findOne({ _id: new ObjectId(rosterId) });

                if (secondTableRecord) {
                    const teamId = secondTableRecord.teamId;

                    // Update the second table with the homeTeam using teamId
                    const count = await thirdTable.countDocuments(
                        { glacierMatchIds: { $in: [glacierClassId] } },
                    );
                    if(count > 1) {
                        console.warn("More than one record found for matchcommon: " + secondTableRecord?._id.toString());
                    }
                    const isUpdated = await thirdTable.updateMany(
                        { glacierMatchIds: { $in: [glacierClassId] } },
                        { $set: { homeTeam: teamId } }
                    );
                    console.log(isUpdated.matchedCount);
                    const updatedRecord = await thirdTable.findOne(
                        { glacierMatchIds: { $in: [glacierClassId] } },
                    );
                    if (!updatedRecord?.squads || updatedRecord.squads.length === 0) {
                        console.warn('record not found');
                        continue;
                    }
                    if (!updatedRecord.squads.includes(teamId)) {
                        console.warn("Team id not exists for matchcommon: " + updatedRecord?._id.toString());
                        continue;
                    }

                    console.log(`Updated second table record with homeTeam: ${teamId} for glacierClassId: ${glacierClassId}`);
                } else {
                    console.log(`No teamId found for rosterId: ${rosterId}`);
                }
            } else {
                console.log(`No home team participant found for recordId: ${recordId}`);
            }
        }
    } finally {
        await clientFirstDB.close();
        await clientSecondDB.close();
    }
}

runMigration().catch(console.error);
