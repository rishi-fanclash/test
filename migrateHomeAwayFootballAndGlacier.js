const { MongoClient, ObjectId } = require('mongodb');

// Connection URIs and database names
const uriFirstDB = "mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true";
const firstDBName = "fc-football";
const uriSecondDB = "mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true";
const secondDBName = "fc_glacier";

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

    // Collections
    const footballMatches = firstDB.collection("matches"); // Replace with actual name
    const glacierRosters = secondDB.collection("rosters"); // Replace with actual roster table name
    const glacierMatches = secondDB.collection("matches"); // Replace with actual match table name
    const footballTeams = firstDB.collection("teams");

    // Step 1: Retrieve the "home" value from the first table
        const footballMatchesRecords = await footballMatches.find({
            "fc_mapping_id": "66bc9cc3d207d9b9de695bf3"
        }).toArray();

        for (const footballMatchRecord of footballMatchesRecords) {
            const homeValue = footballMatchRecord.home;
            const teamRecord = await footballTeams.findOne({ key: homeValue });

            if (!homeValue) {
                console.warn(`Home value missing for record ID: ${footballMatchRecord._id}`);
                continue;
            }

            // Step 3: Update the `isHomeTeam` field in the match table based on the rosterId
            const matchRecord = await glacierMatches.findOne({_id: new ObjectId(footballMatchRecord.fc_mapping_id) });
            if (!matchRecord) {
                console.warn(`Match record not found for record ID: ${footballMatchRecord._id}`);
                continue;
            }
            if (!matchRecord.participants) {
                console.warn(`Match record participants not found for record ID: ${footballMatchRecord._id}`);
                continue;
            }
            const rosterIds = matchRecord.participants.map(participant => new ObjectId(participant.rosterId));

            // Step 2: Find the matching team in the roster table and get the rosterId
            const rosterRecords = await glacierRosters.find({ _id: { $in: rosterIds } }).toArray();
            if (!rosterRecords || !rosterRecords.length === 0) {
                console.warn(`Matching teamId not found in the roster table for home value: ${homeValue}`);
                continue;
            }
            const rosterId = (rosterRecords.find(r => r.teamId === teamRecord.fc_mapping_id))._id.toString();

                const updatedParticipants = matchRecord.participants.map(participant => ({
                    ...participant,
                    isHomeTeam: participant.rosterId === rosterId
                }));

                // Update the match record with the modified participants array
                await glacierMatches.updateOne(
                    { _id: matchRecord._id },
                    { $set: { participants: updatedParticipants } }
                );

            console.log(`Processed home team updates for record ID: ${footballMatchRecord._id}`);
        }

        console.log("Migration completed for all records in the first table.");
    } catch (error) {
        console.error("An error occurred during migration:", error);
    } finally {
        // Close both database connections
        await clientFirstDB.close();
        await clientSecondDB.close();
    }
}

// Run the migration
runMigration().catch(console.dir);