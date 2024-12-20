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

    // Collections
    const matchcommon = secondDB.collection("matchcommon");
    const matches = firstDB.collection("matches");

      const matchCommonRecords = await matchcommon.find({ tournament: new ObjectId('67288c1a99a69a04e0f6f18c')}).toArray();

      for (const matchCommonRecord of matchCommonRecords) {
        if(!matchCommonRecord.glacierMatchIds || matchCommonRecord.glacierMatchIds.length === 0 && matchCommonRecord.glacierClassId) {
            const seriesId = matchCommonRecord?.glacierClassId;
            const matchRecord = await matches.findOne({seriesId});
            if(matchRecord) {
                const updatedMatchCommonRecord = await matchcommon.updateOne({ _id: new ObjectId(matchCommonRecord._id) }, { $set: { glacierMatchIds: [matchRecord._id.toString()] } });
                if(updatedMatchCommonRecord.matchedCount !== 1) {
                    console.warn("Record not updated for matchcommon id: "+ matchCommonRecord._id.toString());
                } else {
                    console.log("Record updated for matchcommon id: "+ matchCommonRecord._id.toString());
                }
            } else {
                console.warn("No match record found for seriesId: "+ matchCommonRecord.glacierClassId);
            }
        }
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