import { MongoClient, ObjectId } from 'mongodb';

async function updateFcMappingId() {
  // MongoDB connection URLs
    const firstDbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the first DB
    const secondDbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the second DB
  
  // Collection names
  const firstCollectionName = 'players';
  const secondCollectionName = 'line_ups';
  const firstDBName = "fc_cricket";
  const secondDBName = "fc_glacier";

  // Connect to the databases
  const firstClient = new MongoClient(firstDbUrl);
  const secondClient = new MongoClient(secondDbUrl);
  const arr = [];

  try {
    await firstClient.connect();
    await secondClient.connect();

    const firstDb = firstClient.db(firstDBName);
      const secondDb = secondClient.db(secondDBName);

    const firstCollection = firstDb.collection(firstCollectionName);
    const secondCollection = secondDb.collection(secondCollectionName);

    // Fetch all records from the first collection
      const glacierTableRecords = (await secondCollection.findOne({ _id: new ObjectId("67568c010b42eeb716e21245") })).playerIds;
      const playerIds = [
          "6756863effa4021869982ca4",
          "6756863e8d0c467d86e4e6dc",
          "64216a0be71c9b0007b45454",
          "641d5a6f9c1107000839042b",
          "6756863fffb9a00734033678",
          "641d5a6f9c11070008390432",
          "64216a0bbe78a40008200780",
          "67568646ffa4021869982caa",
          "6756863ee358749610515c75",
          "6756863fe358749610515c76",
          "675686458d0c467d86e4e6e1",
          "6756863fffa4021869982ca5",
          "675686408d0c467d86e4e6dd",
          "64216a14be78a400082007cb",
          "67568646e358749610515c7b",
          "64216a12be78a400082007bd",
          "64216a09be78a40008200770",
          "67568645ffb9a0073403367d",
          "64216a07be78a40008200762",
      ]

      for (const record of playerIds) {
      const id = record;

        const cricketTableRecord = await firstCollection.findOne({ fc_mapping_id: id});
        const glacierTableRecord = await secondDb.collection('players').findOne({ _id: new ObjectId(id) });
        if (glacierTableRecord || !cricketTableRecord) continue; // Skip if the mapping ID is valid
        const name = cricketTableRecord.name;
        const matchedRecordOnGlacier = await secondDb.collection('players').findOne({ name: name });

        if (matchedRecordOnGlacier) {
        // Update the `fc_mapping_id` in the first table with the `_id` of the matched record
        await firstCollection.updateOne(
          { _id: cricketTableRecord._id }, // Find the record in the first table by `_id`
          { $set: { fc_mapping_id: matchedRecordOnGlacier._id.toString() } } // Update the `fc_mapping_id`
        );

        //   console.log(`Updated fc_mapping_id for record ${cricketTableRecord._id} to ${matchedRecordOnGlacier._id.toString() }`);
            arr.push(matchedRecordOnGlacier._id.toString());
      } else {
          console.log(`No match found for record ${cricketTableRecord._id} with name "${name}"`);
      }
    }

    console.log('Update operation completed!');
    console.log(arr);
  } catch (err) {
    console.error('Error during update:', err);
  } finally {
    // Close the database connections
    await firstClient.close();
    await secondClient.close();
  }
}

updateFcMappingId();
