const { MongoClient, ObjectId } = require('mongodb');

async function updateFcMappingId() {
  // MongoDB connection URLs
    const firstDbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the first DB
    const secondDbUrl = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true'; // Replace with the actual URL of the second DB
  
  // Collection names
  const firstCollectionName = 'players';
  const secondCollectionName = 'players';
  const firstDBName = "fc_cricket";
  const secondDBName = "fc_glacier";

  // Connect to the databases
  const firstClient = new MongoClient(firstDbUrl);
  const secondClient = new MongoClient(secondDbUrl);

  try {
    await firstClient.connect();
    await secondClient.connect();

    const firstDb = firstClient.db(firstDBName);
      const secondDb = secondClient.db(secondDBName);

    const firstCollection = firstDb.collection(firstCollectionName);
    const secondCollection = secondDb.collection(secondCollectionName);

    // Fetch all records from the first collection
    const glacierTableRecords = await secondCollection.find({ teamIds: "6418072daa16b100086df8b6" }).toArray();

      for (const record of glacierTableRecords) {
      const { _id, name } = record;

        const cricketTableRecord = await firstCollection.findOne({ fc_mapping_id: _id.toString() });
        if (cricketTableRecord) continue; // Skip if the mapping ID is valid

      // Search for a matching record in the second table by name using regex
      const regex = new RegExp(`^${name.split(' ')[0]}$`, 'i'); // Case-insensitive match
      const matchedRecordOnCricket = await firstCollection.findOne({ name: name });

          if (matchedRecordOnCricket) {
        // Update the `fc_mapping_id` in the first table with the `_id` of the matched record
        await firstCollection.updateOne(
            { _id: matchedRecordOnCricket._id }, // Find the record in the first table by `_id`
          { $set: { fc_mapping_id: _id.toString() } } // Update the `fc_mapping_id`
        );

        console.log(`Updated fc_mapping_id for record ${_id} to ${matchedRecordOnCricket._id}`);
      } else {
        console.log(`No match found for record ${_id} with name "${name}"`);
      }
    }

    console.log('Update operation completed!');
  } catch (err) {
    console.error('Error during update:', err);
  } finally {
    // Close the database connections
    await firstClient.close();
    await secondClient.close();
  }
}

updateFcMappingId();
