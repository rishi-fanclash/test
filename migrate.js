const { MongoClient } = require('mongodb');

async function copyDatabase(sourceUri, sourceDbName, targetUri, targetDbName, tableNames, clientId) {
    const sourceClient = new MongoClient(sourceUri, { useNewUrlParser: true, useUnifiedTopology: true });
    const targetClient = new MongoClient(targetUri, { useNewUrlParser: true, useUnifiedTopology: true });

    try {
        // Connect to both clusters
        await sourceClient.connect();
        await targetClient.connect();

        console.log(`Connected to both source and target clusters`);

        const sourceDb = sourceClient.db(sourceDbName);
        const targetDb = targetClient.db(targetDbName);

        // Get all collections from the source database
        const collections = await sourceDb.listCollections().toArray();

        // Loop over each collection and copy it to the target database
        for (const collectionInfo of collections) {
            const collectionName = collectionInfo.name;
            if(!tableNames.includes(collectionName)) {
                continue;
            }
            if (collectionName.startsWith('system.') || collectionName === 'csgoskyevent') {
                console.log(`Skipping system collection: ${collectionName}`);
                continue;
            }
            if (collectionInfo.type === 'view') {
                console.log(`Skipping view: ${collectionName}`);
                continue;
            }
            const sourceCollection = sourceDb.collection(collectionName);
            const targetCollection = targetDb.collection(collectionName);

            console.log(`Copying collection: ${collectionName}`);

            // Get all documents from the source collection
            const sampleDocument = await sourceCollection.findOne();
            const hasClientId = sampleDocument && 'clientId' in sampleDocument;
            const clientIdFilter = hasClientId ? { clientId: clientId } : {};
            const documents = await sourceCollection.find().toArray();

            // Insert documents into the target collection
            if (documents.length > 0) {
                await targetCollection.insertMany(documents);
            }

            console.log(`Copied ${documents.length} documents to ${targetDbName}.${collectionName}`);

            // Get indexes from the source collection
            const indexes = await sourceCollection.listIndexes().toArray();

            // Create indexes in the target collection
            for (const index of indexes) {
                // Filter out the _id index since it is automatically created
                if (index.name !== '_id_') {
                    delete index.v;
                    delete index.ns;
                    const key = index.key;
                    delete index.key;
                    const options = index;

                    await targetCollection.createIndex(key, options);
                    console.log(`Copied index ${index.name} to ${targetDbName}.${collectionName}`);
                }
            }
        }

        console.log(`Database copy completed successfully.`);
    } catch (error) {
        console.error(`Error occurred during database copy: ${error}`);
    } finally {
        // Close the connections
        await sourceClient.close();
        await targetClient.close();
}
}

// Configuration: Replace these values with your actual URIs and database names
const sourceUri = 'mongodb://appUsr:myappuser@cluster0-shard-00-00-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-01-5xg5u.gcp.mongodb.net:27017,cluster0-shard-00-02-5xg5u.gcp.mongodb.net:27017/fanclash-staging?authSource=admin&replicaSet=Cluster0-shard-0&readPreference=primary&ssl=true';
const sourceDbName = 'fc_glacier';
const targetUri = 'mongodb://localhost:27017';
//const targetUri = 'mongodb://root:example@localhost:27017/'
const targetDbName = 'fc_glacier';
const tableNames = [
    // 'access_tokens',
    // 'adminuser',
    // 'apikey',
    // 'campaign',
    // 'cc_configs',
    // 'contestcategory',
    // 'contesttype',
    // 'dynamicconfig',
    // 'fantasyscoring',
    // 'countries',
    // 'cv',
    // 'togame',
    // 'tomatchmode',
    // 'entities',
    'games',
    // 'passport',
    // 'permissions',
    // 'regions',
    // 'roles',
    // 'users'
];
const clientId = 'client_fanclash';

copyDatabase(sourceUri, sourceDbName, targetUri, targetDbName, tableNames, clientId);