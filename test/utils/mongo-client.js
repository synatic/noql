const {EJSON} = require('bson');
const {MongoClient, ObjectId} = require('mongodb');
const $schema = require('@synatic/schema-magic');
const fs = require('fs/promises');
const Path = require('path');

const connectionString = 'mongodb://127.0.0.1:27017';
const dbName = 'sql-to-mongo-test';
/** @type {import('mongodb').MongoClient} */
let client;
/** @type {import('mongodb').Db} */
let db;

async function connect() {
    console.log('About to connect to db');
    client = new MongoClient(connectionString);
    await client.connect();
    console.log('Connected!');
    db = client.db(dbName);
}

/**
 *
 * @param {object[]} values
 * @param {string} collectionName
 */
async function generateSchema(values, collectionName) {
    if (!client || !db) {
        throw new Error('Call connect before addTestData');
    }
    const schema = $schema.mergeSchemas(
        values
            .slice(0, 100)
            .filter((v) => Boolean)
            .map((v) => $schema.generateSchemaFromJSON(v))
    );
    const flattenedSchema = $schema.flattenSchema(schema, {
        additionalProperties: ['displayOptions'],
    });
    await db.collection('schemas').insertOne({
        collectionName,
        schema,
        flattenedSchema,
    });
}
async function addTestData() {
    if (!client || !db) {
        throw new Error('Call connect before addTestData');
    }
    const dataDirectory = './test/exampleData/';
    const files = await fs.readdir(dataDirectory);
    for (const file of files) {
        const searchString = '.json';
        const jsonIndex = file.lastIndexOf(searchString);
        if (jsonIndex < 0) {
            continue;
        }
        const collectionName = file.substring(0, jsonIndex);
        const filePath = Path.join(dataDirectory, file);
        const dataString = await fs.readFile(filePath, {encoding: 'utf-8'});
        const data = EJSON.parse(dataString);
        for (const item of data) {
            if (item._id && typeof item._id === 'string') {
                item._id = new ObjectId(item._id);
            }
        }
        await db.collection(collectionName).bulkWrite(
            data.map((d) => {
                return {insertOne: {document: d}};
            })
        );
        await generateSchema(data, collectionName);
    }
}

async function dropTestDb() {
    if (!client || !db) {
        throw new Error('Call connect before dropTestDb');
    }
    const {databases} = await client.db().admin().listDatabases();
    if (databases.findIndex((d) => d.name === dbName) > -1) {
        await client.db(dbName).dropDatabase();
    }
}

async function setup() {
    await connect();
    await dropTestDb();
    await addTestData();
    return {db, client, dbName};
}

async function disconnect() {
    return client.close();
}

module.exports = {connect, addTestData, dropTestDb, setup, disconnect, dbName};
