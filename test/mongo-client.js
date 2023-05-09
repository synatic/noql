const {MongoClient} = require('mongodb');
const _customers = require('./exampleData/customers.json');
const _stores = require('./exampleData/stores.json');
const _films = require('./exampleData/films.json');
const _customerNotes = require('./exampleData/customer-notes.json');
const _customerNotes2 = require('./exampleData/customer-notes2.json');
const _orders = require('./exampleData/orders.json');
const _inventory = require('./exampleData/inventory.json');
const _policies = require('./exampleData/policies.json');
const _policyPremium = require('./exampleData/policy-premiums.json');
const $check = require('check-types');
const $schema = require('@synatic/schema-magic');

const connectionString = 'mongodb://127.0.0.1:27017';
const dbName = 'sql-to-mongo-test';
/** @type {import('mongodb').MongoClient} */
let client;
/** @type {import('mongodb').Db} */
let db;

async function connect() {
    client = new MongoClient(connectionString);
    await client.connect();
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
            .slice(0, 10)
            .filter((v) => Boolean)
            .map((v) => $schema.generateSchemaFromJSON(v))
    );
    const flattenedSchema = $schema.flattenSchema(schema, {
        additionalProperties: ['displayOptions'],
    });
    console.log(schema, flattenedSchema);
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
    await db.collection('customers').bulkWrite(
        // @ts-ignore
        _customers.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await generateSchema(_customers, 'customers');

    await db.collection('stores').bulkWrite(
        _stores.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await generateSchema(_stores, 'stores');

    await db.collection('films').bulkWrite(
        _films.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await generateSchema(_films, 'films');

    await db.collection('customer-notes').bulkWrite(
        _customerNotes.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await generateSchema(_customerNotes, 'customer-note');

    await db.collection('customer-notes2').bulkWrite(
        _customerNotes2.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await generateSchema(_customerNotes2, 'customer-notes2');

    await db.collection('orders').bulkWrite(
        _orders.map((d) => {
            return {insertOne: {document: parseDocForDates(d)}};
        })
    );
    await generateSchema(_orders, 'orders');

    await db.collection('inventory').bulkWrite(
        _inventory.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await generateSchema(_inventory, 'inventory');

    await db.collection('ams360-powerbi-basicpolinfo').bulkWrite(
        _policies.map((d) => {
            return {insertOne: {document: d}};
        })
    );

    await db.collection('ams360-powerbi-policytranpremium').bulkWrite(
        _policyPremium.map((d) => {
            return {insertOne: {document: d}};
        })
    );
}
function parseDocForDates(d, parentKey = '', parentObject = {}) {
    // eslint-disable-next-line guard-for-in
    for (const key in d) {
        const value = d[key];
        if (key === '$date') {
            if ($check.null(value)) {
                parentObject[parentKey] = new Date();
            } else {
                parentObject[parentKey] = new Date(value);
            }
            continue;
        }
        if ($check.object(value)) {
            parseDocForDates(value, key, d);
        }
    }
    return d;
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
    return {db, client};
}

async function disconnect() {
    return client.close();
}

module.exports = {connect, addTestData, dropTestDb, setup, disconnect};
