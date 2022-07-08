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

    await db.collection('stores').bulkWrite(
        _stores.map((d) => {
            return {insertOne: {document: d}};
        })
    );

    await db.collection('films').bulkWrite(
        _films.map((d) => {
            return {insertOne: {document: d}};
        })
    );
    await db.collection('customer-notes').bulkWrite(
        _customerNotes.map((d) => {
            return {insertOne: {document: d}};
        })
    );

    await db.collection('customer-notes2').bulkWrite(
        _customerNotes2.map((d) => {
            return {insertOne: {document: d}};
        })
    );

    await db.collection('orders').bulkWrite(
        _orders.map((d) => {
            return {insertOne: {document: d}};
        })
    );

    await db.collection('inventory').bulkWrite(
        _inventory.map((d) => {
            return {insertOne: {document: d}};
        })
    );

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
