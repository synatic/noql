const {MongoClient} = require('mongodb');
// eslint-disable-next-line no-unused-vars
const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');
const ObjectID = require('bson-objectid');

const _customers = require('./exampleData/customers.json');
const _stores = require('./exampleData/stores.json');
const _films = require('./exampleData/films.json');
const _customerNotes = require('./exampleData/customer-notes.json');
const _customerNotes2 = require('./exampleData/customer-notes2.json');
const _orders = require('./exampleData/orders.json');
const _inventory = require('./exampleData/inventory.json');
const _connectionString = 'mongodb://127.0.0.1:27017';
const _dbName = 'sql-to-mongo-test';

const _queryTests = [].concat(
    require('./queryTests/queryTests.json'),
    require('./queryTests/objectOperators.json'),
    require('./queryTests/arrayOperators.json'),
    require('./queryTests/stringOperators.json'),
    require('./queryTests/dateOperators.json'),
    require('./queryTests/arithmeticOperators.json'),
    require('./queryTests/conversionOperators.js'),
    require('./queryTests/comparisonOperators.json'),
    require('./queryTests/columnOperators.json')
);

const _aggregateTests = [].concat(require('./aggregateTests/aggregateTests.json'), require('./aggregateTests/joins.json'));

describe('Client Queries', function () {
    this.timeout(90000);
    let client;
    before(function (done) {
        const run = async () => {
            try {
                client = new MongoClient(_connectionString);
                await client.connect();
                const {databases} = await client.db().admin().listDatabases();
                if (databases.findIndex((d) => d.name === _dbName) > -1) {
                    await client.db(_dbName).dropDatabase();
                }
                const db = client.db(_dbName);

                await db.collection('customers').bulkWrite(
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

                const details = await db.collection('inventory').findOne({id: 1});
                const details2 = await db.collection('inventory').findOne({_id: new ObjectID(details._id.toString())});
                if (!details2) {
                    throw new Error('Invalid BSOJN Parse');
                }
                done();
            } catch (exp) {
                done(exp);
            }
        };
        run();
    });

    after(function (done) {
        client.close(() => {
            done();
        });
    });

    describe('run query tests', function (done) {
        (async () => {
            const tests = _queryTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${test.query}`, function (done) {
                    (async () => {
                        try {
                            const parsedQuery = SQLParser.parseSQL(test.query);
                            if (parsedQuery.count) {
                                const count = await client
                                    .db(_dbName)
                                    .collection(parsedQuery.collection)
                                    .countDocuments(parsedQuery.query || null);
                                console.log(`${count}`);
                            } else {
                                const find = client
                                    .db(_dbName)
                                    .collection(parsedQuery.collection)
                                    .find(parsedQuery.query || null, {projection: parsedQuery.projection});
                                if (parsedQuery.sort) {
                                    find.sort(parsedQuery.sort);
                                }
                                if (parsedQuery.limit) {
                                    find.limit(parsedQuery.limit);
                                }
                                const results = await find.toArray();
                                console.log(`count:${results.length} | ${results[0] ? JSON.stringify(results[0]) : ''}`);
                            }
                            done();
                        } catch (exp) {
                            done(exp ? exp.message : null);
                        }
                    })();
                });
            }
            done();
        })();
    });

    describe('run query tests as aggregates', function (done) {
        (async () => {
            const tests = _queryTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${test.query}`, function (done) {
                    (async () => {
                        try {
                            const parsedQuery = SQLParser.makeMongoAggregate(test.query);

                            let results = await client.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                            results = await results.toArray();

                            console.log(`count:${results.length} | ${results[0] ? JSON.stringify(results[0]) : ''}`);
                            done();
                        } catch (exp) {
                            done(exp ? exp.message : null);
                        }
                    })();
                });
            }
            done();
        })();
    });

    describe('run aggregate tests', function (done) {
        (async () => {
            const tests = _aggregateTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${test.query}`, function (done) {
                    (async () => {
                        try {
                            const parsedQuery = SQLParser.makeMongoAggregate(test.query);
                            let results = await client.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                            results = await results.toArray();

                            console.log(`count:${results.length} | ${results[0] ? JSON.stringify(results[0]) : ''}`);
                            done();
                        } catch (exp) {
                            done(exp ? exp.message : null);
                        }
                    })();
                });
            }
            done();
        })();
    });
});
