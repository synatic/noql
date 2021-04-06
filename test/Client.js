const { MongoClient } = require('mongodb');
const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');

const _customers = require('./customers.json');
const _stores = require('./stores.json');
const _films = require('./films.json');
const _connectionString = "mongodb://127.0.0.1:27017";
const _dbName = "sql-to-mongo-test";
const _queryTests = require('./MongoQueryTests.json');
const _aggregateTests = require('./MongoAggregateTests.json');

const arithmeticExpressionOperators = require('./expressionOperators/ArithmeticExpressionOperators')

describe('Client Queries', function () {
    this.timeout(90000)
    let client;
    before(function (done) {
        const run = async () => {
            try {
                client = new MongoClient(_connectionString);
                await client.connect();
                const { databases } = await client.db().admin().listDatabases();
                if (databases.findIndex(d => d.name === _dbName) > -1) {
                    await client.db(_dbName).dropDatabase();
                }
                const db = client.db(_dbName);

                await db.collection('customers').bulkWrite(_customers.map(d => {
                    return { insertOne: { document: d } };
                }));

                await db.collection('stores').bulkWrite(_stores.map(d => {
                    return { insertOne: { document: d } };
                }));

                await db.collection('films').bulkWrite(_films.map(d => {
                    return { insertOne: { document: d } };
                }));


                done();
            } catch (exp) {
                done(exp);
            }

        }
        run();
    });

    after(function (done) {
        client.close(() => {
            done();
        });
    });

    describe('SQL Query Parser', function () {
        it('run query tests', function (done) {
            (async () => {
                const tests = _queryTests.filter(q => !!q.query && !q.error);
                for (const test of tests) {
                    try {
                        const parsedQuery = SQLParser.parseSQL(test.query);
                        if (parsedQuery.count) {
                            const count = await client.db(_dbName).collection(parsedQuery.collection).countDocuments(parsedQuery.query || null);
                            console.log(`\u2714 ${test.query} ${count}`);
                        } else {
                            let find = client.db(_dbName).collection(parsedQuery.collection).find(parsedQuery.query || null, { projection: parsedQuery.projection });
                            if (parsedQuery.sort) {
                                find.sort(parsedQuery.sort)
                            }
                            if (parsedQuery.limit) {
                                find.limit(parsedQuery.limit)
                            }
                            const results = await find.toArray();
                            console.log(`\u2714 ${test.query} | count:${results.length} | ${results[0] ? JSON.stringify(results[0]) : ""}`);
                        }
                    } catch (exp) {
                        console.error(`\u2716 ${test.query} ${exp.message || ""}`);
                    }

                }
                done();
            })()


        });

        it('run aggregate tests', function (done) {
            (async () => {
                const tests = _aggregateTests.filter(q => !!q.query && !q.error);
                for (const test of tests) {
                    try {
                        const parsedQuery = SQLParser.parseSQL(test.query, test.type);
                        let results = await client.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                        results = await results.toArray()

                        console.log(`\u2714 ${test.query} | count:${results.length} | ${results[0] ? JSON.stringify(results[0]) : ""}`);

                    } catch (exp) {
                        console.error(`\u2716 ${test.query} ${exp.message || ""}`);
                    }

                }
                done();
            })()


        });

    });

    describe('Arithmetic Expression Operators', function () {
        for (const [key, test] of Object.entries(arithmeticExpressionOperators.tests)) {
            it(key, async function () {
                try {
                    const parsedQuery = SQLParser.parseSQL(test.query, test.type);
                    let results = await client.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray()

                    console.log(`\u2714 ${test.query} | count:${results.length} | ${results[0] ? JSON.stringify(results[0]) : ""}`);

                } catch (exp) {
                    console.error(`\u2716 ${test.query} ${exp.message || ""}`);
                }
            });
        }
    });

});
