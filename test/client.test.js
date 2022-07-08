const SQLParser = require('../lib/SQLParser.js');
const ObjectID = require('bson-objectid');
const {setup, disconnect} = require('./mongo-client');

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
const _aggregateTests = [].concat(
    require('./aggregateTests/aggregateTests.json'),
    require('./aggregateTests/joins.json')
);

describe('Client Queries', function () {
    this.timeout(90000);
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    before(function (done) {
        const run = async () => {
            try {
                const {client, db} = await setup();
                mongoClient = client;

                const details = await db
                    .collection('inventory')
                    .findOne({id: 1});
                const details2 = await db
                    .collection('inventory')
                    .findOne({_id: new ObjectID(details._id.toString())});
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
        disconnect().then(done).catch(done);
    });

    describe('run query tests', function (done) {
        (async () => {
            const tests = _queryTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${
                    test.query
                }`, function (done) {
                    (async () => {
                        try {
                            const parsedQuery = SQLParser.parseSQL(test.query);
                            if (parsedQuery.count) {
                                const count = await mongoClient
                                    .db(_dbName)
                                    .collection(parsedQuery.collection)
                                    .countDocuments(parsedQuery.query || null);
                                console.log(`${count}`);
                            } else {
                                const find = mongoClient
                                    .db(_dbName)
                                    .collection(parsedQuery.collection)
                                    .find(parsedQuery.query || null, {
                                        projection: parsedQuery.projection,
                                    });
                                if (parsedQuery.sort) {
                                    find.sort(parsedQuery.sort);
                                }
                                if (parsedQuery.limit) {
                                    find.limit(parsedQuery.limit);
                                }
                                const results = await find.toArray();
                                console.log(
                                    `count:${results.length} | ${
                                        results[0]
                                            ? JSON.stringify(results[0])
                                            : ''
                                    }`
                                );
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
                it(`${test.name ? test.name + ':' : ''}${
                    test.query
                }`, function (done) {
                    (async () => {
                        try {
                            const parsedQuery = SQLParser.makeMongoAggregate(
                                test.query
                            );

                            let results = await mongoClient
                                .db(_dbName)
                                .collection(parsedQuery.collections[0])
                                .aggregate(parsedQuery.pipeline);
                            results = await results.toArray();

                            console.log(
                                `count:${results.length} | ${
                                    results[0] ? JSON.stringify(results[0]) : ''
                                }`
                            );
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
                it(`${test.name ? test.name + ':' : ''}${
                    test.query
                }`, function (done) {
                    (async () => {
                        try {
                            const parsedQuery = SQLParser.makeMongoAggregate(
                                test.query
                            );
                            let results = await mongoClient
                                .db(_dbName)
                                .collection(parsedQuery.collections[0])
                                .aggregate(parsedQuery.pipeline);
                            results = await results.toArray();

                            console.log(
                                `count:${results.length} | ${
                                    results[0] ? JSON.stringify(results[0]) : ''
                                }`
                            );
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
