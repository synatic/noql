const SQLParser = require('../lib/SQLParser.js');
const {setup, disconnect, dbName} = require('./utils/mongo-client.js');

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
                    .findOne({_id: details._id});
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

    describe('run query tests', function () {
        (async () => {
            const tests = _queryTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${
                    test.query
                }`, async function () {
                    const parsedQuery = SQLParser.parseSQL(test.query, {
                        database: test.database || 'PostgresQL',
                    });
                    if (parsedQuery.count) {
                        const count = await mongoClient
                            .db(dbName)
                            .collection(parsedQuery.collection)
                            .countDocuments(parsedQuery.query || {});
                        console.log(`${count}`);
                    } else {
                        const find = mongoClient
                            .db(dbName)
                            .collection(parsedQuery.collection)
                            .find(parsedQuery.query || {}, {
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
                                results[0] ? JSON.stringify(results[0]) : ''
                            }`
                        );
                    }
                });
            }
        })();
    });

    describe('run query tests as aggregates', function () {
        (async () => {
            const tests = _queryTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${
                    test.query
                }`, async function () {
                    const parsedQuery = SQLParser.makeMongoAggregate(
                        test.query,
                        {
                            database: test.database || 'PostgresQL',
                        }
                    );

                    let results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline);
                    results = await results.toArray();

                    console.log(
                        `count:${results.length} | ${
                            results[0] ? JSON.stringify(results[0]) : ''
                        }`
                    );
                });
            }
        })();
    });

    describe('run aggregate tests', function () {
        (async () => {
            const tests = _aggregateTests.filter((q) => !!q.query && !q.error);
            for (const test of tests) {
                it(`${test.name ? test.name + ':' : ''}${
                    test.query
                }`, async function () {
                    const parsedQuery = SQLParser.makeMongoAggregate(
                        test.query,
                        {
                            database: test.database || 'PostgresQL',
                        }
                    );
                    let results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline);
                    results = await results.toArray();

                    console.log(
                        `count:${results.length} | ${
                            results[0] ? JSON.stringify(results[0]) : ''
                        }`
                    );
                });
            }
        })();
    });
});
