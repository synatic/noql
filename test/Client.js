const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');
const ObjectID = require('bson-objectid');

const _dbName = 'sql-to-mongo-test';
const {setup, disconnect} = require('./mongo-client');
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
const supportsArraySort = false;
const _aggregateTests = [].concat(require('./aggregateTests/aggregateTests.json'), require('./aggregateTests/joins.json'));

describe('Client Queries', function () {
    this.timeout(90000);
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    before(function (done) {
        const run = async () => {
            try {
                const {client, db} = await setup();
                mongoClient = client;

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
        disconnect().then(done).catch(done);
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
                                const count = await mongoClient
                                    .db(_dbName)
                                    .collection(parsedQuery.collection)
                                    .countDocuments(parsedQuery.query || null);
                                console.log(`${count}`);
                            } else {
                                const find = mongoClient
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

                            let results = await mongoClient
                                .db(_dbName)
                                .collection(parsedQuery.collections[0])
                                .aggregate(parsedQuery.pipeline);
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
                            let results = await mongoClient
                                .db(_dbName)
                                .collection(parsedQuery.collections[0])
                                .aggregate(parsedQuery.pipeline);
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

    describe('individual tests', () => {
        it('should be able to sort an array', async (done) => {
            if (!supportsArraySort) {
                return done();
            }
            const queryText = 'SELECT id, (select * from Rentals order by `Rental Date` desc) AS OrderedRentals FROM `customers`';
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            try {
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results);
                done();
            } catch (err) {
                return done(err);
            }
        });
        it('should be able to do a left join', async () => {
            const queryText = 'select * from orders as o left join `inventory` as i  on o.item=i.sku';
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            try {
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });

        it('should be able to do a multipart-binary expression', async () => {
            const queryText = 'select `Replacement Cost`, (log10(3) * floor(`Replacement Cost`) + 1) as S from films limit 1';
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results);
            } catch (err) {
                console.error(err);
                throw err;
            }
        });

        it('should be able to do a select * computed', async () => {
            const queryText = `SELECT *, convert(id, 'string') as idConv FROM inventory`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results);
            } catch (err) {
                console.error(err);
                throw err;
            }
        });

        it('should be able to do n level joins', async () => {
            const queryText = `select
                o.id as orderId
                ,i.id as inventoryId
                ,c.id as customerId
                from orders as o
                inner join \`inventory|unwind\` as i
                on o.item=i.sku
                inner join \`customers|unwind\` as c
                on o.customerId=c.id`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results.length > 0);
                assert(results[0].orderId);
                assert(results[0].inventoryId);
                assert(results[0].customerId);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it("should be able to do n level joins as per the example that didn't work", async () => {
            const queryText = `select
                o.id as orderId
                ,i.id as inventoryId
                ,c.id as customerId
                from orders as o
                inner join \`inventory\` as i
                on o.item=i.sku
                inner join \`customers\` as c
                on o.customerId=c.id
                where o.id=1`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results.length > 0);
                assert(results[0].orderId);
                assert(results[0].inventoryId);
                assert(results[0].customerId);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });

        it('should be able to do a basic root unwind', async () => {
            const queryText = `select unwind('Address') as \`$$ROOT\` from customers LIMIT 1`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results.length > 0);
                assert(results[0].Address);
                assert(results[0].City);
                assert(results[0].Country);
                assert(results[0].District);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do a basic root unwind', async () => {
            const queryText = `select 'Address' as \`$$ROOT\` from customers LIMIT 1`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results.length > 0);
                assert(results[0].Address);
                assert(results[0].City);
                assert(results[0].Country);
                assert(results[0].District);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should result in a query type when the where is not needed to be a pipeline', async () => {
            const queryText = "select * from `customers` where `Address.City` in ('Japan','Pakistan') limit 10";
            const parsedQuery = SQLParser.parseSQL(queryText);
            assert(parsedQuery.type === 'query');
        });
        it('should result in an aggregate type when the where is not a simple query', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id=1)`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            assert(parsedQuery.type === 'aggregate');
        });
        describe('sub query', () => {
            // join
            // group by
            // const queryText = `select * from orders where item in (select sku from inventory where id=1) and sky='almonds'`;
            it('should be able to support subquery syntax', async () => {
                const queryText = `select * from orders where item in (select sku from inventory where id=1)`;
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                try {
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 2);
                    assert(results[0].id === 1);
                    assert(results[0].item === 'almonds');
                    assert(results[0].price === 12);
                    assert(results[0].quantity === 2);
                    assert(results[0].customerId === 1);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to do n level sub queries', async () => {
                const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) order by id`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 3);
                    assert(results[0].id === 1);
                    assert(results[0].item === 'almonds');
                    assert(results[0].price === 12);
                    assert(results[0].quantity === 2);
                    assert(results[0].customerId === 1);
                    assert(results[1].id === 2);
                    assert(results[1].item === 'pecans');
                    assert(results[1].price === 20);
                    assert(results[1].quantity === 1);
                    assert(results[1].customerId === 2);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to do n level sub queries that are selects', async () => {
                const queryText = `select * from orders where item in (select sku from inventory where id in (select id from orders))`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 3);
                    assert(results[0].id === 1);
                    assert(results[0].item === 'almonds');
                    assert(results[0].price === 12);
                    assert(results[0].quantity === 2);
                    assert(results[0].customerId === 1);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to support table aliases', async () => {
                const queryText = `select c.item as Product from orders c where item in (select sku from inventory where id in (select id from orders))`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 3);
                    assert(results[0].Product === 'almonds');
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to support it with a sort', async () => {
                const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) ORDER BY id DESC`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 3);
                    assert(results[0].item === 'almonds');
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to support it with a limit', async () => {
                const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) ORDER BY id DESC limit 1`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 1);
                    assert(results[0].item === 'almonds');
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to support multiple where clauses', async () => {
                const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) and price=12`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 2);
                    assert(results[0].item === 'almonds');
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to support multiple where clauses in any order', async () => {
                const queryText = `select * from orders where price=12 and item in (select sku from inventory where id in (1,4))`;
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 2);
                    assert(results[0].item === 'almonds');
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
        describe('count', () => {
            it('should be able to do a count * with a group by', async () => {
                const queryText = 'select item, count(1) as countVal from orders group by `item`';
                const parsedQuery = SQLParser.parseSQL(queryText);
                try {
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 2);
                    assert(results[0].item === 'pecans');
                    assert(results[0].countVal === 1);
                    assert(results[1].item === 'almonds');
                    assert(results[1].countVal === 2);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to do a count * without a group by', async () => {
                const queryText = `select count(1) as countVal from orders`;
                const parsedQuery = SQLParser.parseSQL(queryText);
                try {
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 1);
                    assert(results[0].countVal === 3);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to do a count * without a group by with a where item=almonds', async () => {
                const queryText = `select count(1) as countVal from orders where item=almonds`;
                const parsedQuery = SQLParser.parseSQL(queryText);
                try {
                    let results = await mongoClient.db(_dbName).collection(parsedQuery.collections[0]).aggregate(parsedQuery.pipeline);
                    results = await results.toArray();
                    assert(results.length === 1);
                    assert(results[0].countVal === 2);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
        describe('unset', () => {
            it('should be able to do a basic unset in a query', async () => {
                const queryText = 'SELECT unset(_id),id,item FROM `orders`';
                try {
                    const parsedQuery = SQLParser.parseSQL(queryText);
                    const results = await mongoClient
                        .db(_dbName)
                        .collection(parsedQuery.collection)
                        .find(parsedQuery.query || null, {projection: parsedQuery.projection})
                        .toArray();

                    assert(results.length === 3);
                    assert(results[0]._id === undefined);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should be able to do a basic unset in an aggregation', async () => {
                const queryText = 'SELECT unset(_id),id,item FROM orders where item in (select sku from inventory where id=1)';
                try {
                    const parsedQuery = SQLParser.parseSQL(queryText);
                    const results = await mongoClient
                        .db(_dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();

                    assert(results.length === 2);
                    assert(results[0]._id === undefined);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
        describe('Union', () => {
            describe('Union all', () => {
                it('should be able to do a basic union all', async () => {
                    const queryText =
                        'SELECT unset(_id), o.id, o.item as product FROM `orders` o UNION ALL select unset(_id),i.id, i.sku as product from inventory i';
                    try {
                        const parsedQuery = SQLParser.parseSQL(queryText);
                        const results = await mongoClient
                            .db(_dbName)
                            .collection(parsedQuery.collections[0])
                            .aggregate(parsedQuery.pipeline)
                            .toArray();
                        assert(results.length === 8);
                        return;
                    } catch (err) {
                        console.error(err);
                        throw err;
                    }
                });
            });
            describe('Union', () => {
                it('should be able to do a basic union all', async () => {
                    const queryText =
                        'SELECT unset(_id), o.id, o.item as product FROM `orders` o UNION select unset(_id),i.id, i.sku as product from inventory i';
                    try {
                        const parsedQuery = SQLParser.parseSQL(queryText);
                        const results = await mongoClient
                            .db(_dbName)
                            .collection(parsedQuery.collections[0])
                            .aggregate(parsedQuery.pipeline)
                            .toArray();
                        assert(results.length === 7);
                        return;
                    } catch (err) {
                        console.error(err);
                        throw err;
                    }
                });
            });
        });
    });
});
