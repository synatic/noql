const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');
const {canQuery} = require('../lib/canQuery');
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

describe('SQL Parser', function () {
    describe('should parse from sql ast: SQLParser.parseSQLtoAST', function () {
        it('should parse simple sql', function () {
            const {ast} = SQLParser.parseSQLtoAST(
                'select * from `collection`',
                {database: 'PostgresQL'}
            );
            assert(ast.from[0].table === 'collection', 'Invalid from');
        });

        it('should return an ast when ast passed', function () {
            const ast = SQLParser.parseSQLtoAST('select * from `collection`', {
                database: 'PostgresQL',
            });
            const ast2 = SQLParser.parseSQLtoAST(ast, {database: 'PostgresQL'});
            assert(ast === ast2, 'Invalid returns from ast');
        });

        it('should fail if no collection passed', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select 1', {
                    database: 'PostgresQL',
                });
            } catch (exp) {
                return assert.equal(
                    exp.message,
                    'SQL statement requires at least 1 collection',
                    'Invalid error message'
                );
            }
            assert(false, 'No error');
        });

        it('should fail on an invalid statement', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select *  `collection`', {
                    database: 'PostgresQL',
                });
            } catch (exp) {
                return assert.ok(
                    exp.message.startsWith(
                        '[Start: Line 1, Col:11][End: Line 1, Col:12] - Expected "#", ",", "--", "/*", ";", "FOR", "FROM", "GO", "GROUP", "HAVING"'
                    )
                );
            }
            assert(false, 'No error');
        });

        it('should fail on an invalid statement 2', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST(
                    'select * from `collection` with unwind',
                    {database: 'PostgresQL'}
                );
            } catch (exp) {
                return assert.equal(
                    exp.message,
                    '[Start: Line 1, Col:32][End: Line 1, Col:33] - Expected [A-Za-z0-9_$] but " " found.'
                );
            }
            assert(false, 'No error');
        });

        it('should fail on no as with function', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST(
                    'select Name,sum(`Replacement Cost`,2)  from `films`',
                    {database: 'PostgresQL'}
                );
            } catch (exp) {
                return assert.equal(
                    exp.message,
                    'Requires as for function:sum'
                );
            }
            assert(false, 'No error');
        });

        it('should fail on no as with binary expr', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST(
                    'select Name,a>b from `films`',
                    {database: 'PostgresQL'}
                );
            } catch (exp) {
                return assert.equal(exp.message, 'Requires as for binary_expr');
            }
            assert(false, 'No error');
        });
    });

    describe('should test can query: canQuery', function () {
        it('should test a simple sql', function () {
            assert(canQuery('select * from `collection`'), 'Invalid can query');
        });

        it('should test a simple sql with where', function () {
            assert(
                canQuery('select * from `collection` where a=1'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with names', function () {
            assert(
                canQuery('select Title,Description from `collection`'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with unwind', function () {
            assert(
                !canQuery('select unwind(a) as b from `collection` where a=1'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with a group by', function () {
            assert(
                !canQuery(
                    'select b,sum(a) as c from `collection` where a=1 group by b'
                ),
                'Invalid can query'
            );
        });

        it('should test a simple sql with a join', function () {
            assert(
                !canQuery(
                    'select b,sum(a) as c from `collection` where a=1 group by b'
                ),
                'Invalid can query'
            );
        });

        it('should test a simple sql with single abs', function () {
            assert(
                canQuery('select abs(`Replacement Cost`) as s from `films`'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with single sum', function () {
            assert(
                canQuery('select sum(`Replacement Cost`,2) as s from `films`'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with single for aggregate', function () {
            assert(
                !canQuery(
                    'select sum(`Replacement Cost`) as s from `films` group by s'
                ),
                'Invalid can query'
            );
        });

        it('should test a simple sql with single sum', function () {
            assert(
                canQuery('select avg(`Replacement Cost`,2) as s from `films`'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with single for avg', function () {
            assert(
                !canQuery(
                    'select id,avg(`Replacement Cost`) as s from `films` group by id'
                ),
                'Invalid can query'
            );
        });

        it('should test a simple sql with an expr sum', function () {
            assert(
                canQuery('select `Replacement Cost` + 2 as s from `films`'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with an expr sum', function () {
            assert(
                canQuery('select `Replacement Cost` + 2 as s from `films`'),
                'Invalid can query'
            );
        });

        it('should test a simple sql with a convert', function () {
            assert(
                canQuery(
                    'select convert(`Replacement Cost`) as s from `films`'
                ),
                'Invalid can query'
            );
        });

        it('should not allow sub from', function () {
            assert(
                !canQuery('select * from (select * from `films`) f'),
                'Invalid can query'
            );
        });

        it('should nt allow *,function ', function () {
            assert(
                !canQuery(
                    "select *,convert(`Replacement Cost`,'int') as s from `films`"
                ),
                'Invalid can query'
            );
        });

        it('should test as ', function () {
            assert(!canQuery('select * from `films` f'), 'Invalid can query');
        });

        it('should not allow with where on sub query ', function () {
            assert(
                !canQuery(
                    "select id,Title,Rating,sum_Array((select salesId as total from Rentals),'total') as resultTotal from `customers` where resultTotal > 1 and id<10"
                ),
                'Invalid can query'
            );
        });
    });

    describe('should run query tests', function () {
        for (const t of _queryTests) {
            it(`${t.name ? t.name + ':' : ''}${t.query}`, function () {
                if (t.error) {
                    try {
                        SQLParser.makeMongoQuery(t.query, {
                            database: t.database || 'PostgresQL',
                        });
                        assert(false, 'No error');
                    } catch (exp) {
                        assert.equal(exp.message, t.error);
                    }
                } else {
                    let err = null;
                    let parsedQuery;
                    try {
                        parsedQuery = SQLParser.makeMongoQuery(t.query, {
                            database: t.database || 'PostgresQL',
                        });
                    } catch (exp) {
                        err = exp.message;
                    }
                    assert(!err, err);
                    t.output.type = t.output.type || 'query';
                    assert.deepEqual(parsedQuery, t.output, 'Invalid parse');
                }
            });
        }
    });

    describe('should run query tests as aggregate tests', function () {
        for (const t of _queryTests.filter((t) => !t.error)) {
            it(`${t.name ? t.name + ':' : ''}${t.query}`, function () {
                let err = null;
                let parsedQuery;
                let parsedAggregate;
                try {
                    parsedQuery = SQLParser.makeMongoQuery(t.query, {
                        database: t.database || 'PostgresQL',
                    });
                    parsedAggregate = SQLParser.makeMongoAggregate(t.query, {
                        database: t.database || 'PostgresQL',
                    });
                } catch (exp) {
                    err = exp.message;
                }
                assert(!err, err);
                const pipeline = [];

                if (parsedQuery.query)
                    pipeline.push({$match: parsedQuery.query});
                if (parsedQuery.projection)
                    pipeline.push({$project: parsedQuery.projection});

                if (parsedQuery.sort) pipeline.push({$sort: parsedQuery.sort});
                if (parsedQuery.limit !== 100)
                    pipeline.push({$limit: parsedQuery.limit});
                if (parsedQuery.skip) pipeline.push({$skip: parsedQuery.skip});

                assert.deepEqual(
                    parsedAggregate,
                    {
                        collections: [parsedQuery.collection],
                        pipeline: pipeline,
                        type: 'aggregate',
                    },
                    'Invalid parse'
                );
            });
        }
    });

    describe('should run aggregate tests', function () {
        for (const t of _aggregateTests) {
            it(`${t.name ? t.name + ':' : ''}${t.query}`, function () {
                if (t.error) {
                    try {
                        SQLParser.makeMongoAggregate(t.query, {
                            database: 'PostgresQL',
                        });
                        assert(false, 'No error');
                    } catch (exp) {
                        assert.equal(exp.message, t.error);
                    }
                } else {
                    let err = null;
                    let parsedQuery;
                    try {
                        parsedQuery = SQLParser.makeMongoAggregate(t.query, {
                            database: 'PostgresQL',
                        });
                    } catch (exp) {
                        err = exp.message;
                    }
                    assert(!err, err);
                    t.output.type = t.output.type || 'aggregate';
                    assert.deepEqual(parsedQuery, t.output, 'Invalid parse');
                }
            });
        }
    });

    it('should parse plain query 2', function () {
        assert.deepEqual(
            SQLParser.parseSQL(
                'select `a.b` as Id ,Name from `global-test` where `a.b`>1',
                {database: 'PostgresQL'}
            ),
            {
                collection: 'global-test',
                limit: 100,
                projection: {
                    Id: '$a.b',
                    Name: '$Name',
                },
                query: {
                    'a.b': {$gt: 1},
                },
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepEqual(
            SQLParser.parseSQL(
                'select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10',
                {database: 'PostgresQL'}
            ),
            {
                collection: 'global-test',
                limit: 10,
                projection: {
                    Id: '$a.b',
                    Name: '$Name',
                },
                query: {
                    'a.b': {$gt: 1},
                },
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepEqual(
            SQLParser.parseSQL(
                'select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5',
                {database: 'PostgresQL'}
            ),
            {
                collection: 'global-test',
                limit: 10,
                skip: 5,
                projection: {
                    Id: '$a.b',
                    Name: '$Name',
                },
                query: {
                    'a.b': {$gt: 1},
                },
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepEqual(
            SQLParser.parseSQL(
                'select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5',
                {database: 'PostgresQL'}
            ),
            {
                collection: 'global-test',
                limit: 10,
                skip: 5,
                projection: {
                    Id: '$a.b',
                    Name: '$Name',
                },
                query: {
                    'a.b': {$gt: 1},
                },
                type: 'query',
            },
            'Invalid parse'
        );
    });

    // SQL to MongoBD Mapping examples from docs.mongobd.com
    it('SQL to MongoBD Mapping', function () {
        // TODO ID issue shoot
        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT id, user_id, status FROM people`, {
                database: 'PostgresQL',
            }),
            {
                limit: 100,
                collection: 'people',
                projection: {id: '$id', user_id: '$user_id', status: '$status'},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT user_id, status FROM people`, {
                database: 'PostgresQL',
            }),
            {
                limit: 100,
                collection: 'people',
                projection: {user_id: '$user_id', status: '$status'},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A"`, {
                database: 'PostgresQL',
            }),
            {
                limit: 100,
                collection: 'people',
                query: {status: {$eq: 'A'}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(
                `SELECT user_id, status FROM people WHERE status = "A"`,
                {database: 'PostgresQL'}
            ),
            {
                limit: 100,
                collection: 'people',
                projection: {user_id: '$user_id', status: '$status'},
                query: {status: {$eq: 'A'}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE status != "A"`, {
                database: 'PostgresQL',
            }),
            {
                limit: 100,
                collection: 'people',
                query: {status: {$ne: 'A'}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(
                `SELECT * FROM people WHERE status = "A" AND age = 50`,
                {database: 'PostgresQL'}
            ),
            {
                limit: 100,
                collection: 'people',
                query: {$and: [{status: {$eq: 'A'}}, {age: {$eq: 50}}]},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(
                `SELECT * FROM people WHERE status = "A" OR age = 50`,
                {database: 'PostgresQL'}
            ),
            {
                limit: 100,
                collection: 'people',
                query: {
                    $or: [{status: {$eq: 'A'}}, {age: {$eq: 50}}],
                },
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE age > 25`, {
                database: 'PostgresQL',
            }),
            {
                limit: 100,
                collection: 'people',
                query: {age: {$gt: 25}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE age < 25`, {
                database: 'PostgresQL',
            }),
            {
                limit: 100,
                collection: 'people',
                query: {age: {$lt: 25}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(
                `SELECT * FROM people WHERE age > 25 and age <30`,
                {database: 'PostgresQL'}
            ),
            {
                limit: 100,
                collection: 'people',
                query: {
                    $and: [
                        {
                            age: {
                                $gt: 25,
                            },
                        },
                        {
                            age: {
                                $lt: 30,
                            },
                        },
                    ],
                },
                type: 'query',
            },
            'Invalid parse'
        );
    });

    it('Should Unset', function () {
        assert.deepStrictEqual(
            SQLParser.makeMongoAggregate(`select _id,x,y from customers`, {
                unsetId: true,
                database: 'PostgresQL',
            }),
            {
                collections: ['customers'],
                pipeline: [
                    {
                        $project: {
                            _id: '$_id',
                            x: '$x',
                            y: '$y',
                        },
                    },
                ],
                type: 'aggregate',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.makeMongoAggregate(`select x,y from customers`, {
                unsetId: true,
                database: 'PostgresQL',
            }),
            {
                collections: ['customers'],
                pipeline: [
                    {
                        $project: {
                            x: '$x',
                            y: '$y',
                        },
                    },
                    {$unset: '_id'},
                ],
                type: 'aggregate',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.makeMongoAggregate(`select x,y from customers`, {
                unsetId: false,
                database: 'PostgresQL',
            }),
            {
                collections: ['customers'],
                pipeline: [
                    {
                        $project: {
                            x: '$x',
                            y: '$y',
                        },
                    },
                ],
                type: 'aggregate',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.makeMongoAggregate(`select * from customers`, {
                unsetId: true,
                database: 'PostgresQL',
            }),
            {
                collections: ['customers'],
                pipeline: [],
                type: 'aggregate',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.makeMongoAggregate(
                `select x,y from customers order by _id`,
                {
                    unsetId: true,
                    database: 'PostgresQL',
                }
            ),
            {
                collections: ['customers'],
                pipeline: [
                    {
                        $project: {
                            x: '$x',
                            y: '$y',
                        },
                    },
                    {
                        $sort: {
                            _id: 1,
                        },
                    },
                    {$unset: '_id'},
                ],
                type: 'aggregate',
            },
            'Invalid parse'
        );
    });
});
