const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');

const _queryTests = [].concat(
    require('./queryTests/queryTests.json'),
    require('./queryTests/objectOperators.json'),
    require('./queryTests/arrayOperators.json'),
    require('./queryTests/stringOperators.json'),
    require('./queryTests/dateOperators.json'),
    require('./queryTests/arithmeticOperators.json'),
    require('./queryTests/conversionOperators.json'),
    require('./queryTests/comparisonOperators.json'),
    require('./queryTests/columnOperators.json')
);
const _aggregateTests = [].concat(require('./aggregateTests/aggregateTests.json'));

describe('SQL Parser', function () {
    describe('should parse from sql ast: SQLParser.parseSQLtoAST', function () {
        it('should parse simple sql', function () {
            const {ast} = SQLParser.parseSQLtoAST('select * from `collection`');
            assert(ast.from[0].table === 'collection', 'Invalid from');
        });

        it('should return an ast when ast passed', function () {
            const ast = SQLParser.parseSQLtoAST('select * from `collection`');
            const ast2 = SQLParser.parseSQLtoAST(ast);
            assert(ast === ast2, 'Invalid returns from ast');
        });

        it('should fail if no collection passed', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select 1');
            } catch (exp) {
                return assert.equal(exp.message, 'SQL statement requires at least 1 collection', 'Invalid error message');
            }
            assert(false, 'No error');
        });

        it('should fail on an invalid statement', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select *  `collection`');
            } catch (exp) {
                return assert.equal(
                    exp.message,
                    '1:11 - Expected "#", ",", "--", "/*", ";", "FOR", "FROM", "GROUP", "HAVING", "LIMIT", "ORDER", "UNION", "WHERE", [ \\t\\n\\r], or end of input but "`" found.'
                );
            }
            assert(false, 'No error');
        });

        it('should fail on an invalid statement 2', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select * from `collection` with unwind');
            } catch (exp) {
                return assert.equal(exp.message, '1:32 - Expected [A-Za-z0-9_] but " " found.');
            }
            assert(false, 'No error');
        });

        it('should fail on no as with function', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select Name,sum(`Replacement Cost`,2)  from `films`');
            } catch (exp) {
                return assert.equal(exp.message, 'Requires as for function:sum');
            }
            assert(false, 'No error');
        });

        it('should fail on no as with binary expr', function () {
            try {
                // eslint-disable-next-line no-unused-vars
                const ast = SQLParser.parseSQLtoAST('select Name,a>b from `films`');
            } catch (exp) {
                return assert.equal(exp.message, 'Requires as for binary_expr');
            }
            assert(false, 'No error');
        });


    });

    describe('should test can query: SQLParser.canQuery', function () {
        it('should test a simple sql', function () {
            assert(SQLParser.canQuery('select * from `collection`'), 'Invalid can query');
        });

        it('should test a simple sql with where', function () {
            assert(SQLParser.canQuery('select * from `collection` where a=1'), 'Invalid can query');
        });

        it('should test a simple sql with names', function () {
            assert(SQLParser.canQuery('select Title,Description from `collection`'), 'Invalid can query');
        });

        it('should test a simple sql with unwind', function () {
            assert(!SQLParser.canQuery('select unwind(a) as b from `collection` where a=1'), 'Invalid can query');
        });

        it('should test a simple sql with a group by', function () {
            assert(!SQLParser.canQuery('select b,sum(a) as c from `collection` where a=1 group by b'), 'Invalid can query');
        });

        it('should test a simple sql with a join', function () {
            assert(!SQLParser.canQuery('select b,sum(a) as c from `collection` where a=1 group by b'), 'Invalid can query');
        });

        it('should test a simple sql with single abs', function () {
            assert(SQLParser.canQuery('select abs(`Replacement Cost`) as s from `films`'), 'Invalid can query');
        });

        it('should test a simple sql with single sum', function () {
            assert(SQLParser.canQuery('select sum(`Replacement Cost`,2) as s from `films`'), 'Invalid can query');
        });

        it('should test a simple sql with single for aggregate', function () {
            assert(!SQLParser.canQuery('select sum(`Replacement Cost`) as s from `films` group by s'), 'Invalid can query');
        });

        it('should test a simple sql with single sum', function () {
            assert(SQLParser.canQuery('select avg(`Replacement Cost`,2) as s from `films`'), 'Invalid can query');
        });

        it('should test a simple sql with single for avg', function () {
            assert(!SQLParser.canQuery('select id,avg(`Replacement Cost`) as s from `films` group by id'), 'Invalid can query');
        });

        it('should test a simple sql with an expr sum', function () {
            assert(SQLParser.canQuery('select `Replacement Cost` + 2 as s from `films`'), 'Invalid can query');
        });

        it('should test a simple sql with an expr sum', function () {
            assert(SQLParser.canQuery('select `Replacement Cost` + 2 as s from `films`'), 'Invalid can query');
        });

        it('should test a simple sql with a convert', function () {
            assert(SQLParser.canQuery('select convert(`Replacement Cost`) as s from `films`'), 'Invalid can query');
        });

        it('should nt allow *,function ', function () {
            assert(!SQLParser.canQuery("select *,convert(`Replacement Cost`,'int') as s from `films`"), 'Invalid can query');
        });

        it('should not allow with where on sub query ', function () {
            assert(
                !SQLParser.canQuery(
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
                        SQLParser.makeMongoQuery(t.query);
                        assert(false, 'No error');
                    } catch (exp) {
                        assert.equal(exp.message, t.error);
                    }
                } else {
                    let err = null;
                    let parsedQuery;
                    try {
                        parsedQuery = SQLParser.makeMongoQuery(t.query);
                    } catch (exp) {
                        err = exp.message;
                    }
                    assert(!err, err);
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
                    parsedQuery = SQLParser.makeMongoQuery(t.query);
                    parsedAggregate = SQLParser.makeMongoAggregate(t.query);
                } catch (exp) {
                    err = exp.message;
                }
                assert(!err, err);
                const pipeline = [];

                if (parsedQuery.query) pipeline.push({$match: parsedQuery.query});
                if (parsedQuery.projection) pipeline.push({$project: parsedQuery.projection});

                if (parsedQuery.sort) pipeline.push({$sort: parsedQuery.sort});
                if (parsedQuery.limit !== 100) pipeline.push({$limit: parsedQuery.limit});
                if (parsedQuery.skip) pipeline.push({$skip: parsedQuery.skip});
                assert.deepEqual(
                    parsedAggregate,
                    {
                        collections: [parsedQuery.collection],
                        pipeline: pipeline,
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
                        SQLParser.makeMongoAggregate(t.query);
                        assert(false, 'No error');
                    } catch (exp) {
                        assert.equal(exp.message, t.error);
                    }
                } else {
                    let err = null;
                    let parsedQuery;
                    try {
                        parsedQuery = SQLParser.makeMongoAggregate(t.query);
                    } catch (exp) {
                        err = exp.message;
                    }
                    assert(!err, err);
                    assert.deepEqual(parsedQuery, t.output, 'Invalid parse');
                }
            });
        }
    });

    it('should parse plain query 2', function () {
        assert.deepEqual(
            SQLParser.parseSQL('select `a.b` as Id ,Name from `global-test` where `a.b`>1'),
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
            SQLParser.parseSQL('select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10'),
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
            SQLParser.parseSQL('select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5'),
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
            SQLParser.parseSQL('select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5'),
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
            SQLParser.parseSQL(`SELECT id, user_id, status FROM people`),
            {
                limit: 100,
                collection: 'people',
                projection: {id: '$id', user_id: '$user_id', status: '$status'},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT user_id, status FROM people`),
            {
                limit: 100,
                collection: 'people',
                projection: {user_id: '$user_id', status: '$status'},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A"`),
            {
                limit: 100,
                collection: 'people',
                query: {status: {$eq: 'A'}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT user_id, status FROM people WHERE status = "A"`),
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
            SQLParser.parseSQL(`SELECT * FROM people WHERE status != "A"`),
            {
                limit: 100,
                collection: 'people',
                query: {status: {$ne: 'A'}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A" AND age = 50`),
            {
                limit: 100,
                collection: 'people',
                query: {$and:[{status: {$eq: 'A'}}, {age: {$eq: 50}}]},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A" OR age = 50`),
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
            SQLParser.parseSQL(`SELECT * FROM people WHERE age > 25`),
            {
                limit: 100,
                collection: 'people',
                query: {age: {$gt: 25}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE age < 25`),
            {
                limit: 100,
                collection: 'people',
                query: {age: {$lt: 25}},
                type: 'query',
            },
            'Invalid parse'
        );

        assert.deepStrictEqual(
            SQLParser.parseSQL(`SELECT * FROM people WHERE age > 25 and age <30`),
            {
                limit: 100,
                collection: 'people',
                query: {
                    "$and": [
                        {
                            "age": {
                                "$gt": 25
                            }
                        },
                        {
                            "age": {
                                "$lt": 30
                            }
                        }
                    ]
                },
                type: 'query',
            },
            'Invalid parse'
        );
    });
});
