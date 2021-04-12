const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');
const $equal = require('deep-equal');

const _queryTests = require('./MongoQueryTests.json');
const _aggregateTests = require('./MongoAggregateTests.json');

const arithmeticExpressionOperators = require('./expressionOperators/ArithmeticExpressionOperators')
const arrayExpressionOperators = require('./expressionOperators/ArrayExpressionOperators')
const booleanExpressionOperators = require('./expressionOperators/BooleanExpressionOperators')
const comparisonExpressionOperators = require('./expressionOperators/ComparisonExpressionOperators')


describe('SQL Parser', function () {

    describe('should parse from sql ast: SQLParser.parseSQLtoAST', function () {
        it('should parse simple sql', function () {
            let ast=SQLParser.parseSQLtoAST('select * from `collection`')
            assert(ast.from[0].table==='collection',"Invalid from")
        })

        it('should return an ast when ast passed', function () {
            let ast=SQLParser.parseSQLtoAST('select * from `collection`')
            let ast2=SQLParser.parseSQLtoAST(ast)
            assert(ast===ast2,"Invalid returns from ast")
        })

        it('should fail if no collection passed', function () {
            try{
                let ast=SQLParser.parseSQLtoAST('select 1');

            }catch(exp){
                return assert.equal(exp.message,"SQL statement requires at least 1 collection","Invalid error message")
            }
            assert(false,'No error')
        })

        it('should fail on an invalid statement', function () {
            try{
                let ast=SQLParser.parseSQLtoAST("select *  `collection`");

            }catch(exp){
                return assert.equal(exp.message,'1:11 - Expected "#", ",", "--", "/*", ";", "FOR", "FROM", "GROUP", "HAVING", "LIMIT", "ORDER", "UNION", "WHERE", [ \\t\\n\\r], or end of input but "`" found.')
            }
            assert(false,'No error')
        })

        it('should fail on an invalid statement 2', function () {
            try{
                let ast=SQLParser.parseSQLtoAST("select * from `collection` with unwind");

            }catch(exp){
                return assert.equal(exp.message,'1:32 - Expected [A-Za-z0-9_] but " " found.')
            }
            assert(false,'No error')
        })

        it('should fail on no as with function', function () {
            try{
                let ast=SQLParser.parseSQLtoAST("select Name,sum(`Replacement Cost`,2)  from `films`");

            }catch(exp){
                return assert.equal(exp.message,'Requires as for function:sum')
            }
            assert(false,'No error')
        })

        it('should fail on no as with binary expr', function () {
            try{
                let ast=SQLParser.parseSQLtoAST("select Name,a>b from `films`");

            }catch(exp){
                return assert.equal(exp.message,'Requires as for binary_expr')
            }
            assert(false,'No error')
        })

        it('should fail on no as with aggr func', function () {
            try{
                let ast=SQLParser.parseSQLtoAST("select sum(a) from `films`");

            }catch(exp){
                return assert.equal(exp.message,'Requires as for aggr_func:SUM')
            }
            assert(false,'No error')
        })

    })

    describe('should test can query: SQLParser.canQuery', function () {
        it('should test a simple sql', function () {
            assert(SQLParser.canQuery('select * from `collection`'), "Invalid can query")
        })

        it('should test a simple sql with where', function () {
            assert(SQLParser.canQuery('select * from `collection` where a=1'), "Invalid can query")
        })

        it('should test a simple sql with names', function () {
            assert(SQLParser.canQuery('select Title,Description from `collection`'), "Invalid can query")
        })

        it('should test a simple sql with unwind', function () {
            assert(!SQLParser.canQuery('select unwind(a) as b from `collection` where a=1'), "Invalid can query")
        })

        it('should test a simple sql with a group by', function () {
            assert(!SQLParser.canQuery('select b,sum(a) as c from `collection` where a=1 group by b'), "Invalid can query")
        })

        it('should test a simple sql with a join', function () {
            assert(!SQLParser.canQuery('select b,sum(a) as c from `collection` where a=1 group by b'), "Invalid can query")
        })

        it('should test a simple sql with single abs', function () {
            assert(SQLParser.canQuery('select abs(`Replacement Cost`) as s from `films`'), "Invalid can query")
        });

        it('should test a simple sql with single sum', function () {
            assert(SQLParser.canQuery('select sum(`Replacement Cost`,2) as s from `films`'), "Invalid can query")
        });

        it('should test a simple sql with single for aggregate', function () {
            assert(!SQLParser.canQuery('select sum(`Replacement Cost`) as s from `films`'), "Invalid can query")
        });

        it('should test a simple sql with single sum', function () {
            assert(SQLParser.canQuery('select avg(`Replacement Cost`,2) as s from `films`'), "Invalid can query")
        })

        it('should test a simple sql with single for avg', function () {
            assert(!SQLParser.canQuery('select avg(`Replacement Cost`) as s from `films`'), "Invalid can query")
        })

        it('should test a simple sql with single for aggregate', function () {
            assert(!SQLParser.canQuery('select count(1) as s from `films`'), "Invalid can query")
        })

        it('should test a simple sql with an expr sum', function () {
            assert(SQLParser.canQuery('select `Replacement Cost` + 2 as s from `films`'), "Invalid can query")
        })

        it('should test a simple sql with an expr sum', function () {
            assert(SQLParser.canQuery('select `Replacement Cost` + 2 as s from `films`'), "Invalid can query")
        })

        it('should test a simple sql with a convert', function () {
            assert(SQLParser.canQuery('select convert(`Replacement Cost`) as s from `films`'), "Invalid can query")
        })

        it('should nt allow *,function ', function () {
            assert(!SQLParser.canQuery("select *,convert(`Replacement Cost`,'int') as s from `films`"), "Invalid can query")
        })

        it('should nt allow where functions ', function () {
            assert(!SQLParser.canQuery(" select * from `films` where arraySize(Rentals)>10 and arraySize(Rentals)<90"), "Invalid can query")
        })

        it('should nt allow where functions with complex where ', function () {
            assert(!SQLParser.canQuery(" select * from `films` where arraySize(Rentals)>10 and (id=10 or arraySize(Rentals)<90)"), "Invalid can query")
        })

    });

    it('should run query tests', function () {
        let results = _queryTests.map(t => {
            if (t.error) {
                try {
                    SQLParser.parseSQL(t.query);
                    return {
                        query: t.query,
                        passed: false,
                        error: "Error condition not met"
                    };
                } catch (exp) {
                    return {
                        query: t.query,
                        passed: exp.message === t.error,
                        error: exp.message !== t.error ? `${exp.message} != ${t.error}` : null
                    };
                }
            } else {
                try {
                    let parsedQuery = SQLParser.parseSQL(t.query);
                    return {
                        query: t.query,
                        passed: $equal(t.output, parsedQuery),
                        error: !$equal(t.output, parsedQuery) ? JSON.stringify(parsedQuery) : null
                    };
                } catch (exp) {
                    return {
                        query: t.query,
                        passed: false,
                        error: exp.message
                    };
                }

            }
        });

        results.forEach(r => {
            if (r.passed) {
                console.log(`\u2714 ${r.query}`);
            } else {
                console.error(`\u2716 ${r.query} ${r.error || ""}`);
            }

        })

        assert.equal(results.filter(r => !r.passed).length, 0, "Mongo Query parsing errors")

    });

    it('should run aggregate tests', function () {
        let results = _aggregateTests.map(t => {
            if (t.error) {
                try {
                    SQLParser.makeMongoAggregate(t.query);
                    return {
                        query: t.query,
                        passed: false,
                        error: "Error condition not met"
                    };
                } catch (exp) {
                    return {
                        query: t.query,
                        passed: exp.message === t.error,
                        error: exp.message !== t.error ? `${exp.message} != ${t.error}` : null
                    };
                }
            } else {
                try {
                    let parsedQuery = SQLParser.makeMongoAggregate(t.query);
                    return {
                        query: t.query,
                        passed: $equal(t.output, parsedQuery),
                        error: !$equal(t.output, parsedQuery) ? JSON.stringify(parsedQuery) : null
                    };
                } catch (exp) {
                    return {
                        query: t.query,
                        passed: false,
                        error: exp.message
                    };
                }

            }
        });

        results.forEach(r => {
            if (r.passed) {
                console.log(`\u2714 ${r.query}`);
            } else {
                console.error(`\u2716 ${r.query} ${r.error || ""}`);
            }

        })

        assert.equal(results.filter(r => !r.passed).length, 0, "Mongo Query parsing errors")

    });

    it('should parse plain query', function () {

        assert.throws(() => { SQLParser.parseSQL("select sum(cnt) from `global-test`") }, Error, "Invalid parse");

        assert.throws(() => { SQLParser.parseSQL("select case when x=1 then 1 else 0 end as d from `global-test`") }, Error, "Invalid parse");

        assert.throws(() => { SQLParser.parseSQL("select subtract(convert('1','int'),abs(`a`)) from `global-test`") }, Error, "Invalid parse");

    });

    it('should parse plain query 2', function () {
        assert.deepEqual(SQLParser.parseSQL("select `a.b` as Id ,Name from `global-test` where `a.b`>1"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                "Id": "$a.b",
                Name: 1
            },
            query: {
                "a.b": { "$gt": 1 }
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.parseSQL("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10"), {
            "collection": "global-test",
            "limit": 10,
            projection: {
                "Id": "$a.b",
                Name: 1
            },
            query: {
                "a.b": { "$gt": 1 }
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.parseSQL("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5"), {
            "collection": "global-test",
            "limit": 10,
            skip: 5,
            projection: {
                "Id": "$a.b",
                Name: 1
            },
            query: {
                "a.b": { "$gt": 1 }
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.parseSQL("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5"), {
            "collection": "global-test",
            "limit": 10,
            skip: 5,
            projection: {
                "Id": "$a.b",
                Name: 1
            },
            query: {
                "a.b": { "$gt": 1 }

            }
        }, "Invalid parse");
    });

    // SQL to MongoBD Mapping examples from docs.mongobd.com
    it('SQL to MongoBD Mapping', function () {

        //TODO ID issue shoot
        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT id, user_id, status FROM people`), {
            limit: 100,
            collection: 'people',
            projection: { id: 1, user_id: 1, status: 1 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT user_id, status FROM people`), {
            limit: 100,
            collection: 'people',
            projection: { user_id: 1, status: 1 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A"`), {
            limit: 100,
            collection: 'people',
            query: { status: 'A' }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT user_id, status FROM people WHERE status = "A"`), {
            limit: 100,
            collection: 'people',
            projection: { user_id: 1, status: 1 },
            query: { status: 'A' }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE status != "A"`), {
            limit: 100,
            collection: 'people',
            query: { status: { '$ne': 'A' } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A" AND age = 50`), {
            limit: 100,
            collection: 'people',
            query: { status: 'A', age: 50 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A" OR age = 50`), {
            limit: 100,
            collection: 'people',
            query: {
                '$or': [{ status: 'A' }, { age: 50 }]
            }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE age > 25`), {
            limit: 100,
            collection: 'people',
            query: { age: { '$gt': 25 } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE age < 25`), {
            limit: 100,
            collection: 'people',
            query: { age: { '$lt': 25 } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE age > 25 and age <30`), {
            limit: 100,
            collection: 'people',
            query: { age: { '$gt': 25, '$lt': 30 } }
        }, "Invalid parse");


        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE user_id like "bc%"`), {
            limit: 100,
            collection: 'people',
            query: { user_id: { '$regex': "^bc", '$options': 'i' } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT * FROM people WHERE status = "A" ORDER BY user_id ASC`), {
            limit: 100,
            collection: 'people',
            query: { status: 'A' },
            sort: { user_id: 1 }
        }, "Invalid parse");
    });

    describe('Arithmetic Expression Operators', function () {
        for (const [key, value] of Object.entries(arithmeticExpressionOperators.tests)) {
            it(key, function () {
                assert.deepStrictEqual(SQLParser.parseSQL(value.query, value.type), value.output, "Invalid parse");
            });
        }
        {
            // // it('should parse aggregate 1', function () {
            // //     assert.deepEqual(SQLParser.makeMongoAggregate("select state,avg(`Replacement Cost`) as avgAge from `films` group by `state`"), {
            // //         "collections": ["films"],
            // //         pipeline: [
            // //             {
            // //                 $match: {
            // //                     state: "a"
            // //                 }
            // //             },
            // //             {

            // //                 $group: {
            // //                     _id: { state: "$state" },
            // //                     avgAge: {
            // //                         $avg: "$age"
            // //                     }
            // //                 }
            // //             }, {
            // //                 $projection: {
            // //                     "state": "$_id.state",
            // //                     avgAge: 1,
            // //                     _id: -1
            // //                 }
            // //             }
            // //         ]
            // //     }, "Invalid parse");
            // // });
        }
    });

    describe('Array Expression Operators', function () {
        for (const [key, value] of Object.entries(arrayExpressionOperators.tests)) {
            it(key, function () {
                assert.deepStrictEqual(SQLParser.parseSQL(value.query, value.type), value.output, "Invalid parse");
            });
        }
    });

    describe('Boolean Expression Operators', function () {
        for (const [key, value] of Object.entries(booleanExpressionOperators.tests)) {
            it(key, function () {
                assert.deepStrictEqual(SQLParser.parseSQL(value.query, value.type), value.output, "Invalid parse");
            });
        }
    });

    describe('Comparison Expression Operators', function () {
        for (const [key, value] of Object.entries(comparisonExpressionOperators.tests)) {
            it(key, function () {
                assert.deepStrictEqual(SQLParser.parseSQL(value.query, value.type), value.output, "Invalid parse");
            });
        }
    });
});
