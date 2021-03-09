const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');

describe('SQL Parser', function () {
    it('should parse plain query', function () {
        assert.deepEqual(SQLParser.makeMongoQuery("select * from `global-test`"), {
            "collection": "global-test",
            "limit": 100
        }, "Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select sum(`a`,2) as s from `global-test`"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                s: {
                    "$add": [
                        "$a",
                        {
                            "$literal": 2
                        }
                    ]
                }
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select Id,Name from `global-test`"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                Id: 1,
                Name: 1
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select Id as id,Name from `global-test`"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                id: "$Id",
                Name: 1
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test`"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                "Id": "$a.b",
                Name: 1
            }
        }, "Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select 'Name' as Id ,Name from `global-test`"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                "Id": { $literal: 'Name' },
                Name: 1
            }
        }, "Invalid parse");

        assert.throws(() => { SQLParser.makeMongoQuery("select sum(cnt) from `global-test`") }, Error, "Invalid parse");

        assert.throws(() => { SQLParser.makeMongoQuery("select case when x=1 then 1 else 0 end as d from `global-test`") }, Error, "Invalid parse");

        assert.throws(() => { SQLParser.makeMongoQuery("select subtract(convert('1','int'),abs(`a`)) from `global-test`") }, Error, "Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select subtract(convert('1','int'),abs(`a`)) as d from `global-test`"), {
            "collection": "global-test",
            "limit": 100,
            projection: {
                "d": {
                    "$subtract": [
                        {
                            "$convert": {
                                "input": {
                                    "$literal": "1"
                                },
                                "to": "int"
                            }
                        },
                        {
                            "$abs": "$a"
                        }
                    ]
                }
            }
        }, "Invalid parse")

    });

    it('should parse plain query 2', function () {
        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1"), {
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

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10"), {
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

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5"), {
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

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5"), {
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
        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people`), {
            limit: 100,
            collection: 'people'
        }, "Invalid parse");

        //TODO ID issue shoot
        assert.deepStrictEqual(SQLParser.parseSQL(`SELECT id, user_id, status FROM people`), {
            type:"query",
            limit: 100,
            collection: 'people',
            projection: { id: 1, user_id: 1, status: 1 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT user_id, status FROM people`), {
            limit: 100,
            collection: 'people',
            projection: { user_id: 1, status: 1 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE status = "A"`), {
            limit: 100,
            collection: 'people',
            query: { status: 'A' }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT user_id, status FROM people WHERE status = "A"`), {
            limit: 100,
            collection: 'people',
            projection: { user_id: 1, status: 1 },
            query: { status: 'A' }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE status != "A"`), {
            limit: 100,
            collection: 'people',
            query: { status: { '$ne': 'A' } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE status = "A" AND age = 50`), {
            limit: 100,
            collection: 'people',
            query: { status: 'A', age: 50 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE status = "A" OR age = 50`), {
            limit: 100,
            collection: 'people',
            query: {
                '$or': [{ status: 'A' }, { age: 50 }]
            }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE age > 25`), {
            limit: 100,
            collection: 'people',
            query: { age: { '$gt': 25 } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE age < 25`), {
            limit: 100,
            collection: 'people',
            query: { age: { '$lt': 25 } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE age > 25 and age <30`), {
            limit: 100,
            collection: 'people',
            query: {age: { '$gt': 25 , '$lt': 30 }}
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE user_id like "%bc%"`), {
            limit: 100,
            collection: 'people',
            query: { user_id: { '$regex': /bc/, '$options': 'i' } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE user_id like "bc%"`), {
            limit: 100,
            collection: 'people',
            query: { user_id: { '$regex': /^bc/, '$options': 'i' } }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT * FROM people WHERE status = "A" ORDER BY user_id ASC`), {
            limit: 100,
            collection: 'people',
            query: { status: 'A' },
            sort: { user_id: 1 }
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT COUNT(*) FROM people`), {
            limit: 100,
            collection: 'people',
            count: true
        }, "Invalid parse");

        assert.deepStrictEqual(SQLParser.makeMongoQuery(`SELECT COUNT(*) FROM people where status = 'a'`), {
            collection: 'people',
            query: { status: 'A' },
            count:true
        }, "Invalid parse");
    });

    it('should parse aggregate 1', function () {
        assert.deepEqual(SQLParser.makeMongoAggregate("select avg(age) as avgAge from `person` where `state`='a'"), {
            "collections": ["person"],
            pipeline:[
                {$match:{
                    state:"a"
                    }},
                {

                    $group:{
                        _id:1,
                        avgAge:{
                            $avg:"$age"
                        }
                    }
                },{
                $projection:{
                    avgAge:1,
                    _id:-1
                }
                }
            ]
        }, "Invalid parse");

        it('should parse aggregate 1', function () {
            assert.deepEqual(SQLParser.makeMongoAggregate("select state,avg(age) as avgAge from `person` where `state`='a' group by `state`"), {
                "collections": ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {

                        $group: {
                            _id: {state:"$state"},
                            avgAge: {
                                $avg: "$age"
                            }
                        }
                    }, {
                        $projection: {
                            "state":"$_id.state",
                            avgAge: 1,
                            _id: -1
                        }
                    }
                ]
            }, "Invalid parse");
        });
    });
});
