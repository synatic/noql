const assert = require('assert');
const {ObjectID} = require('mongodb');
const cloneDeep = require('clone-deep');
const SQLParser = require('../lib/SQLParser.js');

describe('SQL Parser', function () {
    it('should parse plain query', function () {
        assert.deepEqual(SQLParser.makeMongoQuery("select * from `global-test`"),{
            "collection": "global-test",
            "limit": 100
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select sum(`a`,2) as s from `global-test`"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
                s:{
                    "$add": [
                        "$a",
                        {
                            "$literal": 2
                        }
                    ]
                }
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select Id,Name from `global-test`"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
                Id:1,
                Name:1
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select Id as id,Name from `global-test`"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
                id:"$Id",
                Name:1
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test`"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
                "Id":"$a.b",
                Name:1
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select 'Name' as Id ,Name from `global-test`"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
                "Id": {$literal:'Name'},
                Name:1
            }
        },"Invalid parse");

        assert.throws(()=>{SQLParser.makeMongoQuery("select sum(cnt) from `global-test`")},Error,"Invalid parse");

        assert.throws(()=>{SQLParser.makeMongoQuery("select case when x=1 then 1 else 0 end as d from `global-test`")},Error,"Invalid parse");

        assert.throws(()=>{SQLParser.makeMongoQuery("select subtract(convert('1','int'),abs(`a`)) from `global-test`")},Error,"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select subtract(convert('1','int'),abs(`a`)) as d from `global-test`"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
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
        },"Invalid parse")

    });

    it('should parse plain query 2', function () {
        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1"),{
            "collection": "global-test",
            "limit": 100,
            projection:{
                "Id":"$a.b",
                Name:1
            },
            query:{
                "$gt": [
                    "a.b",
                    1
                ]
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10"),{
            "collection": "global-test",
            "limit": 10,
            projection:{
                "Id":"$a.b",
                Name:1
            },
            query:{
                "$gt": [
                    "a.b",
                    1
                ]
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5"),{
            "collection": "global-test",
            "limit": 10,
            skip:5,
            projection:{
                "Id":"$a.b",
                Name:1
            },
            query:{
                "$gt": [
                    "a.b",
                    1
                ]
            }
        },"Invalid parse");

        assert.deepEqual(SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5"),{
            "collection": "global-test",
            "limit": 10,
            skip:5,
            projection:{
                "Id":"$a.b",
                Name:1
            },
            query:{
                "a.b":{"$gt": 1}

            }
        },"Invalid parse");
    });


});
