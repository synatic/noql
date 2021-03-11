const assert = require('assert');
const SQLParser = require('../lib/SQLParser.js');
const $equal = require('deep-equal');

const _queryTests = require('./MongoQueryTests.json');
const _aggregateTests = require('./MongoAggregateTests.json');

describe('SQL Parser', function () {

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
        it('abs', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select abs(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $abs: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('add', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select sum(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $add: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('ceil', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select ceiling(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $ceil: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('divide', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select age / 10 as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $group: {
                            _id: null,
                            aggr: {
                                $divide: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('exp', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select exp(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $exp: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('floor', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select floor(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $floor: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('ln', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select log(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $ln: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('log', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select log(age, 2) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                // $ln: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('log10', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select log10(age) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $log10: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('mod', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select mod(age, 10) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $mod: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('multiply', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select age * 10 as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $multiply: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('pow', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select pow(age, 10) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $pow: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        it('round', function () {
            assert.deepStrictEqual(SQLParser.parseSQL("select round(age, 2) as aggr from `person` where `state`='a'", 'aggregate'), {
                collections: ["person"],
                pipeline: [
                    {
                        $match: {
                            state: "a"
                        }
                    },
                    {
                        $projection: {
                            aggr: 1
                        }
                    }, {
                        $project: {
                            aggr: {
                                $round: "$age"
                            }
                        }
                    }
                ]
            }, "Invalid parse");
        });

        // it('should parse aggregate 1', function () {
        //     assert.deepEqual(SQLParser.makeMongoAggregate("select state,avg(age) as avgAge from `person` where `state`='a' group by `state`"), {
        //         "collections": ["person"],
        //         pipeline: [
        //             {
        //                 $match: {
        //                     state: "a"
        //                 }
        //             },
        //             {

        //                 $group: {
        //                     _id: { state: "$state" },
        //                     avgAge: {
        //                         $avg: "$age"
        //                     }
        //                 }
        //             }, {
        //                 $projection: {
        //                     "state": "$_id.state",
        //                     avgAge: 1,
        //                     _id: -1
        //                 }
        //             }
        //         ]
        //     }, "Invalid parse");
        // });
    });
});
