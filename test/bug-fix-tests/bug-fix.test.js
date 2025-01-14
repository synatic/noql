const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');
const {getAllSchemas} = require('../utils/get-all-schemas.js');
const {parseSQLtoAST, makeMongoAggregate} = require('../../lib/SQLParser');
const fs = require('fs/promises');
const emptyResultsBugPipeline = require('./empty-results-pipeline.json');
const isEqual = require('lodash/isEqual');
const assert = require('node:assert');
const {makeMongoQuery} = require('../../lib/make');
describe('bug-fixes', function () {
    this.timeout(90000);
    const fileName = 'bug-fix';
    /** @type {'test'|'write'} */
    const mode = 'test';
    const dirName = __dirname;
    /** @type {import("../utils/query-tester/types.js").QueryResultTester} */
    let queryResultTester;
    /** @type {import("mongodb").MongoClient} */
    let mongoClient;
    /** @type {import("mongodb").Db} */
    let database;
    before(async function () {
        const {client, db} = await setup();
        mongoClient = client;
        database = db;
        queryResultTester = buildQueryResultTester({
            dirName,
            fileName,
            mongoClient,
            mode,
        });
    });

    after(function (done) {
        disconnect().then(done).catch(done);
    });

    describe('true/false case statement bug', () => {
        it('should work for case 1', async () => {
            const queryString = `
            SELECT  gb.name as gbName,
                    ub.name as ubName,
                    unset(_id),
                    ifnull(ub.dateAchieved, NULL) AS dateAchieved,
                    CASE
                        WHEN (ub.dateAchieved IS NOT NULL) THEN true
                        ELSE false
                    END as achieved
            FROM function-test-data 'gb'
            LEFT OUTER JOIN (
                SELECT *
                FROM function-test-data
                WHERE testId="bugfix.true-false.case1"
            ) 'ub|first|optimize' on ub.leftSide=gb.rightSide
            WHERE gb.testId="bugfix.true-false.case1"`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.true-false.case1',
            });
        });
    });
    describe('join function', () => {
        it('should be able to join strings together with a symbol in a standard select', async () => {
            const queryString = `
            SELECT  join(names,',') as names,
                    unset(_id)
            FROM function-test-data
            WHERE testId="bugfix.join-fn.case1"
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.join-fn.case1',
            });
        });
        // it('should be able to join strings together with a symbol in a group by', async () => {
        //     const queryString = `
        //     SELECT testId,
        //             join(name,',') as names
        //     FROM function-test-data
        //     WHERE testId="bugfix.join-fn.case2"
        //     GROUP BY testId
        //     `;
        //     const res = await queryResultTester({
        //         queryString: queryString,
        //         casePath: 'bugfix.current-date.case2',
        //         mode: 'write',
        //     });
        //     const correctHcp = [
        //         {
        //             $match: {
        //                 testId: {
        //                     $eq: 'bugfix.join-fn.case2',
        //                 },
        //             },
        //         },
        //         {
        //             $group: {
        //                 _id: {
        //                     testId: '$testId',
        //                 },
        //                 names: {
        //                     $accumulator: {
        //                         accumulateArgs: ['$name'],
        //                         init: function () {
        //                             return [];
        //                         },
        //                         accumulate: function (names, name) {
        //                             return names.concat(name);
        //                         },
        //                         merge: function (names1, names2) {
        //                             return names1.concat(names2);
        //                         },
        //                         finalize: function (names) {
        //                             return names.join(',');
        //                         },
        //                         lang: 'js',
        //                     },
        //                 },
        //             },
        //         },
        //         {
        //             $project: {
        //                 testId: '$_id.testId',
        //                 names: '$_id.names',
        //                 _id: 0,
        //             },
        //         },
        //     ];
        // });
    });
    describe('sort order sub query', () => {
        it('Should correctly sort the result set', async () => {
            const queryString = `
                SELECT *
                FROM (
                    SELECT clanId,count(1) as userCount
                    FROM function-test-data
                    WHERE testId='bugfix.sort-order'
                ) c
                ORDER BY c.userCount DESC,
                         c.clanId DESC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.sort-order.case1',
            });
        });
        it('Should correctly sort the result set when not nested', async () => {
            const queryString = `
                SELECT clanId, count(1) as userCount
                FROM function-test-data
                WHERE testId='bugfix.sort-order'
                GROUP BY clanId
                ORDER BY userCount DESC,
                         clanId DESC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.sort-order.case2',
            });
        });
        it('Should correctly sort the result set when not nested with the table name', async () => {
            const queryString = `
                SELECT clanId, count(1) as userCount
                FROM function-test-data
                WHERE testId='bugfix.sort-order'
                GROUP BY clanId
                ORDER BY function-test-data.userCount DESC,
                         function-test-data.clanId DESC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.sort-order.case3',
            });
        });
    });
    describe('coalesce', () => {
        it('Should correctly coalesce the results for numbers', async () => {
            const queryString = `
                SELECT  coalesce(price,0) as Price,
                        coalesce(null,null,3,0) as inlineValue,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.coalesce.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.coalesce.case1',
            });
        });
    });
    describe('rank', () => {
        it('Should correctly rank the results without a partition by', async () => {
            const queryString = `
                SELECT  value,
                        RANK () OVER (
                            ORDER BY value
                        ) rank_number,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.rank.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.rank.case1',
            });
        });
        it('Should correctly rank the results with a partition by', async () => {
            const queryString = `
                SELECT  value,
                        RANK () OVER (
                            PARTITION BY testId
                            ORDER BY value
                        ) rank_number,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.rank.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.rank.case2',
            });
        });
    });
    describe('dense rank', () => {
        it('Should correctly rank the results without a partition by', async () => {
            const queryString = `
                SELECT  value,
                        DENSE_RANK () OVER (
                            ORDER BY value
                        ) rank_number,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.dense-rank.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.dense-rank.case1',
            });
        });
        it('Should correctly rank the results with a partition by', async () => {
            const queryString = `
                SELECT  value,
                        DENSE_RANK () OVER (
                            PARTITION BY testId
                            ORDER BY value
                        ) rank_number,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.dense-rank.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.dense-rank.case2',
            });
        });
    });
    describe('row number', () => {
        it('Should correctly rank the results without a partition by', async () => {
            const queryString = `
                SELECT  value,
                        ROW_NUMBER() OVER (
                            ORDER BY value
                        ) row_number,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.rank.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.row-number.case1',
            });
        });
        it('Should correctly rank the results with a partition by', async () => {
            const queryString = `
                SELECT  value,
                        ROW_NUMBER () OVER (
                            PARTITION BY value
                            ORDER BY value
                        ) rank_number,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.rank.case1'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.row-number.case2',
            });
        });
    });
    describe('ntile', () => {
        it.skip('Should correctly group the results', async () => {
            // https://www.postgresqltutorial.com/postgresql-window-function/postgresql-ntile-function/
            const queryString = `
                SELECT  name,
                        amount,
                        NTILE (3) OVER (
                            ORDER BY amount
                        ) ntile,
                        unset(_id)
                FROM function-test-data
                WHERE testId='bugfix.ntile.case1'
            `;
            const workingPipeline = [
                {
                    $match: {
                        testId: {
                            $eq: 'bugfix.ntile.case1',
                        },
                    },
                },
                {
                    $unset: ['_id'],
                },
                {
                    $project: {
                        name: '$name',
                        amount: '$amount',
                    },
                },
                {
                    $group: {
                        _id: {
                            name: '$name',
                        },
                        amount: {
                            $last: '$amount',
                        },
                    },
                },
                {
                    $project: {
                        name: '$_id.name',
                        amount: '$amount',
                    },
                },
                {
                    $sort: {
                        amount: 1,
                    },
                },
                {
                    $bucketAuto: {
                        groupBy: '$name',
                        buckets: 3,
                        output: {
                            buckets: {
                                $push: {
                                    name: '$name',
                                    amount: '$amount',
                                },
                            },
                        },
                    },
                },
                {
                    $setWindowFields: {
                        sortBy: {
                            _id: 1,
                        },
                        output: {
                            ntile: {
                                $rank: {},
                            },
                        },
                    },
                },
                {
                    $unwind: {
                        path: '$buckets',
                        preserveNullAndEmptyArrays: false,
                    },
                },
                {
                    $project: {
                        name: '$buckets.name',
                        amount: '$buckets.amount',
                        ntile: '$ntile',
                    },
                },
                {
                    $sort: {
                        amount: 1,
                    },
                },
            ];
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.ntile.case1',
                mode: 'write',
                outputPipeline: true,
            });
        });
    });
    describe('to_objectid', () => {
        it('should be able to convert a string object id to an actual ObjectId', async () => {
            const queryString = `
                    SELECT to_objectid('61b0fdcbdee485f7c0682db6') as i
                    FROM customers
                    WHERE _id = to_objectid('61b0fdcbdee485f7c0682db6')
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.to_objectid.case1',
                unsetId: false,
            });
        });
    });
    describe('OBJECT_TO_ARRAY', () => {
        it('should be able to convert a string object id to an actual ObjectId', async () => {
            const queryString = `
                SELECT  id,
                        OBJECT_TO_ARRAY(Address) as test,
                        unset(_id)
                FROM customers LIMIT 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.object_to_array.case1',
                unsetId: false,
            });
        });
    });
    describe('Injected parameters with special characters', () => {
        const mode = 'test';
        const outputPipeline = false;
        it('should be able to do a where statement with lots of special characters using double quotes', async () => {
            const queryString = `
                SELECT  parameter,
                        unset(_id)
                FROM function-test-data ftd
                WHERE ftd.parameter = wrapParam("$eq Isn't a \\"bug\\" \`just\` $ \\\\")
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.special-char-parameters.case1',
                outputPipeline,
                mode,
            });
        });
        it('should be able to do a where statement with lots of special characters using single quotes', async () => {
            const queryString = `
                SELECT  parameter,
                        unset(_id)
                FROM function-test-data ftd
                WHERE ftd.parameter = wrapParam('$eq Isn\\'t a "bug" \`just\` $ \\\\')
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.special-char-parameters.case2',
                outputPipeline,
                mode,
            });
        });
        // it('should be able to do a where statement with lots of special characters using backticks', async () => {
        //     const queryString = `
        //         SELECT  parameter,
        //                 unset(_id)
        //         FROM function-test-data ftd
        //         WHERE ftd.parameter = wrapParam(\`$eq Isn't a "bug" \\\`just\\\` $\`)
        //     `;
        //     await queryResultTester({
        //         queryString: queryString,
        //         casePath: 'bugfix.special-char-parameters.case3',
        //         outputPipeline,
        //         mode,
        //     });
        // });
        it('should be able to do a like statement with lots of special characters', async () => {
            const queryString = `
                SELECT  parameter,
                        unset(_id)
                FROM function-test-data
                WHERE parameter like wrapParam("$eq Isn't a \\"bug\\" \`just\` $ \\\\",true)
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.special-char-parameters.case4',
                outputPipeline,
                mode,
            });
        });
        it('should work for example 1', async () => {
            const queryString = `
                SELECT  id,
                        (f.id + f.Length + 2) as val,
                        unset(_id)
                FROM films f
                where id>1 limit 2
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.special-char-parameters.case5',
                outputPipeline,
                mode,
            });
        });
        it('should be able to do a not like statement with lots of special characters', async () => {
            const queryString = `
                SELECT  parameter,
                        unset(_id)
                FROM function-test-data
                WHERE parameter not like wrapParam("$eq Isn't a \\"bug\\" \`just\` $",true)
                LIMIT 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.special-char-parameters.case6',
                outputPipeline,
                mode,
            });
        });
    });
    // https://stackoverflow.com/questions/63300248/mongodb-aggregation-array-of-objects-to-string-value
    // https://www.mongodb.com/community/forums/t/json-stringify-within-an-aggregation-pipeline/237638
    describe('schema-aware-queries', () => {
        it('should be able to cast a JSON array to a varchar', async () => {
            /**
             * TODO: bug in schema magic merging the schemas:
             * ["a", 1, true, {"b": 3, "c": {}}, [1], null, 2.5]
             * String missing, possibly other issues.
             */
            const queryString = `
                SELECT  testId,
                        cast(jsonObjValues as varchar) as jsonObjValuesStr,
                        cast(stringArray as varchar) as stringArrayStr,
                        cast(numberArray as varchar) as numberArrayStr,
                        cast(jsonArray as varchar) as jsonArrayStr,
                        cast(mixedArray as varchar) as mixedArrayStr,
                        cast(mixedPrimitiveArray as varchar) as mixedPrimitiveArrayStr,
                        cast(objOrArray as varchar) as objOrArrayStr,
                        cast(stringOrObject as varchar) as stringOrObjectStr,
                        cast(commaTest as varchar) as commaTestStr,
                        unset(_id)
                FROM function-test-data
                WHERE testCategory='stringify'
            `;
            const {results} = await queryResultTester({
                queryString: queryString,
                casePath:
                    'bugfix.schema-aware-queries.cast-json-array-to-varchar.case1',
                schemas: await getAllSchemas(database),
            });
            const keysToParse = [
                'jsonObjValuesStr',
                'stringArrayStr',
                'numberArrayStr',
                'jsonArrayStr',
                'mixedArrayStr',
                'mixedPrimitiveArrayStr',
                'objOrArrayStr',
                'commaTestStr',
            ];
            let resultCounter = 0;
            for (const result of results) {
                for (const key of keysToParse) {
                    const str = result[key];
                    if (!str) {
                        continue;
                    }
                    try {
                        JSON.parse(str);
                    } catch (err) {
                        console.error(err);
                        throw new Error(
                            `Unable to parse result ${resultCounter}, key "${key}". Raw String:\n${str}\n${err.message}\n${err.stack}`
                        );
                    }
                }
                resultCounter++;
            }
        });
        it('should be able to cast a JSON array to a varchar with a table alias', async () => {
            const queryString = `
                SELECT  FTD.testId,
                        cast(FTD.jsonObjValues as varchar) as jsonObjValuesStr,
                        unset(_id)
                FROM function-test-data as FTD
                WHERE testCategory='stringify'
            `;
            const {results} = await queryResultTester({
                queryString: queryString,
                casePath:
                    'bugfix.schema-aware-queries.cast-json-array-to-varchar.case2',
                schemas: await getAllSchemas(database),
            });
            const keysToParse = ['jsonObjValuesStr'];
            let resultCounter = 0;
            for (const result of results) {
                for (const key of keysToParse) {
                    const str = result[key];
                    if (!str) {
                        continue;
                    }
                    try {
                        JSON.parse(str);
                    } catch (err) {
                        console.error(err);
                        throw new Error(
                            `Unable to parse result ${resultCounter}, key "${key}". Raw String:\n${str}\n${err.message}\n${err.stack}`
                        );
                    }
                }
                resultCounter++;
            }
        });
        it('should be able to cast a JSON array to a varchar with a table alias inside a sub select', async () => {
            const queryString = `
                SELECT nested.jsonObjValuesStr as subSelectStr
                FROM
                (SELECT  FTD.testId,
                        cast(FTD.jsonObjValues as varchar) as jsonObjValuesStr,
                        unset(_id)
                FROM function-test-data as FTD
                WHERE testCategory='stringify') nested
            `;
            const {results} = await queryResultTester({
                queryString: queryString,
                casePath:
                    'bugfix.schema-aware-queries.cast-json-array-to-varchar.case3',
                schemas: await getAllSchemas(database),
            });
            const keysToParse = ['jsonObjValuesStr'];
            let resultCounter = 0;
            for (const result of results) {
                for (const key of keysToParse) {
                    const str = result[key];
                    if (!str) {
                        continue;
                    }
                    try {
                        JSON.parse(str);
                    } catch (err) {
                        console.error(err);
                        throw new Error(
                            `Unable to parse result ${resultCounter}, key "${key}". Raw String:\n${str}\n${err.message}\n${err.stack}`
                        );
                    }
                }
                resultCounter++;
            }
        });
        it('should auto cast date fields in a where clause with a literal on the right', async () => {
            const queryString = `
                SELECT  *, unset(_id)
                FROM orders
                WHERE orderDate <= '2024-01-01'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.schema-aware-queries.auto-cast.date-case1',
                schemas: await getAllSchemas(database),
                mode,
                ignoreDateValues: true,
            });
        });
        it('should auto cast date fields in a where clause with a literal on the let', async () => {
            const queryString = `
                SELECT  *, unset(_id)
                FROM orders
                WHERE '2024-01-01' >= orderDate
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.schema-aware-queries.auto-cast.date-case2',
                schemas: await getAllSchemas(database),
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
        it('should auto cast date fields in a where clause with 2 columns', async () => {
            const queryString = `
                SELECT  *, unset(_id)
                FROM orders
                WHERE orderDate <= orderDate
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.schema-aware-queries.auto-cast.date-case3',
                schemas: await getAllSchemas(database),
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
    });
    describe('subquery capitalisation', () => {
        it('should not throw an error when the right hand side is capitalised', async () => {
            const queryString = `
                SELECT  Order1.id,
                        Order1.item,
                        unset(_id)
                FROM orders Order1
                INNER JOIN(
                    SELECT * FROM orders) 'Order2|unwind' on Order2.id = Order1.id
                LIMIT 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.subquery-capitalisation.case1',
                mode,
            });
        });
    });
    describe('timestamp query', () => {
        // todo RK this creates a pipeline that is not serialisable as the makeQueryPart converts it to a date object, look at how to fix
        it('should be able to query by timestamp', async () => {
            const queryString = `
                SELECT  *,
                        unset(_id)
                FROM orders
                WHERE orderDate > timestamp '2021-01-01 00:00:00'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.timestamp.case1',
                mode,
                ignoreDateValues: true,
            });
        });
    });
    describe('Large numbers', () => {
        it('limit', async () => {
            const queryString = `
                SELECT  "First Name",
                        "Last Name",
                        unset(_id)
                FROM customers
                LIMIT 9223372036854775807
                offset 598
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.large-number.case1',
            });
        });
    });
    describe('Current_Date', () => {
        it('should work with or without parentheses', async () => {
            const queryString = `
                SELECT  CURRENT_DATE() as Today,
                        current_date as Today2,
                        unset(_id)
                FROM function-test-data
            LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.current-date.case1',
                ignoreDateValues: true,
                mode,
            });
        });
        it('should allow you to compare dates', async () => {
            const queryString = `
                SELECT  id,
                        orderDate,
                        unset(_id)
                FROM orders
                WHERE FIELD_EXISTS('orderDate',true) AND orderDate != null AND orderDate < CURRENT_DATE()
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.current-date.case2',
                mode,
                ignoreDateValues: true,
            });
        });
        it('should allow you to use current_date in the on clause', async () => {
            const queryString = `
                SELECT  o.id,
                        o.orderDate
                FROM orders o
                INNER JOIN inventory i on o.item = i.sku
                    AND o.orderDate <= CURRENT_DATE()
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.current-date.case3',
                mode,
                ignoreDateValues: true,
                unsetId: true,
                outputPipeline: false,
            });
        });
    });
    describe('unique', () => {
        it.skip('should get unique values', async () => {
            const queryString = `
                SELECT DISTINCT
                     item,
                     unset(_id)
                    FROM orders
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.unique.case1',
                ignoreDateValues: true,
                mode: 'test',
            });
        });
    });
    describe('chaining functions in group by', () => {
        it('should be able to use round + sum', async () => {
            const queryString = `
                SELECT  customerId,
                        ROUND(sum(price),0) as roundSumPrice,
                        sum(ROUND(price,0)) as sumRoundPrice
                FROM orders
                GROUP BY customerId
                ORDER BY customerId ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.chain-group-by.case1',
                mode,
            });
        });
        it('should be able to use round + sum without a second argument to round', async () => {
            const queryString = `
                SELECT  customerId,
                        ROUND(sum(price)) as roundSumPrice,
                        sum(ROUND(price)) as sumRoundPrice
                FROM orders
                GROUP BY customerId
                ORDER BY customerId ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.chain-group-by.case2',
            });
        });
        it('should be able to use sum + avg', async () => {
            const queryString = `
                SELECT  customerId,
                        avg(sum(price)) as avgSum,
                        sum(avg(price)) as sumAvg
                FROM orders
                GROUP BY customerId
                ORDER BY customerId ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.chain-group-by.case3',
            });
        });
        it('should be able to use sum + min', async () => {
            const queryString = `
                SELECT  customerId,
                        min(sum(price)) as minSum,
                        sum(min(price)) as sumMin
                FROM orders
                GROUP BY customerId
                ORDER BY customerId ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.chain-group-by.case4',
            });
        });
        it('should be able to use sum + subtract', async () => {
            const queryString = `
                SELECT  customerId,
                        subtract(sum(price),1) as subtractSum,
                        sum(subtract(price, 1)) as sumSubtract
                FROM orders
                GROUP BY customerId
                ORDER BY customerId ASC, subtractSum desc
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.chain-group-by.case5',
            });
        });
    });
    describe('SUM function', () => {
        it('should work with Martins example', async () => {
            const queryString = `
                SELECT
                    sum("stats.2023.06.acord.total") as "2023_06",
                    sum("stats.2023.07.acord.total") as "2023_07",
                    sum("stats.2023.08.acord.total") as "2023_08",
                    sum("stats.2023.09.acord.total") as "2023_09",
                    sum("stats.2023.10.acord.total") as "2023_10",
                    sum("stats.2023.11.acord.total") as "2023_11",
                    sum("stats.2023.12.acord.total") as "2023_12"
                FROM ocr_stats
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.sums.case1',
                mode,
            });
        });
        it('should work with sum(1)', async () => {
            const queryString = `
                SELECT  c.id,
                        sum(1) as cnt
                FROM customers c
                INNER JOIN \`customer-notes\` cn on c.id=cn.id
                WHERE cn.id>1 and c.id>2
                GROUP BY c.id
                HAVING cnt >0
                ORDER BY c.id ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.sums.case2',
                mode,
            });
        });
    });
    describe('extract dates', () => {
        it('Date:EXTRACT', async () => {
            const queryString = `
                SELECT  orderDate,
                        extract(year from orderDate) as year,
                        extract(month from orderDate) as month,
                        extract(day from to_date('2021-10-23')) as day,
                        unset(_id)
                FROM orders
                WHERE orderDate != null
                ORDER BY orderDate ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.extract-dates.case1',
                mode,
                ignoreDateValues: true,
            });
        });
        it('day', async () => {
            const queryString = `
                SELECT  orderDate,
                        day_of_month(orderDate) as day,
                        unset(_id)
                FROM orders
                WHERE orderDate != null
                ORDER BY orderDate ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.extract-dates.case2',
                mode,
                ignoreDateValues: true,
            });
        });
    });
    describe('scratchpad', () => {
        it('Should let you provide the full table name in the on clause on the left', async () => {
            const queryString = `
                SELECT *,unset(_id,company._id,AMSCompany._id,company.orderDate)
                FROM (SELECT *, item  as CName from orders) "company|first"
                LEFT JOIN (select * from inventory) "AMSCompany|first"
                    ON TO_STRING(AMSCompany.sku) = company.CName
                ORDER BY company.id ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'scratchpad.case1',
                mode,
            });
        });
        it('Should let you provide the full table name in the on clause on the right', async () => {
            const queryString = `
                SELECT *,unset(_id,company._id,AMSCompany._id,company.orderDate)
                FROM (SELECT *, item  as CName from orders) "company|first"
                LEFT JOIN (select * from inventory) "AMSCompany|first"
                    ON company.CName = TO_STRING(AMSCompany.sku)
                ORDER BY company.id ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'scratchpad.case1',
                mode,
            });
        });
        it('should allow you to use the like clause in an on', async () => {
            const queryString = `
                SELECT  *,unset(_id,company._id,AMSCompany._id,company.orderDate)
                FROM (SELECT *, item as CName from orders) "company|first"
                LEFT JOIN (select * from inventory) "AMSCompany|first"
                    ON TO_STRING(sku) LIKE company.CName
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'scratchpad.case2',
                mode,
            });
        });
    });
    describe('post-optimizations', () => {
        it('should work on the example query', async () => {
            const queryString = `
                SELECT
                    currentUser._id,
                    currentUser.establishment,
                    currentUser.access_all_child_establishments,
                    establishments.*,
                    child_establishments.*
                FROM
                    \`nfk-users|first\` currentUser
                    LEFT JOIN (
                        SELECT
                            user_establishment.user,
                            establishment._id,
                            establishment.name,
                            establishment.main_establishment,
                            establishment.level
                        FROM
                            \`nfk-user-establishments\` user_establishment
                            LEFT JOIN \`nfk-user-county-establishments|first\` establishment ON user_establishment.establishment = TO_STRING(establishment._id)
                        WHERE
                            user_establishment.user = '66261fd83316a727b53610da'
                        ORDER BY
                            establishment.level ASC,
                            establishment.name ASC
                    ) AS establishments ON establishments.user_establishment.user = TO_STRING(currentUser._id)
                    LEFT JOIN (
                        SELECT
                            TO_STRING(establishment._id) as id,
                            establishment.name,
                            establishment.main_establishment,
                            establishment.level
                        FROM
                            \`nfk-user-county-establishments\` establishment
                        WHERE
                            TO_STRING(establishment._id) = '662545865f01249a315ff1fd'
                            OR establishment.main_establishment = '662545865f01249a315ff1fd'
                        ORDER BY
                            establishment.level ASC,
                            establishment.name ASC
                    ) AS child_establishments ON child_establishments.establishment.id = currentUser.establishment
                    OR child_establishments.establishment.main_establishment = currentUser.establishment
                WHERE
                    TO_STRING(currentUser._id) = '66261fd83316a727b53610da'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'post-optimization.case1',
                mode,
                outputPipeline: false,
                unsetId: false,
            });
        });

        it('should work on the example query 2 with invalid field', async () => {
            const queryString = `
                SELECT
                    currentUser._id,
                    currentUser.establishment,
                    currentUser.access_all_child_establishments,
                    establishments.*,
                    child_establishments.*
                FROM
                    \`nfk-users|first\` currentUser
                    LEFT JOIN (
                        SELECT
                            user_establishment.user,
                            establishment._id,
                            establishment.name,
                            establishment.main_establishment,
                            establishment.level
                        FROM
                            \`nfk-user-establishments\` user_establishment
                            LEFT JOIN \`nfk-user-county-establishments|first\` establishment ON user_establishment.establishment = TO_STRING(establishment._id)
                        WHERE
                            user_establishment.user = '66261fd83316a727b53610da'
                        ORDER BY
                            establishment.level ASC,
                            establishment.xxx ASC
                    ) AS establishments ON establishments.user_establishment.user = TO_STRING(currentUser._id)
                    LEFT JOIN (
                        SELECT
                            TO_STRING(establishment._id) as id,
                            establishment.name,
                            establishment.main_establishment,
                            establishment.level
                        FROM
                            \`nfk-user-county-establishments\` establishment
                        WHERE
                            TO_STRING(establishment._id) = '662545865f01249a315ff1fd'
                            OR establishment.main_establishment = '662545865f01249a315ff1fd'
                        ORDER BY
                            establishment.level ASC,
                            establishment.xxx ASC
                    ) AS child_establishments ON child_establishments.establishment.id = currentUser.establishment
                    OR child_establishments.establishment.main_establishment = currentUser.establishment
                WHERE
                    TO_STRING(currentUser._id) = '66261fd83316a727b53610da'
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'post-optimization.case1',
                mode,
                outputPipeline: false,
                unsetId: false,
            });
        });
    });
    describe('empty-results', () => {
        it("should work with Avi's example", async () => {
            const queryString = `
                SELECT  bp.CustId,
                        bp.PolId,
                        bp.PolNo,
                        bp.PolEffDate,
                        bp.PolExpDate,
                        lob.LineOfBus
                FROM \`faizel-polinfo\` bp
                INNER JOIN (
                        SELECT  PolId,
                                LineOfBus
                                --,EffDate
                        FROM \`faizel-lob\`) \`lob|optimize\`
                        ON lob.PolId = bp.PolId
                                AND lob.EffDate >= bp.PolEffDate
                WHERE bp.Status != 'D'
                --AND lob.LineOfBus IN ('CGL','WORK','AUTOB', 'CUMBR','ELIAB', 'XLIB','INMRC', 'PROP', 'BOPGL', 'CFIRE', 'EMP LIAB OH', 'EPLI', 'MTRTK', 'PL', 'RFRBR', 'POLL' )
                AND TO_DATE(bp.PolExpDate) > CURRENT_DATE()
                AND bp.PolSubType != 'S'
                AND bp.CustId = 'test-customer-1'
                LIMIT 10 `;
            const aggregate = makeMongoAggregate(queryString);

            assert.ok(isEqual(aggregate.pipeline, emptyResultsBugPipeline));
        });
    });
    describe('deeply-nested-divide', () => {
        it('should work in the basic select', async () => {
            const queryString = `
                SELECT
                    disbursedAmount AS TotalFunded,
                    (2.5 * latestQuoteTerm) AS BrokerBuyRatePerTerm,
                    latestQuoteTotalFactor AS FactorAsPercent,
                    disbursedAmount * (((latestQuoteTotalFactor) - (2.5 * latestQuoteTerm)) / 100) AS CheckText,
                    ((disbursedAmount * (latestQuoteTotalFactor / 100)) - ((disbursedAmount * (0.13 / 12)) * (latestQuoteTerm / 2))) AS NetRevenue,
                    CASE
                        WHEN type != 'New Deal' THEN (2 / 100)
                        ELSE
                            CASE
                                WHEN latestQuoteTerm < 6 THEN (2.5 / 100)
                                WHEN latestQuoteTerm > 5 THEN (2.2 / 100)
                                ELSE 0.00
                            END
                    END AS BrokerBuyRate,
                    CASE
                        WHEN type = 'New Deal' AND latestQuoteTerm < 6 THEN (disbursedAmount * (((latestQuoteTotalFactor) - (2.5 * latestQuoteTerm)) / 100))
                        WHEN type = 'New Deal' AND latestQuoteTerm > 5 THEN (disbursedAmount * (((latestQuoteTotalFactor) - (2.2 * latestQuoteTerm)) / 100))
                        WHEN type != 'New Deal' THEN disbursedAmount * 0.020
                        ELSE 0
                    END AS BrokerBuyRateCommission,
                    YEAR(DATE_FROM_STRING(disbursedDate)) AS FundedYear,
                    MONTH(DATE_FROM_STRING(disbursedDate)) AS FundedMonth,
                    unset(_id)
                FROM
                "function-test-data"
                WHERE testId = "bugfix.deeply-nested-divide.case1"`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'deeply-nested-divide.case1',
                mode: 'write',
                outputPipeline: false,
            });
        });
    });
    describe('$and/$or/$nor must be a nonempty array', () => {
        it('should work for case 1', async () => {
            const queryString = `
                    SELECT T0_0."PolId" AS "p0_0",
                        T0_0."PolNo" AS "p1_0",
                        T1_0."CommAmt" AS "p2_0",
                        T1_0."CommPersType" AS "p3_0",
                        T0_0."InvDate" AS "p4_0",
                        T0_0."InvEffDate" AS "p5_0",
                        T2_0."GLDate" AS "p6_0",
                        CONCAT(T3_0.FIRSTNAME,' ',T3_0.LASTNAME) AS "p7_0",
                        CONCAT(T4_0.FIRSTNAME,' ',T4_0.LASTNAME) AS "p8_0"
                    FROM "public"."ams360-data-warehouse-dds-buffers--afwinvoice" T0_0
                    LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afwinvoicecommission" T1_0 ON T0_0."InvId" = T1_0."InvId"
                    LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afw_invoicetransaction" T2_0 ON T0_0."InvId" = T2_0."InvId"
                    LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afwemployee" T3_0 ON T0_0."RepCode" = T3_0."EmpCode"
                    LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afwemployee" T4_0 ON T0_0."ExecCode" = T4_0."EmpCode"
                    WHERE ((T0_0."InvDate" >= '2024-09-01 00:00:00.000'
                        AND T0_0."InvDate" <= '2024-09-30 23:59:59.999')
                        AND T1_0."CommAmt" != 0
                        AND (T1_0."CommPersType" = 'A' OR T1_0."CommPersType" = 'P'))
                    `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'and-or-nor.case-1',
                mode: 'write',
                outputPipeline: false,
                skipDbQuery: true,
                optimizeJoins: true,
                unsetId: true,
                schemas: {
                    'ams360-data-warehouse-dds-buffers--afwinvoice': {
                        type: 'object',
                        properties: {
                            _id: {
                                type: 'string',
                                format: 'mongoid',
                            },
                            ArClosedDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            ARClosedStatus: {
                                type: 'string',
                                stringLength: 1,
                            },
                            BhId: {
                                type: ['string', 'null'],
                                stringLength: 36,
                            },
                            BillMethod: {
                                type: ['string', 'null'],
                                stringLength: 1,
                            },
                            BinderPoltEffDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            BinderPostMethod: {
                                type: ['null', 'string'],
                                stringLength: 1,
                            },
                            BinderStatus: {
                                type: 'string',
                                stringLength: 1,
                            },
                            BrokerCode: {
                                type: 'null',
                            },
                            ChangedBy: {
                                type: 'string',
                                stringLength: 3,
                            },
                            ChangedDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            ClosedDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            ClosedStatus: {
                                type: 'string',
                                stringLength: 1,
                            },
                            CollectionId: {
                                type: 'null',
                            },
                            CshId: {
                                type: ['string', 'null'],
                                stringLength: 36,
                            },
                            CustId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            DbRecClosedDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            DueDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            EnteredDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            ExecCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            ExternalInvoiceId: {
                                type: 'null',
                            },
                            GlBrnchCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            GLDeptCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            GLDivCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            GlGrpCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            InvDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            InvEffDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            InvId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            InvNo: {
                                type: 'integer',
                            },
                            InvSeriesId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            InvType: {
                                type: 'integer',
                            },
                            IsCancelled: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsInstallment: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsPosted: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsPre35Data: {
                                type: 'string',
                                stringLength: 1,
                            },
                            JournalTranId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            LcDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            OADescription: {
                                type: ['null', 'string'],
                                stringLength: 23,
                            },
                            OAFinanceCode: {
                                type: 'null',
                            },
                            OaOrigin: {
                                type: ['integer', 'null'],
                            },
                            OrigDueDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            OriginalInvidInv: {
                                type: 'null',
                            },
                            PolId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            PolNo: {
                                type: 'string',
                                stringLength: 14,
                            },
                            PolRelation: {
                                type: 'string',
                                stringLength: 1,
                            },
                            RepCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            VoidInvidInv: {
                                type: 'null',
                            },
                            _dateUpdated: {
                                type: 'string',
                                format: 'date-time',
                            },
                        },
                    },
                    'ams360-data-warehouse-dds-buffers--afwinvoicecommission': {
                        type: 'object',
                        properties: {
                            _id: {type: 'string', format: 'mongoid'},
                            BhId: {type: 'string', stringLength: 36},
                            BillSeqId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            BillTranId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            ChangedBy: {
                                type: 'string',
                                stringLength: 3,
                            },
                            ChangedDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            CommAmt: {type: 'number'},
                            CommPersCode: {
                                type: ['string', 'null'],
                                stringLength: 3,
                            },
                            CommPersType: {
                                type: 'string',
                                stringLength: 1,
                            },
                            CommPremAmt: {type: 'number'},
                            CshId: {
                                type: ['string', 'null'],
                                stringLength: 36,
                            },
                            EnteredDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            InvCmId: {type: 'string', stringLength: 36},
                            InvCoId: {type: 'string', stringLength: 36},
                            InvId: {type: 'string', stringLength: 36},
                            InvTpId: {type: 'string', stringLength: 36},
                            IsActive: {type: 'string', stringLength: 1},
                            MemoFlag: {type: 'string', stringLength: 1},
                            Method: {type: 'string', stringLength: 1},
                            NetFlag: {type: 'string', stringLength: 1},
                            OverrideFlag: {
                                type: 'string',
                                stringLength: 1,
                            },
                            Percentage: {type: 'integer'},
                            PrimaryFlag: {
                                type: 'string',
                                stringLength: 1,
                            },
                            ProductionCreditAmount: {
                                type: ['null', 'integer'],
                            },
                            ProductionCreditOverrideFlag: {
                                type: 'null',
                            },
                            ProductionCreditSplitPercentage: {
                                type: ['null', 'integer'],
                            },
                            _dateUpdated: {
                                type: 'string',
                                format: 'date-time',
                            },
                        },
                    },
                    'ams360-data-warehouse-dds-buffers--afw_invoicetransaction':
                        {
                            type: 'object',
                            properties: {
                                _id: {type: 'string', format: 'mongoid'},
                                BackInvTPId: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                BHId: {type: 'string', stringLength: 36},
                                BillMethod: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                BillSeqId: {
                                    type: 'string',
                                    stringLength: 36,
                                },
                                BillTranId: {
                                    type: 'string',
                                    stringLength: 36,
                                },
                                BinderPolTEffDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                BinderPostMethod: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                BinderStatus: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                BrokerCode: {type: 'null'},
                                ChangedBy: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                ChangedDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                ChargeCat: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                ChargeCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                CoCode: {type: 'string', stringLength: 3},
                                CommPayType: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                CoType: {type: 'string', stringLength: 1},
                                CSHId: {type: 'null'},
                                CustId: {type: 'string', stringLength: 36},
                                Description: {
                                    type: 'string',
                                    stringLength: 45,
                                },
                                EnteredDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                FullTermPremAmt: {type: 'null'},
                                GLDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                GrossAmt: {type: 'number'},
                                InvEffDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                InvId: {type: 'string', stringLength: 36},
                                InvTPId: {type: 'string', stringLength: 36},
                                IsCancelled: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsInstallment: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsPosted: {type: 'string', stringLength: 1},
                                JournalTranId: {
                                    type: 'string',
                                    stringLength: 36,
                                },
                                LineOfBus: {
                                    type: 'string',
                                    stringLength: 5,
                                },
                                NewInvTPId: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                NonPrRecipient: {type: 'null'},
                                OldInvTPId: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                PlanType: {
                                    type: ['null', 'string'],
                                    stringLength: 5,
                                },
                                PolTEffDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                PolTPId: {
                                    type: ['string', 'null'],
                                    stringLength: 36,
                                },
                                RBBkOutId: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                ReplaceDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                TranType: {type: 'string', stringLength: 3},
                                WritingCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                _dateUpdated: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                            },
                        },
                    'ams360-data-warehouse-dds-buffers--afwemployee': {
                        type: 'object',
                        properties: {
                            _id: {
                                type: 'string',
                                format: 'mongoid',
                            },
                            Address1: {
                                type: ['null', 'string'],
                                stringLength: 18,
                            },
                            Address2: {
                                type: 'null',
                            },
                            BJEClosedStatus: {
                                type: 'string',
                                stringLength: 1,
                            },
                            BUAcsId: {
                                type: ['null', 'string'],
                                stringLength: 36,
                            },
                            BusAreaCode: {
                                type: ['null', 'string'],
                                stringLength: 3,
                            },
                            BusExt: {
                                type: 'null',
                            },
                            BusFullPhone: {
                                type: ['null', 'string'],
                                stringLength: 10,
                            },
                            BusPhone: {
                                type: ['null', 'string'],
                                stringLength: 7,
                            },
                            ChangedBy: {
                                type: 'string',
                                stringLength: 3,
                            },
                            ChangedDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            City: {
                                type: ['null', 'string'],
                                stringLength: 14,
                            },
                            ContactAreaCode: {
                                type: ['null', 'string'],
                                stringLength: 3,
                            },
                            ContactExt: {
                                type: 'null',
                            },
                            ContactFullPhone: {
                                type: ['null', 'string'],
                                stringLength: 10,
                            },
                            ContactPhone: {
                                type: ['null', 'string'],
                                stringLength: 7,
                            },
                            CountryCode: {
                                type: ['null', 'string'],
                            },
                            DefaultGLBrnchCode: {
                                type: 'null',
                            },
                            DefaultGLDeptCode: {
                                type: 'null',
                            },
                            DefaultGLDivCode: {
                                type: 'null',
                            },
                            DefaultGLGrpCode: {
                                type: 'null',
                            },
                            DOB: {
                                type: ['null', 'string'],
                                stringLength: 19,
                            },
                            Doc360HotFolderLoc: {
                                type: 'null',
                            },
                            Doc360Hotspot: {
                                type: 'string',
                                stringLength: 1,
                            },
                            Email: {
                                type: ['null', 'string'],
                                stringLength: 27,
                            },
                            EmergencyContact: {
                                type: ['null', 'string'],
                                stringLength: 11,
                            },
                            EmpCode: {
                                type: 'string',
                                stringLength: 3,
                            },
                            EmpId: {
                                type: 'string',
                                stringLength: 36,
                            },
                            EmployeeId: {
                                type: 'null',
                            },
                            EmpSupervisorCode: {
                                type: 'null',
                            },
                            EnteredDate: {
                                type: 'string',
                                format: 'date-time',
                            },
                            FaxAreaCode: {
                                type: ['null', 'string'],
                                stringLength: 3,
                            },
                            FaxExt: {
                                type: 'null',
                            },
                            FaxFullPhone: {
                                type: ['null', 'string'],
                                stringLength: 10,
                            },
                            FaxPhone: {
                                type: ['null', 'string'],
                                stringLength: 7,
                            },
                            FirstName: {
                                type: ['string', 'null'],
                                stringLength: 8,
                            },
                            FullPartTimeInd: {
                                type: ['null', 'string'],
                                stringLength: 1,
                            },
                            HomeAreaCode: {
                                type: ['null', 'string'],
                                stringLength: 3,
                            },
                            HomeExt: {
                                type: 'null',
                            },
                            HomeFullPhone: {
                                type: ['null', 'string'],
                                stringLength: 10,
                            },
                            HomePhone: {
                                type: ['null', 'string'],
                                stringLength: 7,
                            },
                            ImageId: {
                                type: 'null',
                            },
                            ImageType: {
                                type: ['integer', 'null'],
                            },
                            IsDefaultBuforCustomer: {
                                type: 'null',
                            },
                            IsDefaultBufOrPolicy: {
                                type: 'null',
                            },
                            IsForeign: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsLicensed: {
                                type: ['null', 'string'],
                                stringLength: 1,
                            },
                            IsLimitCustAccess: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsMemoCommissions: {
                                type: ['null', 'string'],
                                stringLength: 1,
                            },
                            IsOther: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsProd: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsRep: {
                                type: 'string',
                                stringLength: 1,
                            },
                            IsTelemarketer: {
                                type: 'string',
                                stringLength: 1,
                            },
                            LastName: {
                                type: 'string',
                                stringLength: 18,
                            },
                            LimitAmount: {
                                type: 'null',
                            },
                            LogSuspense: {
                                type: ['null', 'string'],
                                stringLength: 1,
                            },
                            MiddleName: {
                                type: ['null', 'string'],
                                stringLength: 1,
                            },
                            MobileAreaCode: {
                                type: 'null',
                            },
                            MobileExt: {
                                type: 'null',
                            },
                            MobileFullPhone: {
                                type: 'null',
                            },
                            MobilePhone: {
                                type: 'null',
                            },
                            NatlProdCode: {
                                type: 'null',
                            },
                            PagerAreaCode: {
                                type: 'null',
                            },
                            PagerExt: {
                                type: 'null',
                            },
                            PagerFullPhone: {
                                type: 'null',
                            },
                            PagerPhone: {
                                type: 'null',
                            },
                            S1099Category: {
                                type: ['null', 'integer'],
                            },
                            S1099Type: {
                                type: ['null', 'integer'],
                            },
                            ShortName: {
                                type: 'string',
                                stringLength: 6,
                            },
                            SSN: {
                                type: ['null', 'string'],
                                stringLength: 9,
                            },
                            State: {
                                type: ['null', 'string'],
                                stringLength: 2,
                            },
                            Status: {
                                type: 'string',
                                stringLength: 1,
                            },
                            Title: {
                                type: ['null', 'string'],
                                stringLength: 15,
                            },
                            TzCode: {
                                type: 'integer',
                            },
                            YearEmployed: {
                                type: ['null', 'string'],
                                stringLength: 4,
                            },
                            Zip: {
                                type: ['null', 'string'],
                                stringLength: 5,
                            },
                            _dateUpdated: {
                                type: 'string',
                                format: 'date-time',
                            },
                        },
                    },
                },
            });
            assert(
                isEqual(pipeline, [
                    {
                        $project: {
                            T0_0: '$$ROOT',
                        },
                    },
                    {
                        $match: {
                            $and: [
                                {
                                    $expr: {
                                        $and: [
                                            {
                                                $gte: [
                                                    {
                                                        $toDate:
                                                            '$T0_0.InvDate',
                                                    },
                                                    {
                                                        $toDate: {
                                                            $literal:
                                                                '2024-09-01T00:00:00.000Z',
                                                        },
                                                    },
                                                ],
                                            },
                                            {
                                                $ne: [
                                                    {
                                                        $type: '$T0_0.InvDate',
                                                    },
                                                    'null',
                                                ],
                                            },
                                            {
                                                $ne: [
                                                    {
                                                        $type: '$T0_0.InvDate',
                                                    },
                                                    'missing',
                                                ],
                                            },
                                        ],
                                    },
                                },
                                {
                                    $expr: {
                                        $and: [
                                            {
                                                $lte: [
                                                    {
                                                        $toDate:
                                                            '$T0_0.InvDate',
                                                    },
                                                    {
                                                        $toDate: {
                                                            $literal:
                                                                '2024-09-30T23:59:59.999Z',
                                                        },
                                                    },
                                                ],
                                            },
                                            {
                                                $ne: [
                                                    {
                                                        $type: '$T0_0.InvDate',
                                                    },
                                                    'null',
                                                ],
                                            },
                                            {
                                                $ne: [
                                                    {
                                                        $type: '$T0_0.InvDate',
                                                    },
                                                    'missing',
                                                ],
                                            },
                                        ],
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $lookup: {
                            from: 'ams360-data-warehouse-dds-buffers--afwinvoicecommission',
                            as: 'T1_0',
                            let: {
                                t_0_0_inv_id: '$T0_0.InvId',
                            },
                            pipeline: [
                                {
                                    $match: {
                                        $expr: {
                                            $or: [
                                                {
                                                    $eq: ['$CommPersType', 'A'],
                                                },
                                                {
                                                    $eq: ['$CommPersType', 'P'],
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $ne: ['$CommAmt', 0],
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$InvId', '$$t_0_0_inv_id'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $lookup: {
                            from: 'ams360-data-warehouse-dds-buffers--afw_invoicetransaction',
                            as: 'T2_0',
                            localField: 'T0_0.InvId',
                            foreignField: 'InvId',
                        },
                    },
                    {
                        $lookup: {
                            from: 'ams360-data-warehouse-dds-buffers--afwemployee',
                            as: 'T3_0',
                            localField: 'T0_0.RepCode',
                            foreignField: 'EmpCode',
                        },
                    },
                    {
                        $lookup: {
                            from: 'ams360-data-warehouse-dds-buffers--afwemployee',
                            as: 'T4_0',
                            localField: 'T0_0.ExecCode',
                            foreignField: 'EmpCode',
                        },
                    },
                    {
                        $project: {
                            p0_0: '$T0_0.PolId',
                            p1_0: '$T0_0.PolNo',
                            p2_0: '$T1_0.CommAmt',
                            p3_0: '$T1_0.CommPersType',
                            p4_0: '$T0_0.InvDate',
                            p5_0: '$T0_0.InvEffDate',
                            p6_0: '$T2_0.GLDate',
                            p7_0: {
                                $concat: [
                                    '$T3_0.FIRSTNAME',
                                    {
                                        $literal: ' ',
                                    },
                                    '$T3_0.LASTNAME',
                                ],
                            },
                            p8_0: {
                                $concat: [
                                    '$T4_0.FIRSTNAME',
                                    {
                                        $literal: ' ',
                                    },
                                    '$T4_0.LASTNAME',
                                ],
                            },
                        },
                    },
                    {
                        $unset: '_id',
                    },
                ])
            );
        });
    });
    describe('$nin', () => {
        it('should generate and execute a valid query', async () => {
            // noinspection SqlNoDataSourceInspection
            const queryString = `
                SELECT
                    id,
                    CASE
                        WHEN item NOT IN ('pecans','potatoes')
                        THEN 1
                        ELSE 0
                    END AS "Funded"
                FROM orders
                    `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'nin.case-1',
                mode: 'test',
                outputPipeline: false,
                skipDbQuery: false,
                optimizeJoins: false,
                unsetId: true,
                schemas: {},
            });
        });
        it('should generate and execute a valid query when complicated', async () => {
            // noinspection SqlNoDataSourceInspection
            const queryString = `
                SELECT
                    _id,
                    Id,
                    Account__c AS Account,
                    Account_Name AS AccountName,
                    Account_Account_Funding_Status AS AccountFundingStatus,
                    Account_Legal_Name__c AS AccountLegalName,
                    Account_Contract_Principle AS AccountContractPrinciple,
                    Application_Status__c AS ApplicationStatus,
                    Closed_By__c AS ClosedBy,
                    CASE
                        WHEN Source_Initiative != 'Automated Mining'
                            AND Product__c = 'merchantcashadvance'
                            AND Type__c = 'New Deal'
                            AND Sales_Executive_User != 'Pool of Hope'
                            AND Closed_By__c != 'System'
                            AND Account_Name IS NOT NULL
                            AND Account_Account_Funding_Status != 'Contactless'
                            AND Closed_Reason_New__c NOT IN ('Duplicate','Qualifying Criteria','Contact data wrong','poor contactability','unsupported industry','MC Balance too high','wants to become MC partner','poor MC payment efficiency')
                            AND Application_Status__c = 'Complete'
                            AND Closed_By__c != 'Underwriting'
                            AND Credit_Status__c = 'Approved'
                            AND Status__c = 'Active - Disbursed'
                        THEN 1
                        ELSE 0
                    END AS "Funded"
                FROM public."global-00--00--flat-offers-full-set"
                    `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'nin.case-2',
                mode: 'write',
                outputPipeline: true,
                skipDbQuery: true,
                optimizeJoins: false,
                unsetId: true,
                schemas: {},
            });
        });
    });
    describe('nested case statements', () => {
        it('should parse nested complex case statements correctly', async () => {
            const queryString = `
                    select  "_"."active" as "c52",
                            "_"."email" as "c55",
                            "_"."name" as "c59"
                        from
                        (
                            select "active",
                                "email",
                                "name",
                                "_"."t2_0" as "t2_0",
                                "_"."t3_0" as "t3_0"
                            from
                            (
                                select "_"."active",
                                    "_"."email",
                                    "_"."name",
                                    "_"."o2",
                                    "_"."t2_0",
                                    "_"."t3_0"
                                from
                                (
                                    select "_"."active" as "active",
                                        "_"."email" as "email",
                                        "_"."name" as "name",
                                        "_"."o2" as "o2",
                                        case
                                            when "_"."o2" is not null
                                            then (case
                                                when "_"."o2" is null
                                                then null
                                                when "_"."o2" = true
                                                then 1
                                                else 0
                                            end)
                                            else 0
                                        end as "t2_0",
                                        case
                                            when "_"."o2" is null
                                            then 0
                                            else 1
                                        end as "t3_0"
                                    from
                                    (
                                        select "rows"."active" as "active",
                                            "rows"."email" as "email",
                                            "rows"."name" as "name",
                                            "rows"."o2" as "o2"
                                        from
                                        (
                                            select "active" as "active",
                                                "email" as "email",
                                                "name" as "name",
                                                "active" as "o2"
                                            from "public"."Agencies" "$Table"
                                        ) "rows"
                                        group by "active",
                                            "email",
                                            "name",
                                            "o2"
                                    ) "_"
                                ) "_"
                            ) "_"
                        ) "_"
                        order by "_"."name",
                                "_"."email",
                                "_"."t2_0",
                                "_"."t3_0"
                        limit 501`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'nested-case.case-1',
                mode: 'write',
                outputPipeline: false,
                skipDbQuery: true,
                optimizeJoins: false,
                unsetId: false,
                schemas: {},
            });
        });
        it('should parse nested complex case statements correctly and get the right data', async () => {
            const queryString = `
                    select  "_"."id",
                            "_"."item",
                            "_"."category"
                        from
                        (
                            select
                                "_"."id",
                                "_"."item",
                                "_"."category"
                            from
                            (
                                select
                                    "_"."id",
                                    "_"."item",
                                    "_"."category"
                                from
                                (
                                    select
                                        id,
                                        item,
                                        case
                                            when item is not null
                                            then (
                                                case
                                                    when _.item = "almonds"
                                                        then "nut"
                                                    when _.item = "pecans"
                                                        then "nut"
                                                    when _.item = "potatoes"
                                                        then "starch"
                                                    else "uncategorized"
                                                end
                                            )
                                            else "Null"
                                        end as category
                                    from
                                    (
                                        select *
                                        from orders
                                    ) "_"
                                ) "_"
                            ) "_"
                        ) "_"
                        order by id
                        limit 501`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'nested-case.case-2',
                mode: 'write',
                outputPipeline: false,
                skipDbQuery: false,
                optimizeJoins: false,
                unsetId: true,
                schemas: {},
            });
        });
    });

    describe('invalid sort fields after project', () => {
        it('should handle a simple sort', async () => {
            const sql = `
                SELECT *
                FROM users
                ORDER BY name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $sort: {
                                name: -1,
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a complex project sort', async () => {
            const sql = `
                SELECT
                    LOWER(name) as full_name
                FROM users
                ORDER BY full_name,first_name`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                full_name: {
                                    $toLower: '$name',
                                },
                                first_name: 1,
                            },
                        },
                        {
                            $sort: {
                                full_name: 1,
                                first_name: 1,
                            },
                        },
                        {
                            $unset: ['first_name'],
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a complex project sort with alias', async () => {
            const sql = `
                SELECT
                    LOWER(u.name) as full_name
                FROM users u
                ORDER BY full_name,u.first_name`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                u: '$$ROOT',
                            },
                        },
                        {
                            $project: {
                                full_name: {
                                    $toLower: '$u.name',
                                },
                                'u.first_name': 1,
                            },
                        },
                        {
                            $sort: {
                                full_name: 1,
                                'u.first_name': 1,
                            },
                        },
                        {
                            $unset: ['u.first_name'],
                        },
                        {
                            $unset: ['u'],
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a complex project sort with alias no unset prefix', async () => {
            const sql = `
                SELECT
                    LOWER(u.name) as "u.full_name"
                FROM users u
                ORDER BY u.full_name,u.first_name`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                u: '$$ROOT',
                            },
                        },
                        {
                            $project: {
                                'u.full_name': {
                                    $toLower: '$u.name',
                                },
                                'u.first_name': 1,
                            },
                        },
                        {
                            $sort: {
                                'u.full_name': 1,
                                'u.first_name': 1,
                            },
                        },
                        {
                            $unset: ['u.first_name'],
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a simple sort with projection', async () => {
            const sql = `
                SELECT name, email
                FROM users
                ORDER BY name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                name: '$name',
                                email: '$email',
                            },
                        },
                        {
                            $sort: {
                                name: -1,
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a simple sort with projection and non selected column', async () => {
            const sql = `
                SELECT email
                FROM users
                ORDER BY name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $sort: {
                                name: -1,
                            },
                        },
                        {
                            $project: {
                                email: '$email',
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a simple sort with projection and root', async () => {
            const sql = `
                SELECT _.email
                FROM users _
                ORDER BY _.name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                _: '$$ROOT',
                            },
                        },
                        {
                            $sort: {
                                '_.name': -1,
                            },
                        },
                        {
                            $project: {
                                email: '$_.email',
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a simple sort with projection and root and alias', async () => {
            const sql = `
                SELECT _.email as t
                FROM users _
                ORDER BY _.name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                _: '$$ROOT',
                            },
                        },
                        {
                            $sort: {
                                '_.name': -1,
                            },
                        },
                        {
                            $project: {
                                t: '$_.email',
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a simple sort with projection and root unset', async () => {
            const sql = `
                SELECT _.email as "_.email"
                FROM users _
                ORDER BY _.name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                _: '$$ROOT',
                            },
                        },
                        {
                            $sort: {
                                '_.name': -1,
                            },
                        },
                        {
                            $project: {
                                '_.email': '$_.email',
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a simple sort with projection and *', async () => {
            const sql = `
                SELECT _.*
                FROM users _
                ORDER BY _.name desc`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                _: '$$ROOT',
                            },
                        },
                        {
                            $sort: {
                                '_.name': -1,
                            },
                        },
                        {
                            $project: {
                                _: '$_',
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should handle a function in sort', async () => {
            console.warn(
                'should handle a function in sort: ORDER BY LOWER(name)`'
            );
            // todo this needs to be catered for:

            // const sql = `
            //     SELECT
            //         name
            //     FROM test_names
            //     ORDER BY LOWER(name)`;
        });

        it('should sort after project with subquery', async () => {
            const sql = `
                select "_"."AgentFullName" as "c122",
                    "_"."CarrierName" as "c134",
                    "_"."DateCreated" as "c153",
                    "_"."HolderName" as "c179"
                from
                (
                    select "AgentFullName",
                    "CarrierName",
                    "DateCreated",
                    "HolderName"
                from "test_table") _

                order by "_"."AgentFullName",
                    "_"."HolderName",
                    "_"."CarrierName"`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                AgentFullName: '$AgentFullName',
                                CarrierName: '$CarrierName',
                                DateCreated: '$DateCreated',
                                HolderName: '$HolderName',
                            },
                        },
                        {
                            $project: {
                                _: '$$ROOT',
                            },
                        },
                        {
                            $sort: {
                                '_.AgentFullName': 1,
                                '_.HolderName': 1,
                                '_.CarrierName': 1,
                            },
                        },
                        {
                            $project: {
                                c122: '$_.AgentFullName',
                                c134: '$_.CarrierName',
                                c153: '$_.DateCreated',
                                c179: '$_.HolderName',
                            },
                        },
                    ],
                    collections: ['test_table'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should sort after project with subquery and sort', async () => {
            const sql = `
                select "_"."AgentFullName" as "c122",
                       "_"."CarrierName" as "c134",
                       "_"."DateCreated" as "c153",
                       "_"."HolderName" as "c179"
                from
                    (
                        select "AgentFullName",
                               "CarrierName",
                               "DateCreated",
                               "HolderName"
                        from "test_table" order by AgentFullName) _

                order by "_"."AgentFullName",
                         "_"."HolderName",
                         "_"."CarrierName"`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                AgentFullName: '$AgentFullName',
                                CarrierName: '$CarrierName',
                                DateCreated: '$DateCreated',
                                HolderName: '$HolderName',
                            },
                        },
                        {
                            $sort: {
                                AgentFullName: 1,
                            },
                        },
                        {
                            $project: {
                                _: '$$ROOT',
                            },
                        },
                        {
                            $sort: {
                                '_.AgentFullName': 1,
                                '_.HolderName': 1,
                                '_.CarrierName': 1,
                            },
                        },
                        {
                            $project: {
                                c122: '$_.AgentFullName',
                                c134: '$_.CarrierName',
                                c153: '$_.DateCreated',
                                c179: '$_.HolderName',
                            },
                        },
                    ],
                    collections: ['test_table'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should sort after project is correct with no alias', async () => {
            const sql = `
                select "AgentFullName" as "c122",
                    "CarrierName" as "c134",
                    "DateCreated" as "c153",
                    "HolderName" as "c179"

                from "test_table"

                order by "AgentFullName",
                    "HolderName",
                    "CarrierName"`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $sort: {
                                AgentFullName: 1,
                                HolderName: 1,
                                CarrierName: 1,
                            },
                        },
                        {
                            $project: {
                                c122: '$AgentFullName',
                                c134: '$CarrierName',
                                c153: '$DateCreated',
                                c179: '$HolderName',
                            },
                        },
                    ],
                    collections: ['test_table'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('test that sort after project is correct with alias and function no alias', async () => {
            const sql = `
                SELECT
                    LOWER(name) as full_name
                FROM users
                ORDER BY full_name`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                full_name: {
                                    $toLower: '$name',
                                },
                            },
                        },
                        {
                            $sort: {
                                full_name: 1,
                            },
                        },
                    ],
                    collections: ['users'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('test that sort with full outer joing', async () => {
            const sql = `
                SELECT c.customerName as customerName,
                       o.orderId as orderId,
                       unset(_id)
                FROM "foj-customers" c
                         FULL OUTER JOIN "foj-orders" o
                                         ON c.customerId = o.customerId
                ORDER BY c.customerName ASC, o.orderId ASC
            `;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                c: '$$ROOT',
                            },
                        },
                        {
                            $lookup: {
                                from: 'foj-orders',
                                as: 'o',
                                localField: 'c.customerId',
                                foreignField: 'customerId',
                            },
                        },
                        {
                            $unwind: {
                                path: '$o',
                                preserveNullAndEmptyArrays: true,
                            },
                        },
                        {
                            $unionWith: {
                                coll: 'foj-orders',
                                pipeline: [
                                    {
                                        $lookup: {
                                            from: 'foj-customers',
                                            localField: 'customerId',
                                            foreignField: 'customerId',
                                            as: 'c',
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $unwind: {
                                path: '$c',
                                preserveNullAndEmptyArrays: true,
                            },
                        },
                        {
                            $project: {
                                customerName: {
                                    $ifNull: [
                                        '$customerName',
                                        '$c.customerName',
                                    ],
                                },
                                orderId: {
                                    $ifNull: ['$orderId', '$o.orderId'],
                                },
                            },
                        },
                        {
                            $group: {
                                _id: {
                                    customerName: '$customerName',
                                    orderId: '$orderId',
                                },
                            },
                        },
                        {
                            $project: {
                                _id: 0,
                                customerName: '$_id.customerName',
                                orderId: '$_id.orderId',
                            },
                        },
                        {
                            $unset: ['_id'],
                        },
                        {
                            $sort: {
                                customerName: 1,
                                orderId: 1,
                            },
                        },
                    ],
                    collections: ['foj-customers', 'foj-orders'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('test that sort with having join', async () => {
            const sql = `
                SELECT  c.id,
                        sum(1) as cnt
                FROM customers c
                         INNER JOIN \`customer-notes\` cn on c.id=cn.id
                WHERE cn.id>1 and c.id>2
                GROUP BY c.id
                HAVING cnt >0
                ORDER BY c.id ASC
            `;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $project: {
                                c: '$$ROOT',
                            },
                        },
                        {
                            $lookup: {
                                from: 'customer-notes',
                                as: 'cn',
                                localField: 'c.id',
                                foreignField: 'id',
                            },
                        },
                        {
                            $match: {
                                $expr: {
                                    $gt: [
                                        {
                                            $size: '$cn',
                                        },
                                        0,
                                    ],
                                },
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'cn.id': {
                                            $gt: 1,
                                        },
                                    },
                                    {
                                        'c.id': {
                                            $gt: 2,
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $group: {
                                _id: {
                                    id: '$c.id',
                                },
                                cnt: {
                                    $sum: 1,
                                },
                            },
                        },
                        {
                            $project: {
                                id: '$_id.id',
                                _id: 0,
                                cnt: '$cnt',
                            },
                        },
                        {
                            $match: {
                                cnt: {
                                    $gt: 0,
                                },
                            },
                        },
                        {
                            $sort: {
                                id: 1,
                            },
                        },
                    ],
                    collections: ['customers', 'customer-notes'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('test that sort after project is correct with no alias', async () => {
            const sql = `
                select "AgentFullName" as "c122",
                    "CarrierName" as "c134",
                    "DateCreated" as "c153",
                    "HolderName" as "c179"

                from "test_table"

                order by "AgentFullName",
                    "HolderName",
                    "CarrierName"`;

            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $sort: {
                                AgentFullName: 1,
                                HolderName: 1,
                                CarrierName: 1,
                            },
                        },
                        {
                            $project: {
                                c122: '$AgentFullName',
                                c134: '$CarrierName',
                                c153: '$DateCreated',
                                c179: '$HolderName',
                            },
                        },
                    ],
                    collections: ['test_table'],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });

        it('should successfully call findLastIndex if pipeline is empty', async () => {
            const sql = `
                select records as \`$$ROOT\`
from (select    RecordId,
                AncestorInstanceId,
                AncestorRecordId,
                AncestorRecordNumber,
                ChecklistStatusId,
                CurrentDv,
                InsertDv,
                InstanceId,
                IsInUseByOtherRecords,
                ModuleId,
                ProcessFlowId,
                RecordNumber,
                RecordStatus,
                SQ,
                CreatedByUserId,
                LatestModifiedByUserId,
                DeletedByUserId
    from \`global-list-module-records--vbfr-std-glb-module-record\`
    where RecordStatus in ('active')
    and InstanceId in ('19F881AA-94BA-4F9A-8E04-C37B172AF652' )
    ) as records
where 1=1
order by CurrentDv asc , SQ asc`;
            await queryResultTester({
                queryString: sql,
                casePath: 'nested-case.case-2',
                mode: 'write',
                outputPipeline: false,
                skipDbQuery: true,
                optimizeJoins: false,
                unsetId: true,
                schemas: {
                    // 'global-list-module-records--vbfr-std-glb-module-record': {
                    //     type: 'object',
                    //     properties: {
                    //         _id: {
                    //             type: 'string',
                    //             format: 'mongoid',
                    //         },
                    //         RecordId: {
                    //             type: 'string',
                    //             stringLength: 36,
                    //         },
                    //         AncestorInstanceId: {
                    //             type: 'null',
                    //         },
                    //         AncestorRecordId: {
                    //             type: 'null',
                    //         },
                    //         AncestorRecordNumber: {
                    //             type: 'null',
                    //         },
                    //         AuditSQ: {
                    //             type: 'null',
                    //         },
                    //         ChecklistStatusId: {
                    //             type: ['null', 'string'],
                    //             stringLength: 36,
                    //         },
                    //         CreatedByUserId: {
                    //             type: 'string',
                    //             stringLength: 36,
                    //         },
                    //         CurrentDv: {
                    //             type: 'string',
                    //             format: 'date-time',
                    //         },
                    //         DeletedByUserId: {
                    //             type: 'null',
                    //         },
                    //         InsertDv: {
                    //             type: 'string',
                    //             format: 'date-time',
                    //         },
                    //         InstanceId: {
                    //             type: 'string',
                    //             stringLength: 36,
                    //         },
                    //         IsInUseByOtherRecords: {
                    //             type: 'integer',
                    //         },
                    //         LatestModifiedByUserId: {
                    //             type: 'string',
                    //             stringLength: 36,
                    //         },
                    //         ModuleId: {
                    //             type: 'string',
                    //             stringLength: 36,
                    //         },
                    //         ProcessFlowId: {
                    //             type: 'string',
                    //             stringLength: 36,
                    //         },
                    //         RecordNumber: {
                    //             type: 'integer',
                    //         },
                    //         RecordStatus: {
                    //             type: 'string',
                    //             stringLength: 6,
                    //         },
                    //         SQ: {
                    //             type: 'integer',
                    //         },
                    //         _dateUpdated: {
                    //             type: 'string',
                    //             format: 'date-time',
                    //         },
                    //     },
                    // },
                },
            });
            const aggr = makeMongoAggregate(sql);
            assert.deepStrictEqual(
                aggr,
                {
                    pipeline: [
                        {
                            $match: {
                                $and: [
                                    {
                                        RecordStatus: {
                                            $in: ['active'],
                                        },
                                    },
                                    {
                                        InstanceId: {
                                            $in: [
                                                '19F881AA-94BA-4F9A-8E04-C37B172AF652',
                                            ],
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $project: {
                                RecordId: '$RecordId',
                                AncestorInstanceId: '$AncestorInstanceId',
                                AncestorRecordId: '$AncestorRecordId',
                                AncestorRecordNumber: '$AncestorRecordNumber',
                                ChecklistStatusId: '$ChecklistStatusId',
                                CurrentDv: '$CurrentDv',
                                InsertDv: '$InsertDv',
                                InstanceId: '$InstanceId',
                                IsInUseByOtherRecords: '$IsInUseByOtherRecords',
                                ModuleId: '$ModuleId',
                                ProcessFlowId: '$ProcessFlowId',
                                RecordNumber: '$RecordNumber',
                                RecordStatus: '$RecordStatus',
                                SQ: '$SQ',
                                CreatedByUserId: '$CreatedByUserId',
                                LatestModifiedByUserId:
                                    '$LatestModifiedByUserId',
                                DeletedByUserId: '$DeletedByUserId',
                            },
                        },
                        {
                            $match: {
                                $expr: {
                                    $eq: [1, 1],
                                },
                            },
                        },
                        {
                            $replaceRoot: {
                                newRoot: '$records',
                            },
                        },
                        {
                            $project: {
                                records: '$$ROOT',
                                CurrentDv: 1,
                                SQ: 1,
                            },
                        },
                        {
                            $sort: {
                                CurrentDv: 1,
                                SQ: 1,
                            },
                        },
                        {
                            $unset: ['CurrentDv', 'SQ'],
                        },
                    ],
                    collections: [
                        'global-list-module-records--vbfr-std-glb-module-record',
                    ],
                    type: 'aggregate',
                },
                'Invalid sort order'
            );
        });
    });
});
