const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');

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

    async function getAllSchemas() {
        /** @type {import("../../lib/types").Schemas} */
        const result = {};
        const collections = await database.collections();
        const collectionNames = collections
            .map((c) => c.collectionName)
            .filter((c) => c !== 'schemas');
        for (const collectionName of collectionNames) {
            const searchResult = await database
                .collection('schemas')
                .findOne({collectionName}, {projection: {_id: 0, schema: 1}});
            result[collectionName] = searchResult.schema;
        }

        return result;
    }

    // /**
    //  *
    //  * @param  {...string} collectionNames
    //  * @returns
    //  */
    // async function getSchemas(...collectionNames) {
    //     /** @type {import('../../lib/types').FlattenedSchemas} */
    //     const result = {};
    //     for (const collectionName of collectionNames) {
    //         const searchResult = await database
    //             .collection('schemas')
    //             .findOne(
    //                 {collectionName},
    //                 {projection: {_id: 0, flattenedSchema: 1}}
    //             );
    //         result[collectionName] = searchResult.flattenedSchema;
    //     }

    //     return result;
    // }

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
    describe('currentDate', async () => {
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
    // describe('ntile', () => {
    //     it('Should correctly group the results', async () => {
    //         const queryString = `
    //             SELECT  name,
    //                     amount,
    //                     NTILE (3) OVER (
    //                         ORDER BY amount
    //                     ) ntile,
    //                     unset(_id)
    //             FROM function-test-data
    //             WHERE testId='bugfix.ntile.case1'
    //         `;
    //         await queryResultTester({
    //             queryString: queryString,
    //             casePath: 'bugfix.row-number.case1',
    //             mode: 'write',
    //         });
    //     });
    // });
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
                schemas: await getAllSchemas(),
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
                schemas: await getAllSchemas(),
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
                schemas: await getAllSchemas(),
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
                LIMIT 1
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
                mode: 'write',
            });
        });
    });
    describe('Current_Date', () => {
        it('should allow you to compare dates', async () => {
            const queryString = `
                SELECT  orderDate,
                        unset(_id)
                FROM orders
                WHERE orderDate < CURRENT_DATE()
                LIMIT 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.current_date.case1',
                mode,
                ignoreDateValues: true,
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
});
