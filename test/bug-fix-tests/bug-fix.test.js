const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');

describe('bug-fixes', function () {
    this.timeout(90000);
    const fileName = 'bug-fix';
    /** @type {'test'|'write'} */
    const mode = 'test';
    const dirName = __dirname;
    /** @type {import('../utils/query-tester/types.js').QueryResultTester} */
    let queryResultTester;
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    before(function (done) {
        const run = async () => {
            try {
                const {client} = await setup();
                mongoClient = client;
                queryResultTester = buildQueryResultTester({
                    dirName,
                    fileName,
                    mongoClient,
                    mode,
                });
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
});
