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
                mode: 'write',
            });
        });
    });
});
