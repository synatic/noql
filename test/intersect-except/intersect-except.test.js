const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');
const {getAllSchemas} = require('../utils/get-all-schemas.js');

describe('intersect & except', function () {
    this.timeout(90000);
    /** @type {'test'|'write'} */
    const mode = 'test';
    const fileName = __filename.replace('.test.js', '');
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
    describe('intercept', () => {
        it('should allow you to get the INTERSECT of two queries from different tables', async () => {
            const queryString = `
                SELECT  item,
                        unset(_id)
                FROM orders
                WHERE id = 1

                intersect

                SELECT  sku,
                        unset(_id)
                FROM inventory
                WHERE id = 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'intercept.case1',
                mode,
            });
        });
        it('should allow you to get the INTERSECT of two queries from the same table', async () => {
            const queryString = `
                SELECT  item,
                        unset(_id)
                FROM orders
                WHERE id = 1

                intersect

                SELECT  item,
                        unset(_id)
                FROM orders
                WHERE id = 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'intercept.case2',
                mode,
            });
        });
        it('should allow you to get the INTERSECT of two queries from the same table for a number', async () => {
            const queryString = `
                SELECT  id,
                        unset(_id)
                FROM orders
                WHERE id = 1

                intersect

                SELECT  id,
                        unset(_id)
                FROM orders
                WHERE id = 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'intercept.case3',
                mode,
            });
        });
        it('should allow you to get the INTERSECT of two queries from different tables for a * with string and number values', async () => {
            const queryString = `
            SELECT  *,unset(_id)
            FROM "most-popular-films"

            INTERSECT

            SELECT  *,unset(_id)
            FROM "top-rated-films"
            ORDER BY name
        `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'intercept.case4',
                mode,
                schemas: await getAllSchemas(database),
            });
        });
    });
    describe('except', () => {
        it('should allow you to get the EXCEPT of two queries from different tables for a * with string and number values and order by', async () => {
            const queryString = `
                SELECT  *,unset(_id)
                FROM "top-rated-films"
                WHERE 1=1

                EXCEPT

                SELECT  *,unset(_id)
                FROM "most-popular-films"
                ORDER BY name
`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'except.case1',
                mode,
                schemas: await getAllSchemas(database),
            });
        });
        it.skip('should allow you to get the EXCEPT of two queries from different tables for a * with string and number values and order by without a where clause', async () => {
            const queryString = `
                SELECT *,unset(_id)
                FROM "top-rated-films"
                EXCEPT
                SELECT *,unset(_id)
                FROM "most-popular-films"
                ORDER BY name
`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'except.case2',
                mode,
                schemas: await getAllSchemas(database),
            });
        });
    });
});
