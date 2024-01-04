const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');

describe('bug-fixes', function () {
    this.timeout(90000);
    /** @type {'test'|'write'} */
    const mode = 'write';
    const fileName = __filename.replace('.test.js', '');
    const dirName = __dirname;
    /** @type {import("../utils/query-tester/types.js").QueryResultTester} */
    let queryResultTester;
    /** @type {import("mongodb").MongoClient} */
    let mongoClient;
    before(async function () {
        const {client} = await setup();
        mongoClient = client;
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
        it.skip('should allow you to get the interception of two queries', async () => {
            const queryString = `
                SELECT  item
                FROM public.orders
                WHERE ID = 1

                intersect

                SELECT "sku"
                FROM public.inventory
                WHERE ID = 1
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'intercept.case1',
                mode,
                outputPipeline: false,
            });
        });
    });
});
