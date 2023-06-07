const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');
const _dbName = 'sql-to-mongo-test';
const {setup, disconnect} = require('../mongo-client');

describe('Joins', function () {
    this.timeout(90000);
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    before(function (done) {
        const run = async () => {
            try {
                const {client} = await setup();
                mongoClient = client;
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

    it('should be able to join from A to B', async (done) => {
        const queryText =
            'SELECT id, (select * from Rentals order by `Rental Date` desc) AS OrderedRentals FROM `customers`';
        const parsedQuery = SQLParser.makeMongoAggregate(queryText);
        try {
            const results = await mongoClient
                .db(_dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
            done();
        } catch (err) {
            return done(err);
        }
    });
});
