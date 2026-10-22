const assert = require('node:assert');
const {setup, disconnect} = require('../utils/mongo-client.js');
const SQLParser = require('../../lib/SQLParser');

/**
 * Returns the $limit/$skip stages of a pipeline in order
 * @param {object[]} pipeline the pipeline
 * @returns {object[]} the limit and skip stages
 */
function limitAndSkipStages(pipeline) {
    return pipeline.filter((s) => '$limit' in s || '$skip' in s);
}

describe('limit & offset', function () {
    this.timeout(90000);
    /** @type {import("mongodb").Db} */
    let database;
    /** @type {number[]} */
    let allIds;

    before(async function () {
        const {db} = await setup();
        database = db;
        const docs = await database
            .collection('customers')
            .find({}, {projection: {id: 1}})
            .sort({id: 1})
            .toArray();
        allIds = docs.map((d) => d.id);
    });

    after(function (done) {
        disconnect().then(done).catch(done);
    });

    /**
     * Runs a SQL query as an aggregate and returns the ids
     * @param {string} sql the query
     * @returns {Promise<number[]>} the ids
     */
    async function runAggregate(sql) {
        const {pipeline, collections} = SQLParser.makeMongoAggregate(sql);
        const results = await database
            .collection(collections[0])
            .aggregate(pipeline)
            .toArray();
        return results.map((r) => r.id);
    }

    /**
     * Runs a SQL query as a find and returns the ids
     * @param {string} sql the query
     * @returns {Promise<number[]>} the ids
     */
    async function runFind(sql) {
        const parsed = SQLParser.makeMongoQuery(sql);
        let cursor = database
            .collection(parsed.collection)
            .find(parsed.query || {}, {projection: parsed.projection});
        if (parsed.sort) {
            cursor = cursor.sort(parsed.sort);
        }
        if (parsed.skip) {
            cursor = cursor.skip(parsed.skip);
        }
        if (parsed.limit) {
            cursor = cursor.limit(parsed.limit);
        }
        const results = await cursor.toArray();
        return results.map((r) => r.id);
    }

    const pagingCases = [
        {name: 'offset < limit', limit: 3, offset: 1},
        {name: 'offset == limit', limit: 2, offset: 2},
        {name: 'offset > limit', limit: 1, offset: 5},
        {name: 'LIMIT 1 OFFSET 1', limit: 1, offset: 1},
        {name: 'later page', limit: 10, offset: 20},
    ];

    describe('aggregate pipeline', () => {
        it('should put $skip before $limit', () => {
            const {pipeline} = SQLParser.makeMongoAggregate(
                'select * from customers order by id limit 3 offset 1'
            );
            assert.deepStrictEqual(pipeline, [
                {$sort: {id: 1}},
                {$skip: 1},
                {$limit: 3},
            ]);
        });

        for (const c of pagingCases) {
            it(`should return the right page for ${c.name} (LIMIT ${c.limit} OFFSET ${c.offset})`, async () => {
                const ids = await runAggregate(
                    `select id from customers order by id limit ${c.limit} offset ${c.offset}`
                );
                assert.deepStrictEqual(
                    ids,
                    allIds.slice(c.offset, c.offset + c.limit)
                );
            });
        }

        it('should apply OFFSET without LIMIT as a skip, not a limit', async () => {
            const {pipeline} = SQLParser.makeMongoAggregate(
                'select * from customers order by id offset 2'
            );
            assert.deepStrictEqual(limitAndSkipStages(pipeline), [{$skip: 2}]);
            const ids = await runAggregate(
                'select id from customers order by id offset 2'
            );
            assert.deepStrictEqual(ids, allIds.slice(2));
        });

        it('should ignore OFFSET 0', () => {
            const {pipeline} = SQLParser.makeMongoAggregate(
                'select * from customers limit 5 offset 0'
            );
            assert.deepStrictEqual(limitAndSkipStages(pipeline), [{$limit: 5}]);
        });

        it('should keep LIMIT without OFFSET unchanged', () => {
            const {pipeline} = SQLParser.makeMongoAggregate(
                'select * from customers limit 5'
            );
            assert.deepStrictEqual(limitAndSkipStages(pipeline), [{$limit: 5}]);
        });

        it('should put $skip before $limit with GROUP BY', () => {
            const {pipeline} = SQLParser.makeMongoAggregate(
                'select id, count(*) as n from customers group by id order by id limit 2 offset 1'
            );
            assert.deepStrictEqual(limitAndSkipStages(pipeline), [
                {$skip: 1},
                {$limit: 2},
            ]);
        });

        it('should put $skip before $limit with a JOIN', () => {
            const {pipeline} = SQLParser.makeMongoAggregate(
                'select c.id from customers c inner join orders o on c.id = o.customerId limit 2 offset 1'
            );
            assert.deepStrictEqual(limitAndSkipStages(pipeline), [
                {$skip: 1},
                {$limit: 2},
            ]);
        });

        it('should put $skip before $limit in a FROM subquery', async () => {
            const sql =
                'select t.id from (select id from customers order by id limit 3 offset 1) t';
            const {pipeline} = SQLParser.makeMongoAggregate(sql);
            assert.deepStrictEqual(limitAndSkipStages(pipeline), [
                {$skip: 1},
                {$limit: 3},
            ]);
        });
    });

    describe('find query', () => {
        for (const c of pagingCases) {
            it(`should return the right page for ${c.name} (LIMIT ${c.limit} OFFSET ${c.offset})`, async () => {
                const ids = await runFind(
                    `select id from customers order by id limit ${c.limit} offset ${c.offset}`
                );
                assert.deepStrictEqual(
                    ids,
                    allIds.slice(c.offset, c.offset + c.limit)
                );
            });
        }

        it('should apply OFFSET without LIMIT as a skip, not a limit', () => {
            const parsed = SQLParser.makeMongoQuery(
                'select * from customers offset 2'
            );
            assert.strictEqual(parsed.skip, 2);
            assert.notStrictEqual(parsed.limit, 2);
        });
    });
});
