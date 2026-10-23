const assert = require('node:assert');
const {setup, disconnect} = require('../utils/mongo-client.js');
const SQLParser = require('../../lib/SQLParser');

const collectionName = 'cast-array-no-items';

describe('cast array to varchar when schema has no items', function () {
    this.timeout(90000);
    /** @type {import("mongodb").Db} */
    let database;

    before(async function () {
        const {db} = await setup();
        database = db;
        await database.collection(collectionName).deleteMany({});
        await database.collection(collectionName).insertMany([
            {id: 1, arr: []},
            {id: 2, arr: [1, 'a', true]},
            {id: 3, arr: []},
        ]);
    });

    after(async function () {
        await database.collection(collectionName).drop();
        await disconnect();
    });

    /**
     * Runs a cast query with the given schema for `arr`
     * @param {object} arrSchema the JSON schema for the arr property
     * @returns {Promise<object[]>} the results
     */
    async function runCast(arrSchema) {
        const options = {
            schemas: {
                [collectionName]: {
                    type: 'object',
                    properties: {id: {type: 'integer'}, arr: arrSchema},
                },
            },
        };
        const sql = `SELECT id, cast(arr as varchar) as arr FROM "${collectionName}" ORDER BY id`;
        const {pipeline, collections} = SQLParser.makeMongoAggregate(
            sql,
            options
        );
        return database
            .collection(collections[0])
            .aggregate(pipeline)
            .toArray();
    }

    it('should not throw and should render empty arrays as [] when items is missing', async () => {
        const results = await runCast({type: 'array'});
        assert.deepStrictEqual(
            results.map((r) => r.arr),
            ['[]', '[1,"a",true]', '[]']
        );
    });

    it('should treat an empty items schema the same as missing items', async () => {
        const results = await runCast({type: 'array', items: {}});
        assert.deepStrictEqual(
            results.map((r) => r.arr),
            ['[]', '[1,"a",true]', '[]']
        );
    });

    it('should still use typed items when present', async () => {
        const results = await runCast({
            type: 'array',
            items: {type: ['integer', 'string', 'boolean']},
        });
        assert.deepStrictEqual(
            results.map((r) => r.arr),
            ['[]', '[1,"a",true]', '[]']
        );
    });

    describe('dates and null elements', function () {
        const datesCollection = 'cast-array-dates-nulls';
        before(async function () {
            await database.collection(datesCollection).deleteMany({});
            await database.collection(datesCollection).insertMany([
                {
                    id: 1,
                    arr: [
                        new Date('2025-01-01T00:00:00.000Z'),
                        new Date('2025-06-30T12:00:00.000Z'),
                    ],
                },
                {id: 2, arr: [7, null, 9]},
                {id: 3, arr: ['a', null]},
            ]);
        });
        after(async function () {
            await database.collection(datesCollection).drop();
        });

        /**
         * Casts arr to varchar for the given row using the given items schema
         * @param {object} arrSchema the JSON schema for arr
         * @returns {Promise<string[]>} the casted values ordered by id
         */
        async function castAll(arrSchema) {
            const options = {
                schemas: {
                    [datesCollection]: {
                        type: 'object',
                        properties: {id: {type: 'integer'}, arr: arrSchema},
                    },
                },
            };
            const sql = `SELECT id, cast(arr as varchar) as arr FROM "${datesCollection}" ORDER BY id`;
            const {pipeline, collections} = SQLParser.makeMongoAggregate(
                sql,
                options
            );
            const results = await database
                .collection(collections[0])
                .aggregate(pipeline)
                .toArray();
            return results.map((r) => r.arr);
        }

        it('should render date elements as quoted ISO strings for date-time items', async () => {
            const [dates] = await castAll({
                type: 'array',
                items: {type: 'string', format: 'date-time'},
            });
            assert.strictEqual(
                dates,
                '["2025-01-01T00:00:00.000Z","2025-06-30T12:00:00.000Z"]'
            );
        });

        it('should render date elements when items is missing', async () => {
            const [dates] = await castAll({type: 'array'});
            assert.strictEqual(
                dates,
                '["2025-01-01T00:00:00.000Z","2025-06-30T12:00:00.000Z"]'
            );
        });

        it('should render null elements as null for number items', async () => {
            const [, numbers] = await castAll({
                type: 'array',
                items: {type: 'integer'},
            });
            assert.strictEqual(numbers, '[7,null,9]');
        });

        it('should render null elements as null for string items', async () => {
            const results = await castAll({
                type: 'array',
                items: {type: ['string', 'null']},
            });
            assert.strictEqual(results[2], '["a",null]');
        });
    });
});
