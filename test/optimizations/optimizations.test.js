const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');
const assert = require('assert');
const {isEqual} = require('lodash');
describe('bug-fixes', function () {
    this.timeout(90000);
    const fileName = 'optimizations';
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
    describe('where statements on joins', () => {
        /**
         * TODO Test for:
         * [] or statement instead of and
         * [] where contains table
         * [] subqueries
         * [] 3 queries?
         */
        it('should optimise the pipeline for a where statement after the join', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.price >= 0
                        AND i.instock >= 0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-1',
                mode,
            });
            // eslint-disable-next-line no-unused-vars
            const [_rootProject, match, lookup] = pipeline;
            assert(
                isEqual(match, {
                    $match: {
                        'o.price': {
                            $gte: 0,
                        },
                    },
                })
            );
            assert(
                isEqual(lookup, {
                    $lookup: {
                        from: 'inventory',
                        as: 'i',
                        let: {
                            o_item: '$o.item',
                        },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                            {
                                                $eq: ['$sku', '$$o_item'],
                                            },
                                            {
                                                $gte: ['$instock', 0],
                                            },
                                        ],
                                    },
                                },
                            },
                        ],
                    },
                })
            );
        });
        it('should optimise the pipeline for a where statement after the join with an additional on condition', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        AND i.id > 0
                        WHERE o.price >= 0
                        AND i.instock >= 0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-2',
                mode,
            });
            // eslint-disable-next-line no-unused-vars
            const [_rootProject, match, lookup] = pipeline;
            assert(
                isEqual(match, {
                    $match: {
                        'o.price': {
                            $gte: 0,
                        },
                    },
                })
            );
            assert(
                isEqual(lookup, {
                    $lookup: {
                        from: 'inventory',
                        as: 'i',
                        let: {
                            o_item: '$o.item',
                        },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                            {
                                                $eq: ['$sku', '$$o_item'],
                                            },
                                            {
                                                $gt: ['$id', 0],
                                            },
                                            {
                                                $gte: ['$instock', 0],
                                            },
                                        ],
                                    },
                                },
                            },
                        ],
                    },
                })
            );
        });
        it('should optimise the pipeline for a where statement after the join with an additional on condition that is an or', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        OR i.id > 0
                        WHERE o.price >= 0
                        AND i.instock >= 0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-3',
                mode,
            });
            // eslint-disable-next-line no-unused-vars
            const [_rootProject, match, lookup] = pipeline;
            assert(
                isEqual(match, {
                    $match: {
                        'o.price': {
                            $gte: 0,
                        },
                    },
                })
            );
            assert(
                isEqual(lookup, {
                    $lookup: {
                        from: 'inventory',
                        as: 'i',
                        let: {
                            o_item: '$o.item',
                        },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $or: [
                                            {
                                                $eq: ['$sku', '$$o_item'],
                                            },
                                            {
                                                $gt: ['$id', 0],
                                            },
                                        ],
                                    },
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $gte: ['$instock', 0],
                                    },
                                },
                            },
                        ],
                    },
                })
            );
        });
        it('should optimise the pipeline for a where statement after the join with or', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.price >= 0
                        OR i.instock >= 0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-1',
                mode,
            });
            // eslint-disable-next-line no-unused-vars
            const [_rootProject, match, lookup] = pipeline;
            assert(
                isEqual(match, {
                    $match: {
                        'o.price': {
                            $gte: 0,
                        },
                    },
                })
            );
            assert(
                isEqual(lookup, {
                    $lookup: {
                        from: 'inventory',
                        as: 'i',
                        let: {
                            o_item: '$o.item',
                        },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $and: [
                                            {
                                                $eq: ['$sku', '$$o_item'],
                                            },
                                            {
                                                $gte: ['$instock', 0],
                                            },
                                        ],
                                    },
                                },
                            },
                        ],
                    },
                })
            );
        });
    });
});
