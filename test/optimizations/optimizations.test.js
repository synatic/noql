const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');
const assert = require('assert');
const {isEqual} = require('lodash');
describe('optimizations', function () {
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
         * [] 3+ queries
         * [] support for not
         * [] multiple and/ ors with brackets
         * source & destination | and vs or |
         * Maybe clone the where and pipeline before optimising, return errors:true and revert?
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
                                        $gte: ['$instock', 0],
                                    },
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $eq: ['$sku', '$$o_item'],
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
                                        $gte: ['$instock', 0],
                                    },
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $gt: ['$id', 0],
                                    },
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $eq: ['$sku', '$$o_item'],
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
                                        $gte: ['$instock', 0],
                                    },
                                },
                            },
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
                casePath: 'where.case-4',
                mode: 'write',
                outputPipeline: true,
            });
            // eslint-disable-next-line no-unused-vars
            const lookup = pipeline.find((p) => !!p.$lookup);
            assert(
                isEqual(lookup, {
                    $lookup: {
                        from: 'inventory',
                        as: 'i',
                        let: {
                            o_item: '$o.item',
                            o_price: '$o.price',
                        },
                        pipeline: [
                            {
                                $match: {
                                    $expr: {
                                        $or: [
                                            {
                                                $gte: ['$instock', 0],
                                            },
                                            {
                                                $gte: ['$$o_price', 0],
                                            },
                                        ],
                                    },
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $eq: ['$sku', '$$o_item'],
                                    },
                                },
                            },
                        ],
                    },
                })
            );
        });
        it('should optimise the pipeline for a where statement after the join with or both same table', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR i.instock >= 0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-5',
                mode: 'write',
                outputPipeline: true,
            });
            assert.equal(pipeline.length, 5);
            // eslint-disable-next-line no-unused-vars
            const [_project, lookup, _innerJoinSizeCheck, _unset, _limit] =
                pipeline;
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
                                                $gte: ['$id', 0],
                                            },
                                            {
                                                $gte: ['$instock', 0],
                                            },
                                        ],
                                    },
                                },
                            },
                            {
                                $match: {
                                    $expr: {
                                        $eq: ['$sku', '$$o_item'],
                                    },
                                },
                            },
                        ],
                    },
                })
            );
        });
        it('should optimise the pipeline for a where statement after the join with multiple ands', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        AND i.instock >= 0
                        AND i.id >0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-6',
                mode: 'write',
                outputPipeline: true,
            });
            assert(
                isEqual(pipeline, [
                    {
                        $project: {
                            o: '$$ROOT',
                        },
                    },
                    {
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
                                            $gt: ['$id', 0],
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
                                {
                                    $match: {
                                        $expr: {
                                            $gte: ['$id', 0],
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$sku', '$$o_item'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$i',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                    },
                    {
                        $limit: 1,
                    },
                ])
            );
        });
        it('should optimise the pipeline for a where statement after the join with multiple ors', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR i.instock >= 0
                        OR i.id >0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-6',
                mode: 'write',
                outputPipeline: true,
            });
            assert(
                isEqual(pipeline, [
                    {
                        $project: {
                            o: '$$ROOT',
                        },
                    },
                    {
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
                                                    $gt: ['$id', 0],
                                                },
                                                {
                                                    $gte: ['$id', 0],
                                                },
                                                {
                                                    $gte: ['$instock', 0],
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$sku', '$$o_item'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$i',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                    },
                    {
                        $limit: 1,
                    },
                ])
            );
        });
        it('should not optimise a simple join', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-7',
                mode,
            });
            assert(
                isEqual(pipeline, [
                    {
                        $project: {
                            o: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'inventory',
                            as: 'i',
                            localField: 'o.item',
                            foreignField: 'sku',
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$i',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                    },
                    {
                        $limit: 1,
                    },
                ])
            );
        });
        it('should optimise a simple join with a where on destination', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-8',
                mode: 'write',
                outputPipeline: true,
            });
            assert(
                isEqual(pipeline, [
                    {
                        $project: {
                            o: '$$ROOT',
                        },
                    },
                    {
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
                                            $gte: ['$id', 0],
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$sku', '$$o_item'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$i',
                                    },
                                    0,
                                ],
                            },
                        },
                    },
                    {
                        $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                    },
                    {
                        $limit: 1,
                    },
                ])
            );
        });
        it('should optimise a join with a where with misc', async () => {
            const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE (o.price >= 0
                        OR i.instock >= 0)
                        AND 1 = 1
                        LIMIT 1
                        `;
            const {pipeline} = await queryResultTester({
                queryString: queryString,
                casePath: 'where.case-9',
                mode: 'write',
                outputPipeline: true,
            });
            assert(
                isEqual(pipeline, [
                    {
                        $project: {
                            o: '$$ROOT',
                        },
                    },
                    {
                        $lookup: {
                            from: 'inventory',
                            as: 'i',
                            let: {
                                o_item: '$o.item',
                                o_price: '$o.price',
                            },
                            pipeline: [
                                {
                                    $match: {
                                        $expr: {
                                            $or: [
                                                {
                                                    $gte: ['$instock', 0],
                                                },
                                                {
                                                    $gte: ['$$o_price', 0],
                                                },
                                            ],
                                        },
                                    },
                                },
                                {
                                    $match: {
                                        $expr: {
                                            $eq: ['$sku', '$$o_item'],
                                        },
                                    },
                                },
                            ],
                        },
                    },
                    {
                        $match: {
                            $expr: {
                                $gt: [
                                    {
                                        $size: '$i',
                                    },
                                    0,
                                ],
                            },
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
                        $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                    },
                    {
                        $limit: 1,
                    },
                ])
            );
        });
    });
});
