const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');
const assert = require('assert');
const {isEqual} = require('lodash');
describe('optimizations', function () {
    this.timeout(90000);
    const fileName = 'optimizations';
    /** @type {'test'|'write'} */
    const mode = 'test';
    const outputPipeline = false;
    const dirName = __dirname;
    /** @type {import("../utils/query-tester/types.js").QueryResultTester} */
    let queryResultTester;
    /** @type {import("mongodb").MongoClient} */
    let mongoClient;
    before(async function () {
        const {client, db} = await setup();
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

    describe('where statements on joins', () => {
        /**
         * TODO Test for:
         * [] subqueries
         * [] 2+ joins
         * [] support for not
         * [] multiple and/ ors with brackets
         * source & destination | and vs or |
         * Maybe clone the where and pipeline before optimising, return errors:true and revert?
         */
        describe('simple', () => {
            it('01. should not optimize a simple join', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-01',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
            it('02. should optimize a simple join with a where on destination', async () => {
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
                    casePath: 'where.case-02',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
            it('03. should optimize a simple join with a where on source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.id >= 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-03',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.id': {
                                    $gte: 0,
                                },
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
        });

        describe('one AND', () => {
            it('04. should work for an AND for source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.price >= 0
                        AND o.quantity >= 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-04',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'o.price': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.quantity': {
                                            $gte: 0,
                                        },
                                    },
                                ],
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
            it('05. should work for an AND for destination', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.instock >= 0
                        AND i.id >= 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-05',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                                $and: [
                                                    {
                                                        $gte: ['$instock', 0],
                                                    },
                                                    {
                                                        $gte: ['$id', 0],
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
            it('06. should work for an AND for source and destination', async () => {
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
                    casePath: 'where.case-06',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.price': {
                                    $gte: 0,
                                },
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
            it('07. should work for an AND for destination and source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.instock >= 0
                        AND o.price >= 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-07',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.price': {
                                    $gte: 0,
                                },
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
        });

        describe('one OR', () => {
            it('08. should work for an OR for source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.price >= 0
                        OR o.quantity >= 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-08',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $or: [
                                    {
                                        'o.price': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.quantity': {
                                            $gte: 0,
                                        },
                                    },
                                ],
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
            it('09. should work for an OR for destination', async () => {
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
                    casePath: 'where.case-09',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
            it('10. should work for an OR for source and destination', async () => {
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
                    casePath: 'where.case-10',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                            $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                        },
                        {
                            $limit: 1,
                        },
                    ])
                );
            });
            it('11. should work for an OR for destination and source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.instock >= 0
                        OR o.price >= 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-11',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                            $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                        },
                        {
                            $limit: 1,
                        },
                    ])
                );
            });
        });

        describe('two ANDs', () => {
            it('12. all destination', async () => {
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
                    casePath: 'where.case-12',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                                $and: [
                                                    {
                                                        $gte: ['$id', 0],
                                                    },
                                                    {
                                                        $gte: ['$instock', 0],
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
            it('13. destination,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        AND i.instock >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-13',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.id': {
                                    $gt: 0,
                                },
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
                                                $and: [
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
            it('14. destination,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        AND o.price >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-14',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.id': {
                                    $gt: 0,
                                },
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
                                                $and: [
                                                    {
                                                        $gte: ['$id', 0],
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
                            $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                        },
                        {
                            $limit: 1,
                        },
                    ])
                );
            });
            it('15. source,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        AND o.price >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-15',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'o.customerId': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.price': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
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
        });

        describe('two ORs ', () => {
            it('16. all destination', async () => {
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
                    casePath: 'where.case-16',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                                        $gte: ['$id', 0],
                                                    },
                                                    {
                                                        $gte: ['$instock', 0],
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
            it('17. destination,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR i.instock >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-17',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
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
                                                    {
                                                        $gt: ['$$o_id', 0],
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
            it('18. destination,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR o.price >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-14',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
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
                                                        $gte: ['$$o_price', 0],
                                                    },
                                                    {
                                                        $gt: ['$$o_id', 0],
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
            it('19. source,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        OR o.price >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-19',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $or: [
                                    {
                                        'o.customerId': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.price': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
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
        });

        describe('AND OR', () => {
            it('21. AND OR, destination,destination,destination', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        AND i.instock >= 0
                        OR i.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-21',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                                        $and: [
                                                            {
                                                                $gte: [
                                                                    '$id',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$instock',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('22. AND OR, destination,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        AND i.instock >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-22',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $gt: ['$$o_id', 0],
                                                    },
                                                    {
                                                        $and: [
                                                            {
                                                                $gte: [
                                                                    '$id',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$instock',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('23. AND OR, destination,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        AND o.price >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-23',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
                                    o_price: '$o.price',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $gt: ['$$o_id', 0],
                                                    },
                                                    {
                                                        $and: [
                                                            {
                                                                $gte: [
                                                                    '$id',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$$o_price',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('24. AND OR, source,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        AND i.instock >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-24',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
                                    o_customer_id: '$o.customerId',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $gt: ['$$o_id', 0],
                                                    },
                                                    {
                                                        $and: [
                                                            {
                                                                $gte: [
                                                                    '$instock',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$$o_customer_id',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('25. AND OR, source,destination,destination', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        AND i.instock >= 0
                        OR i.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-25',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_customer_id: '$o.customerId',
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
                                                        $and: [
                                                            {
                                                                $gte: [
                                                                    '$instock',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$$o_customer_id',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('26. AND OR, source,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        AND o.price >= 0
                        OR o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-26',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $or: [
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                    {
                                        $and: [
                                            {
                                                'o.customerId': {
                                                    $gte: 0,
                                                },
                                            },
                                            {
                                                'o.price': {
                                                    $gte: 0,
                                                },
                                            },
                                        ],
                                    },
                                ],
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
        });

        describe('OR AND', () => {
            it('27. OR AND, destination,destination,destination', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR i.instock >= 0
                        AND i.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-27',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                                $and: [
                                                    {
                                                        $gt: ['$id', 0],
                                                    },
                                                    {
                                                        $or: [
                                                            {
                                                                $gte: [
                                                                    '$id',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$instock',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('28. OR AND, destination,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR i.instock >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-28',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
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
            it('29. OR AND, destination,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.id >= 0
                        OR o.price >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-29',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
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
                                                        $gte: ['$id', 0],
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
                            $unset: ['_id', 'o._id', 'i._id', 'o.orderDate'],
                        },
                        {
                            $limit: 1,
                        },
                    ])
                );
            });
            it('30. OR AND, source,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        OR i.instock >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-30',
                    mode,
                    outputPipeline,
                    expectZeroResults: true,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $lookup: {
                                from: 'inventory',
                                as: 'i',
                                let: {
                                    o_item: '$o.item',
                                    o_customer_id: '$o.customerId',
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
                                                        $gte: [
                                                            '$$o_customer_id',
                                                            0,
                                                        ],
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
            it('31. OR AND, source,destination,destination', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        OR i.instock >= 0
                        AND i.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-31',
                    mode: 'write',
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_customer_id: '$o.customerId',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $and: [
                                                    {
                                                        $gt: ['$id', 0],
                                                    },
                                                    {
                                                        $or: [
                                                            {
                                                                $gte: [
                                                                    '$instock',
                                                                    0,
                                                                ],
                                                            },
                                                            {
                                                                $gte: [
                                                                    '$$o_customer_id',
                                                                    0,
                                                                ],
                                                            },
                                                        ],
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
            it('32. OR AND, source,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.customerId >=0
                        OR o.price >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-32',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $and: [
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                    {
                                        $or: [
                                            {
                                                'o.customerId': {
                                                    $gte: 0,
                                                },
                                            },
                                            {
                                                'o.price': {
                                                    $gte: 0,
                                                },
                                            },
                                        ],
                                    },
                                ],
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
        });

        describe('three ORs ', () => {
            it('33. destination,destination,destination,destination', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.sku = 'almonds'
                        OR i.instock >= 0
                        OR i.id >0
                        OR i.description ='product 1'
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-33',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                                        $eq: [
                                                            '$sku',
                                                            'almonds',
                                                        ],
                                                    },
                                                    {
                                                        $gte: ['$instock', 0],
                                                    },
                                                    {
                                                        $gt: ['$id', 0],
                                                    },
                                                    {
                                                        $eq: [
                                                            '$description',
                                                            'product 1',
                                                        ],
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
            it('34. destination,destination,destination,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.sku = 'almonds'
                        OR i.instock >= 0
                        OR i.id >0
                        OR o.id > 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-34',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $eq: [
                                                            '$sku',
                                                            'almonds',
                                                        ],
                                                    },
                                                    {
                                                        $gte: ['$instock', 0],
                                                    },
                                                    {
                                                        $gt: ['$id', 0],
                                                    },
                                                    {
                                                        $gt: ['$$o_id', 0],
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
            it('35. destination,destination,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.sku = 'almonds'
                        OR i.instock >= 0
                        OR o.price >= 0
                        OR o.id > 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-35',
                    mode: 'write',
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_id: '$o.id',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $eq: [
                                                            '$sku',
                                                            'almonds',
                                                        ],
                                                    },
                                                    {
                                                        $gte: ['$instock', 0],
                                                    },
                                                    {
                                                        $gte: ['$$o_price', 0],
                                                    },
                                                    {
                                                        $gt: ['$$o_id', 0],
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
            it('36. destination,source,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE i.sku = 'almonds'
                        OR o.customerId >=0
                        OR o.price >= 0
                        OR o.id > 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-36',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                                    o_customer_id: '$o.customerId',
                                    o_price: '$o.price',
                                    o_id: '$o.id',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $eq: [
                                                            '$sku',
                                                            'almonds',
                                                        ],
                                                    },
                                                    {
                                                        $gte: [
                                                            '$$o_customer_id',
                                                            0,
                                                        ],
                                                    },
                                                    {
                                                        $gte: ['$$o_price', 0],
                                                    },
                                                    {
                                                        $gt: ['$$o_id', 0],
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
            it('37. source,source,source,source', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN inventory i on i.sku=o.item
                        WHERE o.item = 'almonds'
                        OR o.customerId >=0
                        OR o.price >= 0
                        OR o.id > 0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-37',
                    mode: 'write',
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $or: [
                                    {
                                        'o.item': {
                                            $eq: 'almonds',
                                        },
                                    },
                                    {
                                        'o.customerId': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.price': {
                                            $gte: 0,
                                        },
                                    },
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
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
        });

        describe('misc', () => {
            it('38. should work for an AND for source and destination with additional AND on destination join', async () => {
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
                    casePath: 'where.case-38',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.price': {
                                    $gte: 0,
                                },
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
                                                $and: [
                                                    {
                                                        $eq: [
                                                            '$sku',
                                                            '$$o_item',
                                                        ],
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
            it('39. should work for an AND for source and destination with additional OR on destination join', async () => {
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
                    casePath: 'where.case-39',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                o: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                'o.price': {
                                    $gte: 0,
                                },
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
                                                $gte: ['$instock', 0],
                                            },
                                        },
                                    },
                                    {
                                        $match: {
                                            $expr: {
                                                $or: [
                                                    {
                                                        $eq: [
                                                            '$sku',
                                                            '$$o_item',
                                                        ],
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
            it('40. should optimize a join with a where with misc', async () => {
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
                    casePath: 'where.case-40',
                    mode: 'write',
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
            it('41. should not optimize if the nooptimize join hint is included', async () => {
                const queryString = `
                        SELECT *,
                            unset(_id, o._id, i._id,o.orderDate)
                        FROM orders o
                        INNER JOIN \`inventory|nooptimize\` i on i.sku=o.item
                        WHERE o.customerId >=0
                        AND o.price >= 0
                        AND o.id >0
                        LIMIT 1
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'where.case-15',
                    mode,
                    outputPipeline,
                    optimizeJoins: true,
                    unsetId: false,
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
                            $match: {
                                $and: [
                                    {
                                        $and: [
                                            {
                                                'o.customerId': {
                                                    $gte: 0,
                                                },
                                            },
                                            {
                                                'o.price': {
                                                    $gte: 0,
                                                },
                                            },
                                        ],
                                    },
                                    {
                                        'o.id': {
                                            $gt: 0,
                                        },
                                    },
                                ],
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

        describe('use-cases', () => {
            it('1. should correctly optimise the query', async () => {
                const queryString = `
                        SELECT   T0_0."BilledPremium" AS "p0_0",
                                 T0_0."FullTermPremium" AS "p1_0",
                                 T0_0."Premium" AS "p2_0",
                                 T0_0."WrittenPremium" AS "p3_0",
                                 T1_0."PolNo" AS "p4_0",
                                 T0_0."LineOfBus" AS "p5_0",
                                 T0_0."EstRevenue" AS "p6_0",
                                 T1_0."PolType" AS "p7_0",
                                 T2_0."Name" AS "p8_0",
                                 T3_0."Name" AS "p9_0",
                                 T4_0."Name" AS "p10_0",
                                 T5_0."Name" AS "p11_0",
                                 T6_0."Name" AS "p12_0",
                                 T0_0."EffDate" AS "p13_0",
                                 T0_0."EnteredDate" AS "p14_0"
                        FROM "public"."ams360-data-warehouse-dds-buffers--afw_policytranpremium" T0_0
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afwbasicpolinfo" T1_0 ON T0_0."PolId" = T1_0."PolId"
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afw_company" T2_0 ON T1_0."WritingCode" = T2_0."CoCode"
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afw_company" T3_0 ON T1_0."Cocode" = T3_0."CoCode"
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afw_generalledgerdepartment" T4_0 ON T1_0."GLDeptCode" = T4_0."GLDeptCode"
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afw_generalledgerdivision" T5_0 ON T1_0."GLDivCode" = T5_0."GLDivCode"
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afw_generalledgergroup" T6_0 ON T1_0."GlGrpCode" = T6_0."GlGrpCode"
                        WHERE (T1_0."PolExpDate" >= '2024-09-10 00:00:00.000')
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'use-cases.case-1',
                    mode: 'write',
                    outputPipeline,
                    skipDbQuery: true,
                    optimizeJoins: true,
                    unsetId: false,
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                T0_0: '$$ROOT',
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afwbasicpolinfo',
                                as: 'T1_0',
                                let: {
                                    t_0_0_pol_id: '$T0_0.PolId',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $gte: [
                                                    '$PolExpDate',
                                                    '2024-09-10 00:00:00.000',
                                                ],
                                            },
                                        },
                                    },
                                    {
                                        $match: {
                                            $expr: {
                                                $eq: [
                                                    '$PolId',
                                                    '$$t_0_0_pol_id',
                                                ],
                                            },
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afw_company',
                                as: 'T2_0',
                                localField: 'T1_0.WritingCode',
                                foreignField: 'CoCode',
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afw_company',
                                as: 'T3_0',
                                localField: 'T1_0.Cocode',
                                foreignField: 'CoCode',
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afw_generalledgerdepartment',
                                as: 'T4_0',
                                localField: 'T1_0.GLDeptCode',
                                foreignField: 'GLDeptCode',
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afw_generalledgerdivision',
                                as: 'T5_0',
                                localField: 'T1_0.GLDivCode',
                                foreignField: 'GLDivCode',
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afw_generalledgergroup',
                                as: 'T6_0',
                                localField: 'T1_0.GlGrpCode',
                                foreignField: 'GlGrpCode',
                            },
                        },
                        {
                            $project: {
                                p0_0: '$T0_0.BilledPremium',
                                p1_0: '$T0_0.FullTermPremium',
                                p2_0: '$T0_0.Premium',
                                p3_0: '$T0_0.WrittenPremium',
                                p4_0: '$T1_0.PolNo',
                                p5_0: '$T0_0.LineOfBus',
                                p6_0: '$T0_0.EstRevenue',
                                p7_0: '$T1_0.PolType',
                                p8_0: '$T2_0.Name',
                                p9_0: '$T3_0.Name',
                                p10_0: '$T4_0.Name',
                                p11_0: '$T5_0.Name',
                                p12_0: '$T6_0.Name',
                                p13_0: '$T0_0.EffDate',
                                p14_0: '$T0_0.EnteredDate',
                            },
                        },
                    ])
                );
            });

            it('2. should correctly optimise the query when the schema implicitly casts to $date', async () => {
                const queryString = `
                        SELECT  t0_0."GLDate" AS "p0_0",
                                t0_0."Description" AS "p1_0",
                                t0_0."CommPayType" AS "p2_0",
                                t1_0."CommAmt" AS "p3_0",
                                t1_0."CommPersCode" AS "p4_0"
                        FROM "public"."ams360-data-warehouse-dds-buffers--afw_invoicetransaction" t0_0
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afwinvoicecommission" t1_0
                            ON t0_0."InvId" = t1_0."InvId"
                        WHERE (t0_0."GLDate" >= '2024-07-01 00:00:00.000' AND t1_0."CommPersType" = 'A')
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'use-cases.case-2',
                    mode: 'write',
                    outputPipeline,
                    skipDbQuery: true,
                    optimizeJoins: true,
                    unwindJoins: true,
                    unsetId: true,
                    schemas: {
                        'ams360-data-warehouse-dds-buffers--afw_invoicetransaction':
                            {
                                type: 'object',
                                properties: {
                                    _id: {type: 'string', format: 'mongoid'},
                                    BackInvTPId: {
                                        type: ['null', 'string'],
                                        stringLength: 36,
                                    },
                                    BHId: {type: 'string', stringLength: 36},
                                    BillMethod: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    BillSeqId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    BillTranId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    BinderPolTEffDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    BinderPostMethod: {
                                        type: ['null', 'string'],
                                        stringLength: 1,
                                    },
                                    BinderStatus: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    BrokerCode: {type: 'null'},
                                    ChangedBy: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    ChangedDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    ChargeCat: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    ChargeCode: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    CoCode: {type: 'string', stringLength: 3},
                                    CommPayType: {
                                        type: ['null', 'string'],
                                        stringLength: 1,
                                    },
                                    CoType: {type: 'string', stringLength: 1},
                                    CSHId: {type: 'null'},
                                    CustId: {type: 'string', stringLength: 36},
                                    Description: {
                                        type: 'string',
                                        stringLength: 45,
                                    },
                                    EnteredDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    FullTermPremAmt: {type: 'null'},
                                    GLDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    GrossAmt: {type: 'number'},
                                    InvEffDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    InvId: {type: 'string', stringLength: 36},
                                    InvTPId: {type: 'string', stringLength: 36},
                                    IsCancelled: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    IsInstallment: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    IsPosted: {type: 'string', stringLength: 1},
                                    JournalTranId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    LineOfBus: {
                                        type: 'string',
                                        stringLength: 5,
                                    },
                                    NewInvTPId: {
                                        type: ['null', 'string'],
                                        stringLength: 36,
                                    },
                                    NonPrRecipient: {type: 'null'},
                                    OldInvTPId: {
                                        type: ['null', 'string'],
                                        stringLength: 36,
                                    },
                                    PlanType: {
                                        type: ['null', 'string'],
                                        stringLength: 5,
                                    },
                                    PolTEffDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    PolTPId: {
                                        type: ['string', 'null'],
                                        stringLength: 36,
                                    },
                                    RBBkOutId: {
                                        type: ['null', 'string'],
                                        stringLength: 36,
                                    },
                                    ReplaceDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    TranType: {type: 'string', stringLength: 3},
                                    WritingCode: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    _dateUpdated: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                },
                            },
                        'ams360-data-warehouse-dds-buffers--afwinvoicecommission':
                            {
                                type: 'object',
                                properties: {
                                    _id: {type: 'string', format: 'mongoid'},
                                    BhId: {type: 'string', stringLength: 36},
                                    BillSeqId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    BillTranId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    ChangedBy: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    ChangedDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    CommAmt: {type: 'number'},
                                    CommPersCode: {
                                        type: ['string', 'null'],
                                        stringLength: 3,
                                    },
                                    CommPersType: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    CommPremAmt: {type: 'number'},
                                    CshId: {
                                        type: ['string', 'null'],
                                        stringLength: 36,
                                    },
                                    EnteredDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    InvCmId: {type: 'string', stringLength: 36},
                                    InvCoId: {type: 'string', stringLength: 36},
                                    InvId: {type: 'string', stringLength: 36},
                                    InvTpId: {type: 'string', stringLength: 36},
                                    IsActive: {type: 'string', stringLength: 1},
                                    MemoFlag: {type: 'string', stringLength: 1},
                                    Method: {type: 'string', stringLength: 1},
                                    NetFlag: {type: 'string', stringLength: 1},
                                    OverrideFlag: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    Percentage: {type: 'integer'},
                                    PrimaryFlag: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    ProductionCreditAmount: {
                                        type: ['null', 'integer'],
                                    },
                                    ProductionCreditOverrideFlag: {
                                        type: 'null',
                                    },
                                    ProductionCreditSplitPercentage: {
                                        type: ['null', 'integer'],
                                    },
                                    _dateUpdated: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                },
                            },
                    },
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                t0_0: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $expr: {
                                    $and: [
                                        {
                                            $gte: [
                                                {
                                                    $toDate: '$t0_0.GLDate',
                                                },
                                                {
                                                    $toDate: {
                                                        $literal:
                                                            '2024-07-01T00:00:00.000Z',
                                                    },
                                                },
                                            ],
                                        },
                                        {
                                            $ne: [
                                                {
                                                    $type: '$t0_0.GLDate',
                                                },
                                                'null',
                                            ],
                                        },
                                        {
                                            $ne: [
                                                {
                                                    $type: '$t0_0.GLDate',
                                                },
                                                'missing',
                                            ],
                                        },
                                    ],
                                },
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afwinvoicecommission',
                                as: 't1_0',
                                let: {
                                    t_0_0_inv_id: '$t0_0.InvId',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $eq: ['$CommPersType', 'A'],
                                            },
                                        },
                                    },
                                    {
                                        $match: {
                                            $expr: {
                                                $eq: [
                                                    '$InvId',
                                                    '$$t_0_0_inv_id',
                                                ],
                                            },
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $unwind: {
                                path: '$t1_0',
                                preserveNullAndEmptyArrays: true,
                            },
                        },
                        {
                            $project: {
                                p0_0: '$t0_0.GLDate',
                                p1_0: '$t0_0.Description',
                                p2_0: '$t0_0.CommPayType',
                                p3_0: '$t1_0.CommAmt',
                                p4_0: '$t1_0.CommPersCode',
                            },
                        },
                        {
                            $unset: '_id',
                        },
                    ])
                );
            });

            it('3. should correctly optimise the query when the schema implicitly casts to $date', async () => {
                const queryString = `
                        SELECT T0_0."BilledPremium" AS "p0_0",
                            T0_0."FullTermPremium" AS "p1_0",
                            T0_0."Premium" AS "p2_0",
                            T0_0."WrittenPremium" AS "p3_0",
                            T0_0."EffDate" AS "p4_0",
                            T0_0."EnteredDate" AS "p5_0",
                            T1_0."PolExpDate" AS "p6_0",
                            T1_0."PolNo" AS "p7_0"
                        FROM "public"."ams360-data-warehouse-dds-buffers--afw_policytranpremium" T0_0
                        LEFT OUTER JOIN "public"."ams360-data-warehouse-dds-buffers--afwbasicpolinfo" T1_0
                            ON T0_0."PolId" = T1_0."PolId"
                        WHERE (T0_0."EffDate" >= '2024-01-01 00:00:00.000' AND T1_0."PolExpDate" >= '2024-10-08 00:00:00.000')
                        LIMIT 10
                        `;
                const {pipeline} = await queryResultTester({
                    queryString: queryString,
                    casePath: 'use-cases.case-2',
                    mode: 'write',
                    outputPipeline,
                    skipDbQuery: true,
                    optimizeJoins: true,
                    unwindJoins: true,
                    unsetId: true,
                    schemas: {
                        'ams360-data-warehouse-dds-buffers--afw_policytranpremium':
                            {
                                type: 'object',
                                properties: {
                                    _id: {
                                        type: 'string',
                                        format: 'mongoid',
                                    },
                                    BilledPremium: {
                                        type: 'number',
                                    },
                                    ChangedBy: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    ChangedDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    ChargeCatPolTP: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    ChargeCodePolTP: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    CoCodePolTP: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    CoTypePolTP: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    DescriptionPolTP: {
                                        type: 'string',
                                        stringLength: 25,
                                    },
                                    EffDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    EnteredDate: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                    EstRevenue: {
                                        type: ['number', 'null'],
                                    },
                                    FullTermPremium: {
                                        type: 'number',
                                    },
                                    HowBilled: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    IncludePremium: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    InsertSeqNo: {
                                        type: 'integer',
                                    },
                                    IsCorrected: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    IsPosted: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    IsSuspended: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    LineOfBus: {
                                        type: ['string', 'null'],
                                        stringLength: 5,
                                    },
                                    Nonprinsttreatpoltp: {
                                        type: ['string', 'null'],
                                        stringLength: 1,
                                    },
                                    NonPrRecipientPolTP: {
                                        type: ['null', 'string'],
                                        stringLength: 1,
                                    },
                                    PercentOfRisk: {
                                        type: 'null',
                                    },
                                    PlanType: {
                                        type: 'null',
                                    },
                                    PolId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    PolTPId: {
                                        type: 'string',
                                        stringLength: 36,
                                    },
                                    Premium: {
                                        type: 'number',
                                    },
                                    Reconciled: {
                                        type: 'string',
                                        stringLength: 1,
                                    },
                                    TiComId: {
                                        type: 'null',
                                    },
                                    WritingCode: {
                                        type: 'string',
                                        stringLength: 3,
                                    },
                                    WrittenPremium: {
                                        type: 'integer',
                                    },
                                    _dateUpdated: {
                                        type: 'string',
                                        format: 'date-time',
                                    },
                                },
                            },
                        'ams360-data-warehouse-dds-buffers--afwbasicpolinfo': {
                            type: 'object',
                            properties: {
                                _id: {
                                    type: 'string',
                                    format: 'mongoid',
                                },
                                AgcyBusClass: {
                                    type: ['null', 'string'],
                                    stringLength: 13,
                                },
                                AnotId: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                AuditFlag: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                AuditPeriod: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                BillAcctNo: {
                                    type: ['null', 'string'],
                                    stringLength: 10,
                                },
                                BilledStmtPrem: {
                                    type: 'integer',
                                },
                                BillMethod: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                BrokerCode: {
                                    type: 'null',
                                },
                                BusOriginCode: {
                                    type: 'null',
                                },
                                CarrierStatus: {
                                    type: 'null',
                                },
                                ChangedBy: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                ChangedDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                Cocode: {
                                    type: ['string', 'null'],
                                    stringLength: 3,
                                },
                                Compcustno: {
                                    type: 'null',
                                },
                                Cotype: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                CsrCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                CustId: {
                                    type: 'string',
                                    stringLength: 36,
                                },
                                DescriptionBpol: {
                                    type: 'string',
                                    stringLength: 30,
                                },
                                EnteredDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                ExcludeFrmDownload: {
                                    type: ['string', 'null'],
                                    stringLength: 1,
                                },
                                ExecCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                FirstWrittenDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                FlatAmount: {
                                    type: 'null',
                                },
                                FullTermPremium: {
                                    type: 'integer',
                                },
                                Glbrnchcode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                GLDeptCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                GLDivCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                GlGrpCode: {
                                    type: 'string',
                                    stringLength: 3,
                                },
                                InstDay: {
                                    type: 'integer',
                                },
                                IsContinuous: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsExclDelete: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsFiltered: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsFinanced: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                IsMultiEntity: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsNewBPol: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                IsPosted: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsProdCredRequire100: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IsProductionCreditEnabled: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                IsReinsuranceEnabled: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IssuedState: {
                                    type: 'string',
                                    stringLength: 2,
                                },
                                IsSuspended: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                IstId: {
                                    type: 'null',
                                },
                                MasterAgent: {
                                    type: ['null', 'string'],
                                    stringLength: 6,
                                },
                                Method: {
                                    type: 'null',
                                },
                                MethodOfPayments: {
                                    type: 'null',
                                },
                                MultiEntityARFlag: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                NatlProdCode: {
                                    type: 'null',
                                },
                                NegCommValidDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                NotRenewable: {
                                    type: ['null', 'string'],
                                    stringLength: 1,
                                },
                                NumOfPayments: {
                                    type: 'null',
                                },
                                PayPid: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                Percentage: {
                                    type: 'null',
                                },
                                PolChangedDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                PolEffDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                PolExpDate: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                                PolId: {
                                    type: 'string',
                                    stringLength: 36,
                                },
                                PolNo: {
                                    type: 'string',
                                    stringLength: 19,
                                },
                                PolSubtype: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                PolType: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                PolTypeLob: {
                                    type: 'string',
                                    stringLength: 20,
                                },
                                Premadj: {
                                    type: 'null',
                                },
                                PriorPolicy: {
                                    type: ['null', 'string'],
                                    stringLength: 10,
                                },
                                PriorPolid: {
                                    type: ['string', 'null'],
                                    stringLength: 36,
                                },
                                RenewalList: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                RenewalRptFlag: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                ShortPolNo: {
                                    type: 'string',
                                    stringLength: 17,
                                },
                                SourcePolId: {
                                    type: ['null', 'string'],
                                    stringLength: 36,
                                },
                                Status: {
                                    type: 'string',
                                    stringLength: 1,
                                },
                                SubAgent: {
                                    type: ['null', 'string'],
                                },
                                TicomId: {
                                    type: 'null',
                                },
                                TypeOfBus: {
                                    type: 'integer',
                                },
                                Underwriter: {
                                    type: ['null', 'string'],
                                    stringLength: 17,
                                },
                                WritingCode: {
                                    type: ['string', 'null'],
                                    stringLength: 3,
                                },
                                RenewalTermCount: {
                                    type: 'null',
                                },
                                _dateUpdated: {
                                    type: 'string',
                                    format: 'date-time',
                                },
                            },
                        },
                    },
                });
                assert(
                    isEqual(pipeline, [
                        {
                            $project: {
                                T0_0: '$$ROOT',
                            },
                        },
                        {
                            $match: {
                                $expr: {
                                    $and: [
                                        {
                                            $gte: [
                                                {
                                                    $toDate: '$T0_0.EffDate',
                                                },
                                                {
                                                    $toDate: {
                                                        $literal:
                                                            '2024-01-01T00:00:00.000Z',
                                                    },
                                                },
                                            ],
                                        },
                                        {
                                            $ne: [
                                                {
                                                    $type: '$T0_0.EffDate',
                                                },
                                                'null',
                                            ],
                                        },
                                        {
                                            $ne: [
                                                {
                                                    $type: '$T0_0.EffDate',
                                                },
                                                'missing',
                                            ],
                                        },
                                    ],
                                },
                            },
                        },
                        {
                            $lookup: {
                                from: 'ams360-data-warehouse-dds-buffers--afwbasicpolinfo',
                                as: 'T1_0',
                                let: {
                                    t_0_0_pol_id: '$T0_0.PolId',
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $and: [
                                                    {
                                                        $gte: [
                                                            {
                                                                $toDate:
                                                                    '$T1_0.PolExpDate',
                                                            },
                                                            {
                                                                $toDate: {
                                                                    $literal:
                                                                        '2024-10-08T00:00:00.000Z',
                                                                },
                                                            },
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            {
                                                                $type: '$T1_0.PolExpDate',
                                                            },
                                                            'null',
                                                        ],
                                                    },
                                                    {
                                                        $ne: [
                                                            {
                                                                $type: '$T1_0.PolExpDate',
                                                            },
                                                            'missing',
                                                        ],
                                                    },
                                                ],
                                            },
                                        },
                                    },
                                    {
                                        $match: {
                                            $expr: {
                                                $eq: [
                                                    '$PolId',
                                                    '$$t_0_0_pol_id',
                                                ],
                                            },
                                        },
                                    },
                                ],
                            },
                        },
                        {
                            $unwind: {
                                path: '$T1_0',
                                preserveNullAndEmptyArrays: true,
                            },
                        },
                        {
                            $project: {
                                p0_0: '$T0_0.BilledPremium',
                                p1_0: '$T0_0.FullTermPremium',
                                p2_0: '$T0_0.Premium',
                                p3_0: '$T0_0.WrittenPremium',
                                p4_0: '$T0_0.EffDate',
                                p5_0: '$T0_0.EnteredDate',
                                p6_0: '$T1_0.PolExpDate',
                                p7_0: '$T1_0.PolNo',
                            },
                        },
                        {
                            $unset: '_id',
                        },
                        {
                            $limit: 10,
                        },
                    ])
                );
            });
        });
    });
});
