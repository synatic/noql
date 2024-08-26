const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');
const assert = require('assert');
const {isEqual} = require('lodash');
describe.skip('optimizations', function () {
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
         * [] subqueries
         * [] 2+ joins
         * [] support for not
         * [] multiple and/ ors with brackets
         * source & destination | and vs or |
         * Maybe clone the where and pipeline before optimising, return errors:true and revert?
         */
        describe('simple', () => {
            it('01. should not optimise a simple join', async () => {
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
            it('02. should optimise a simple join with a where on destination', async () => {
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
            it('03. should optimise a simple join with a where on source', async () => {
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
            it('40. should optimise a join with a where with misc', async () => {
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
            it('41. should not optimise if the nooptimise join hint is included', async () => {
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
    });
});
