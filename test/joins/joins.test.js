const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester');
const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');

describe('joins', function () {
    this.timeout(90000);
    const fileName = 'join-cases';
    const mode = 'test';
    const dirName = __dirname;
    /** @type {import('../utils/query-tester/types').QueryResultTester} */
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
    describe('existing regression tests', () => {
        it('should work for case 1', async () => {
            await queryResultTester({
                queryString: `
                select
                    c.id,
                    c.'First Name',
                    c.'Last Name',
                    cn.id as CNoteId,
                    cn.notes as Note,
                    cn.date as CNDate,
                    unset(_id)
                from customers c
                inner join 'customer-notes|unwind' cn on cn.id=c.id
                inner join 'customer-notes|unwind' cn2 on cn2.id=convert(c.id,'int')
                limit 4`,
                casePath: 'case1',
            });
        });

        it('should work for case 2', async () => {
            await queryResultTester({
                queryString: `
                SELECT
                    c.id,
                    c.'First Name',
                    c.'Last Name',
                    cn.id as CNoteId,
                    cn.notes as Note,
                    cn.date as CNDate,
                    unset(_id)
                FROM customers c
                left outer join 'customer-notes' 'cn|first' on cn.id=to_int(c.id)
                LIMIT 5`,
                casePath: 'case2',
            });
        });

        it('should work for case 3', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|unwind` as cn on `cn`.id=c.id and cn.id<5 where c.id>1',
                casePath: 'case3',
            });
        });

        it('should work for case 4', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|unwind` as cn on `cn`.id=c.id and cn.id<5',
                casePath: 'case4',
            });
        });

        it('should work for case 5', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|first` as cn on `cn`.id=c.id and cn.id<5',
                casePath: 'case5',
            });
        });

        it('should work for case 6', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|first` as cn on `cn`.id=c.id and cn.id<5',
                casePath: 'case6',
            });
        });

        it('should work for case 7', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|optimize|first` on c.id=cn.id',
                casePath: 'case7',
            });
        });

        it('should work for case 8', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|optimize|first` on cn.id=c.id',
                casePath: 'case8',
            });
        });

        it('should work for case 9', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|optimize` on cn.id=c.id',
                casePath: 'case9',
            });
        });

        it('should work for case 10', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|first` on cn.id=c.id',
                casePath: 'case10',
            });
        });

        it('should work for case 11', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes` cn on cn.id=c.id and (cn.id>2 or cn.id<5)',
                casePath: 'case11',
            });
        });
    });
    describe('left join', () => {
        it('should be able to do a left join', async () => {
            await queryResultTester({
                queryString:
                    'select *, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory` as i  on o.item=i.sku',
                casePath: 'basic-left-join',
            });
        });
        it('should be able to do a left join with an unwind', async () => {
            await queryResultTester({
                queryString:
                    'select *, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory|unwind` as i on o.item=i.sku',
                casePath: 'left-join-with-unwind-hint',
            });
        });

        it('should be able to do a left join with an unwind in the query', async () => {
            await queryResultTester({
                queryString:
                    'select o.id as OID,unwind(i) as inv, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory` i on o.item=i.sku',
                casePath: 'left-join-with-unwind-query',
            });
        });

        it('should be able to do a left join with an unwind and a case statement', async () => {
            await queryResultTester({
                queryString:
                    "select (case when o.id=1 then 'Yes' else 'No' end) as IsOne, unset(_id) from orders as o left join `inventory|unwind` as i  on o.item=i.sku",
                casePath: 'left-join-with-unwind-and-case',
            });
        });

        it('should be able to do a left join on special characters', async () => {
            await queryResultTester({
                queryString:
                    'select i.description,i.specialChars as iChars, o.item, o.specialChars as oChars, unset(_id) from orders as o left join `inventory|unwind` as i on o.specialChars=i.specialChars',
                casePath: 'left-join-with-unwind-and-special-chars',
            });
        });

        it('should be able to do a left join the other way round', async () => {
            await queryResultTester({
                queryString:
                    'select *, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory` as i on i.sku=o.item',
                casePath: 'left-join-reversed',
            });
        });
        // it('should be able to do a left join with multiple conditions', async () => {
        //     const queryText =
        //         'select * from orders as o left join `inventory` as i on o.item=i.sku and o.id=i.id';
        //     const parsedQuery = SQLParser.makeMongoAggregate(queryText);
        //     const hcp = [
        //         {
        //             $project: {
        //                 o: '$$ROOT',
        //             },
        //         },
        //         {
        //             $lookup: {
        //                 from: 'inventory',
        //                 as: 'i',
        //                 let: {
        //                     o_item: '$o.item',
        //                     o_id: '$o.id',
        //                 },
        //                 pipeline: [
        //                     {
        //                         $match: {
        //                             $expr: {
        //                                 $and: [
        //                                     {
        //                                         $eq: ['$sku', '$$o_item'],
        //                                     },
        //                                     {
        //                                         $eq: ['$id', '$$o_id'],
        //                                     },
        //                                 ],
        //                             },
        //                         },
        //                     },
        //                 ],
        //                 // pipeline: [
        //                 //     {
        //                 //         $match: {
        //                 //             $expr: {
        //                 //                 $and: [
        //                 //                     {
        //                 //                         $eq: ['$item', '$$i_sku'],
        //                 //                     },
        //                 //                     {
        //                 //                         $eq: ['$id', '$$i_id'],
        //                 //                     },
        //                 //                 ],
        //                 //             },
        //                 //         },
        //                 //     },
        //                 // ],
        //             },
        //         },
        //     ];
        //     try {
        //         const results = await mongoClient
        //             .db(dbName)
        //             .collection(parsedQuery.collections[0])
        //             .aggregate(parsedQuery.pipeline)
        //             .toArray();
        //         assert(results);
        //         assert(results.length === 4);
        //         for (const result of results) {
        //             assert(result.i.length);
        //         }
        //         return;
        //     } catch (err) {
        //         console.error(err);
        //         throw err;
        //     }
        // });
        // it('should be able to do a left join with multiple conditions the other way around', async () => {
        //     const queryText =
        //         'select * from orders as o left join `inventory` as i on i.sku=o.item and i.id=o.id';
        //     const parsedQuery = SQLParser.makeMongoAggregate(queryText);
        //     try {
        //         const results = await mongoClient
        //             .db(dbName)
        //             .collection(parsedQuery.collections[0])
        //             .aggregate(parsedQuery.pipeline)
        //             .toArray();
        //         assert(results);
        //         assert(results.length === 4);
        //         for (const result of results) {
        //             assert(result.i.length);
        //         }
        //         return;
        //     } catch (err) {
        //         console.error(err);
        //         throw err;
        //     }
        // });
    });

    describe('join order of queries', () => {
        it('should prase the query that was not working on prod', async () => {
            await queryResultTester({
                queryString: `
                SELECT
                    unset(_id)
                    ,pol.CustId
                    ,cust.CustId as c_CustId
                    ,pol.ExecCode
                    ,emp.EmpCode as emp_EmpCode
                FROM \`ams360-powerbi-basicpolinfo\` pol
                INNER join \`ams360-powerbi-customer|unwind\` cust on pol.CustId = cust.CustId
                LEFT join \`ams360-powerbi-employee|unwind\` emp on pol.ExecCode = emp.EmpCode
                LIMIT 5`,
                casePath: 'join-order',
            });
        });
        describe('Existing example', () => {
            it('should have the right lookup when no aliases specified', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on sku=item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
            it('should have the right lookup when only left alias is specified', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on i.sku=item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
            it('should have the right lookup when only right alias is specified', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on sku=o.item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
            it('should have the right lookup when both aliases are specified', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on i.sku=o.item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
        });
        describe('Existing example - reversed', () => {
            it('should have the right lookup when no aliases specified - reversed', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on item=sku';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'item',
                    localField: 'o.sku',
                    as: 'i',
                });
            });
            it('should have the right lookup when only left alias is specified - reversed', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on i.sku=item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
            it('should have the right lookup when only right alias is specified - reversed', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on sku=o.item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
            it('should have the right lookup when both aliases are specified - reversed', () => {
                const queryText =
                    'select * from orders as o inner join `inventory` as i on i.sku=o.item';
                const parsedQuery = SQLParser.parseSQL(queryText);
                const lookup = parsedQuery.pipeline[1].$lookup;
                assert.deepEqual(lookup, {
                    from: 'inventory',
                    foreignField: 'sku',
                    localField: 'o.item',
                    as: 'i',
                });
            });
        });
    });

    describe('n-level joins', () => {
        it('should be able to do n level joins without an unwind', async () => {
            await queryResultTester({
                queryString: `SELECT
                    o.id as orderId
                    ,i.id as inventoryId
                    ,c.id as customerId,
                    unset(_id)
                FROM orders as o
                INNER JOIN \`inventory\` as i ON o.item=i.sku
                INNER JOIN \`customers\` as c ON o.customerId=c.id
                WHERE o.id=1`,
                casePath: 'n-level-join-without-unwind',
            });
        });
        it('should be able to do n level joins with unwinds', async () => {
            await queryResultTester({
                queryString: `SELECT
                    o.id as orderId
                    ,i.id as inventoryId
                    ,c.id as customerId,
                    unset(_id)
                FROM orders as o
                INNER JOIN \`inventory|unwind\` as i ON o.item=i.sku
                INNER JOIN \`customers|unwind\` as c ON o.customerId=c.id`,
                casePath: 'n-level-join-with-unwind',
            });
        });
    });
});
