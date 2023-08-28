const {setup, disconnect} = require('../utils/mongo-client.js');
const {buildQueryResultTester} = require('../utils/query-tester');
const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');

describe('joins', function () {
    this.timeout(90000);
    const fileName = 'join-cases';
    /** @type {'test'|'write'} */
    const mode = 'test';
    const dirName = __dirname;
    /** @type {import("../utils/query-tester/types").QueryResultTester} */
    let queryResultTester;
    /** @type {import("mongodb").MongoClient} */
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
    describe('regression tests', () => {
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
                casePath: 'regression.case1',
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
                casePath: 'regression.case2',
            });
        });

        it('should work for case 3', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|unwind` as cn on `cn`.id=c.id and cn.id<5 where c.id>1',
                casePath: 'regression.case3',
            });
        });

        it('should work for case 4', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|unwind` as cn on `cn`.id=c.id and cn.id<5',
                casePath: 'regression.case4',
            });
        });

        it('should work for case 5', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|first` as cn on `cn`.id=c.id and cn.id<5',
                casePath: 'regression.case5',
            });
        });

        it('should work for case 6', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes|first` as cn on `cn`.id=c.id and cn.id<5',
                casePath: 'regression.case6',
            });
        });

        it('should work for case 7', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|optimize|first` on c.id=cn.id',
                casePath: 'regression.case7',
            });
        });

        it('should work for case 8', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|optimize|first` on cn.id=c.id',
                casePath: 'regression.case8',
            });
        });

        it('should work for case 9', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|optimize` on cn.id=c.id',
                casePath: 'regression.case9',
            });
        });

        it('should work for case 10', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join (select * from `customer-notes` where id>2) `cn|first` on cn.id=c.id',
                casePath: 'regression.case10',
            });
        });

        it('should work for case 11', async () => {
            await queryResultTester({
                queryString:
                    'select c.*,cn.*,unset(_id,c._id,c.Rentals,c.Address,cn._id) from customers c inner join `customer-notes` cn on cn.id=c.id and (cn.id>2 or cn.id<5)',
                casePath: 'regression.case11',
            });
        });
    });
    describe('left join', () => {
        it('should be able to do a left join', async () => {
            await queryResultTester({
                queryString:
                    'select *, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory` as i  on o.item=i.sku',
                casePath: 'left-join.basic',
            });
        });
        it('should be able to do a left join with an unwind', async () => {
            await queryResultTester({
                queryString:
                    'select *, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory|unwind` as i on o.item=i.sku',
                casePath: 'left-join.unwind-hint',
            });
        });

        it('should be able to do a left join with an unwind in the query', async () => {
            await queryResultTester({
                queryString:
                    'select o.id as OID,unwind(i) as inv, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory` i on o.item=i.sku',
                casePath: 'left-join.unwind-query',
            });
        });

        it('should be able to do a left join with an unwind and a case statement', async () => {
            await queryResultTester({
                queryString:
                    "select (case when o.id=1 then 'Yes' else 'No' end) as IsOne, unset(_id) from orders as o left join `inventory|unwind` as i  on o.item=i.sku",
                casePath: 'left-join.unwind-and-case',
            });
        });

        it('should be able to do a left join on special characters', async () => {
            await queryResultTester({
                queryString:
                    'select i.description,i.specialChars as iChars, o.item, o.specialChars as oChars, unset(_id) from orders as o left join `inventory|unwind` as i on o.specialChars=i.specialChars',
                casePath: 'left-join.unwind-and-special-chars',
            });
        });

        it('should be able to do a left join the other way round', async () => {
            await queryResultTester({
                queryString:
                    'select *, unset(_id,o._id,o.orderDate,i._id) from orders as o left join `inventory` as i on i.sku=o.item',
                casePath: 'left-join.basic-reversed',
            });
        });
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
                casePath: 'join-order.case1',
                expectZeroResults: true,
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
                casePath: 'n-level-join.without-unwind',
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
                casePath: 'n-level-join.with-unwind',
            });
        });
    });

    describe('inner joins', () => {
        it('should be able to do a basic inner join with 1 on condition', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            o.id,
                            o.item,
                            o.price,
                            o.customerId,
                            i.id as inventoryId,
                            i.sku,
                            i.instock,
                            unset(_id)
                        FROM orders o
                        INNER JOIN 'inventory' i on i.sku=o.item
                        `,
                casePath: 'inner-join.basic',
            });
        });
        it('should be able to do a basic inner join with 1 on condition and no aliases', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            orders.id,
                            orders.item,
                            orders.price,
                            orders.customerId,
                            inventory.id as inventoryId,
                            inventory.sku,
                            inventory.instock,
                            unset(_id)
                        FROM orders
                        INNER JOIN 'inventory' on inventory.sku=orders.item
                        `,
                casePath: 'inner-join.basic-no-alias',
            });
        });
        it('should be able to do a basic inner join with 1 on condition reversed', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            o.id,
                            o.item,
                            o.price,
                            o.customerId,
                            i.id as inventoryId,
                            i.sku,
                            i.instock,
                            unset(_id)
                        FROM orders o
                        INNER JOIN 'inventory' i on o.item=i.sku
                        `,
                casePath: 'inner-join.basic-reversed',
            });
        });
        it('should be able to do a basic inner join with 1 on condition reversed no aliases', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            orders.id,
                            orders.item,
                            orders.price,
                            orders.customerId,
                            inventory.id as inventoryId,
                            inventory.sku,
                            inventory.instock,
                            unset(_id)
                        FROM orders
                        INNER JOIN 'inventory' on orders.item=inventory.sku
                        `,
                casePath: 'inner-join.basic-reversed-no-alias',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            o.id,
                            o.item,
                            o.price,
                            o.customerId,
                            i.id as inventoryId,
                            i.sku,
                            i.instock,
                            unset(_id)
                        FROM orders o
                        INNER JOIN 'inventory' i on i.sku=o.item and i.id=o.id
                        `,
                casePath: 'inner-join.two-conditions',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions no aliases', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            orders.id,
                            orders.item,
                            orders.price,
                            orders.customerId,
                            inventory.id as inventoryId,
                            inventory.sku,
                            inventory.instock,
                            unset(_id)
                        FROM orders
                        INNER JOIN 'inventory' on inventory.sku=orders.item and inventory.id=orders.id
                        `,
                casePath: 'inner-join.two-conditions-no-aliases',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions both reversed', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            o.id,
                            o.item,
                            o.price,
                            o.customerId,
                            i.id as inventoryId,
                            i.sku,
                            i.instock,
                            unset(_id)
                        FROM orders o
                        INNER JOIN 'inventory' i on o.item=i.sku and o.id=i.id
                        `,
                casePath: 'inner-join.two-conditions-both-reversed',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions both reversed with no aliases', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            orders.id,
                            orders.item,
                            orders.price,
                            orders.customerId,
                            inventory.id as inventoryId,
                            inventory.sku,
                            inventory.instock,
                            unset(_id)
                        FROM orders
                        INNER JOIN 'inventory' on orders.item=inventory.sku and orders.id=inventory.id
                        `,
                casePath: 'inner-join.two-conditions-both-reversed-no-aliases',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions, inversed', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            o.id,
                            o.item,
                            o.price,
                            o.customerId,
                            i.id as inventoryId,
                            i.sku,
                            i.instock,
                            unset(_id)
                        FROM orders o
                        INNER JOIN 'inventory' i on i.sku=o.item and o.id=i.id
                        `,
                casePath: 'inner-join.two-conditions-inversed',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions, inversed, no aliases', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            orders.id,
                            orders.item,
                            orders.price,
                            orders.customerId,
                            inventory.id as inventoryId,
                            inventory.sku,
                            inventory.instock,
                            unset(_id)
                        FROM orders
                        INNER JOIN 'inventory' on inventory.sku=orders.item and orders.id=inventory.id
                        `,
                casePath: 'inner-join.two-conditions-inversed-no-alias',
            });
        });
        it('should be able to do a basic inner join with 2 on conditions, inversed, no aliases, no original table name', async () => {
            await queryResultTester({
                queryString: `
                        SELECT
                            orders.id,
                            orders.item,
                            orders.price,
                            orders.customerId,
                            inventory.id as inventoryId,
                            inventory.sku,
                            inventory.instock,
                            unset(_id)
                        FROM orders
                        INNER JOIN 'inventory' on inventory.sku=item and id=inventory.id
                        `,
                casePath:
                    'inner-join.two-conditions-inversed-no-alias-no-table-name',
            });
        });
    });

    describe('Deep level joins', () => {
        it('should work with the errored query from powerbi', async () => {
            const qs1 = `
            SELECT  "_"."OpportunityId" AS "basetable0.c22",
                    "_"."AnnualLicenseRevenueUSD" AS "basetable0.a0"
            FROM "public"."opportunities" "_"
            WHERE ("_"."RecordType" in ('Direct Customer',
                                       'Channel Customer'))
            AND ("_"."StageName" in ('Discovery',
                                    'Needs Analysis',
                                    'Negotiation',
                                    'Proposal',
                                    'Qualification',
                                    'Technical Discovery',
                                    'Business Discovery'))`;
            const qs2 = `SELECT "OpportunityId",
                                "Name"
                         FROM "public"."opportunitySolutions" "_Table"`;
            /**
             * semijoin1.c22 => semijoin1_c22
             * semijoin1.c151 => semijoin1_c151
             */
            const qs3 = `
                SELECT "rows"."OpportunityId" AS "semijoin1_c22",
                       "rows"."Name" AS "semijoin1_c151"
                FROM
                    (${qs2}) "rows"
                    GROUP BY "OpportunityId",
                             "Name"`;
            /**
             * $Outer => outer
             * $Inner => inner
             */
            const qs4 = `
                        SELECT "rows"."semijoin1_c151" AS "semijoin1_c151",
                                sum(cast("rows"."basetable0.a0" AS decimal)) AS "a0"
                        FROM
                        (SELECT "outer"."basetable0.a0",
                                "inner"."semijoin1_c151"
                        FROM
                            (${qs1}) "outer"
                        INNER JOIN
                            (${qs3}) "inner" ON ("outer"."basetable0.c22" = "inner"."semijoin1_c22"
                                                            OR "outer"."basetable0.c22" IS NULL
                                                            AND "inner"."semijoin1_c22" IS NULL)) "rows"
                        GROUP BY "semijoin1_c151"`;
            const fullQueryString = `;
                SELECT "_"."semijoin1_c151" AS "c151",
                       "_"."a0" AS "a0",
                       unset(_id)
                FROM (${qs4}) "_"
                WHERE NOT "_"."a0" IS NULL
                LIMIT 1000001`;
            await queryResultTester({
                queryString: fullQueryString,
                casePath: 'deep-level-joins.case1',
                outputPipeline: true,
                expectZeroResults: true,
                mode: 'write',
            });
        });
    });
});
