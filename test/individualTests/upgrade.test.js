const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');
const {setup, disconnect, dbName} = require('../utils/mongo-client.js');
const $check = require('check-types');
const {buildQueryResultTester} = require('../utils/query-tester/index.js');

describe('node-sql-parser upgrade tests', function () {
    this.timeout(90000);
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    /** @type {import("../utils/query-tester/types.js").QueryResultTester} */
    let queryResultTester;
    const mode = 'test';
    const dirName = __dirname;
    const fileName = 'upgrade';

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

    it('should work after upgrading', async () => {
        const queryText =
            "select SUM_ARRAY((select SUM_ARRAY(`Payments`,'Amount') as total from `Rentals`),'total') as t from customers";
        try {
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
        } catch (err) {
            console.error(err);
            throw err;
        }
    });
    describe('Not Like', () => {
        describe('query part', () => {
            it('should work with like', async () => {
                const queryText =
                    "select `item` as productName from `orders` where `item` Like 'alm%'";
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    const results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();
                    assert(results);
                    assert(results.length === 3);
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should work with not like', async () => {
                const queryText =
                    "select `item` as productName from `orders` where `item` Not Like 'alm%'";
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    const results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();
                    assert(results);
                    assert(results.length === 2);
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
        describe('filter condition', () => {
            it('should work with like', async () => {
                const queryText =
                    "select id,`First Name`,`Last Name`,(select * from Rentals where `Film Title` like 'MUSKETEERS%') as rentalsArr from `customers` where id=1";
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    const results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();
                    assert(results);
                    assert(results.length === 1);
                    assert(results[0].rentalsArr.length === 1);
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
            it('should work with not like', async () => {
                const queryText =
                    "select id,`First Name`,`Last Name`,(select * from Rentals where `Film Title` not like 'MUSKETEERS%') as rentalsArr from `customers` where id=1";
                try {
                    const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                    const results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();
                    assert(results);
                    assert(results.length === 1);
                    assert(results[0].rentalsArr.length === 31);
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
    });
    describe('CURRENT_DATE', () => {
        it('should return the current date in a select', async () => {
            const queryText = `
            SELECT CURRENT_DATE() as currDate
            FROM orders`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results);
                assert(results.length === 5);
                assert($check.date(results[0].currDate));
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should return the current date in a select for alias getdate', async () => {
            const queryText = `
            SELECT getdate() as currDate
            FROM orders`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results);
                assert(results.length === 5);
                assert($check.date(results[0].currDate));
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should allow other functions on current date in a select', async () => {
            const queryText = `
            SELECT YEAR(CURRENT_DATE()) as currDate
            FROM orders`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results);
                assert(results.length === 5);
                assert($check.number(results[0].currDate));
                assert(results[0].currDate.toString().length === 4);
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should allow use of current date in a where', async () => {
            const queryText = `
            SELECT *
            FROM orders
            WHERE YEAR(orderDate) < YEAR(CURRENT_DATE())`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results);
                assert(results.length === 5);
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should get the correct collection name', async () => {
            const queryString = 'select * from `xxx`';
            const options = {};
            const parsedAST = SQLParser.parseSQLtoAST(queryString, options);
            const parsedQuery = SQLParser.makeMongoAggregate(
                parsedAST,
                options
            );
            assert(parsedQuery.collections);
        });
    });
    describe('Group by split', () => {
        it('should perform a union with 2 sub selects', async () => {
            const queryString = `
            SELECT  o.customerId,
                    o.quantity,
                    o.Category,
                    o.priceZAR
            FROM (
                SELECT  o.customerId,
                        o.quantity,
                        CASE
                            WHEN o.item = 'almonds' THEN 'Fats'
                            WHEN o.item = 'potatoes' THEN 'Carbs'
                            WHEN o.item = 'pecans' THEN 'Fats'
                            ELSE o.item
                        END AS 'Category',
                        ROUND(o.priceZAR,2) as priceZAR
                FROM (
                    SELECT  customerId,
                            quantity,
                            item,
                            sum(ROUND(price * 19.6,0)) as priceZAR
                    FROM orders
                    GROUP BY customerId
                    ORDER BY customerId ASC
                ) o
                WHERE customerId = 1
                UNION
                SELECT  o.customerId,
                        o.quantity,
                        CASE
                            WHEN o.notes = 'testing' THEN 'Test Order'
                            ELSE 'Real Order'
                        END AS 'Category',
                        ROUND(o.priceZAR,2) as priceZAR
                FROM (
                    SELECT  customerId,
                            quantity,
                            item,
                            sum(ROUND(price * 19.6,0)) as priceZAR
                    FROM orders
                    GROUP BY customerId
                    ORDER BY customerId ASC
                ) o
                WHERE customerId = 1
            ) o
            --TODO Bug, o.Category doesn't work
            ORDER BY Category ASC
            `;
            await queryResultTester({
                queryString: queryString,
                casePath: 'bugfix.join-fn.case1',
                mode,
                outputPipeline: false,
            });
        });
    });
    describe('IIF', () => {
        it('should work', async () => {
            const queryString = `
                SELECT  item,
                        IIF(price>10,'double digits','single digits') as digits,
                        unset(_id)
                FROM orders
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.iif.case1',
                mode,
                outputPipeline: false,
            });
        });
    });
    describe('date_add', () => {
        it('should work without a timezone', async () => {
            const queryString = `
                SELECT  id,
                        item,
                        orderDate as od1,
                        date_add(orderDate,'hour',2) as od2,
                        unset(_id)
                FROM orders
                WHERE id=2
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.dateAdd.case1',
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
        it('should work with a timezone', async () => {
            const queryString = `
                SELECT  id,
                        item,
                        orderDate as od1,
                        date_add(orderDate,'hour',2,"America/New_York") as od2,
                        unset(_id)
                FROM orders
                WHERE id=2
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.dateAdd.case2',
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
    });
    describe('date_subtract', () => {
        it('should work without a timezone', async () => {
            const queryString = `
                SELECT  id,
                        item,
                        orderDate as od1,
                        date_subtract(orderDate,'hour',2) as od2,
                        unset(_id)
                FROM orders
                WHERE id=2
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.dateSubtract.case1',
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
        it('should work with a timezone', async () => {
            const queryString = `
                SELECT  id,
                        item,
                        orderDate as od1,
                        date_subtract(orderDate,'hour',2,"America/New_York") as od2,
                        unset(_id)
                FROM orders
                WHERE id=2
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.dateSubtract.case2',
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
    });
    describe('date_diff', () => {
        it('should work without a timezone', async () => {
            const queryString = `
                SELECT  id,
                        item,
                        orderDate,
                        getdate() as now,
                        date_diff(orderDate, date_add(orderDate,'day',2),'day') as diff,
                        unset(_id)
                FROM orders
                WHERE id=2
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.dateDiff.case1',
                mode,
                outputPipeline: false,
                ignoreDateValues: true,
            });
        });
    });
    describe('left', () => {
        it('should work without a timezone', async () => {
            const queryString = `
                SELECT  id,
                        left(item,2) as firstTwoChars,
                        unset(_id)
                FROM orders
                WHERE id=2
                LIMIT 1`;
            await queryResultTester({
                queryString: queryString,
                casePath: 'new.left.case1',
                mode,
                outputPipeline: false,
            });
        });
    });
    describe('PIVOT and UNPIVOT', () => {
        describe('PIVOT', () => {
            it('should pivot DaysToManufacture to columns with one aggregation function', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([avg(StandardCost) as AverageCost],DaysToManufacture,[0,1,2,3,4])'
                `;

                await queryResultTester({
                    queryString: queryText,
                    casePath: 'pivot.case1',
                    mode,
                    outputPipeline: false,
                });
            });
            it('should pivot DaysToManufacture to columns with two aggregation functions, one with an as', async () => {
                const queryText = `
                    SELECT 'Costs' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([avg(StandardCost), max(StandardCost) as MaxCost],DaysToManufacture,[0,1,2,3,4])'
                `;

                await queryResultTester({
                    queryString: queryText,
                    casePath: 'pivot.case2',
                    mode,
                    outputPipeline: false,
                });
            });
            const formatErrorSearchString = 'operation had the wrong format';
            it('should throw an error if the pivot is en empty array', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([ ],DaysToManufacture,[0,1,2,3,4])'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot is not provided', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot(,DaysToManufacture,[0,1,2,3,4])'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot has only 2 arguments', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot(DaysToManufacture,[0,1,2,3,4])'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot has only 2 arguments and the first is empty', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot(,[0,1,2,3,4])'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot has only 1 argument', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([0,1,2,3,4])'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot is missing the for argument', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([avg(StandardCost) as AverageCost], [0,1,2,3,4])'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot is missing the columns argument', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([avg(StandardCost) as AverageCost],DaysToManufacture'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot has an empty columns argument', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([avg(StandardCost) as AverageCost],DaysToManufacture,[]'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
            it('should throw an error if the pivot has an empty columns argument', async () => {
                const queryText = `
                    SELECT 'AverageCost' as CostSortedByProductionDays,
                            "0",
                            "1",
                            "2",
                            "3",
                            "4"
                    FROM (
                        SELECT DaysToManufacture,
                               StandardCost
                        FROM Production_Product
                        GROUP BY DaysToManufacture, StandardCost
                        ORDER BY DaysToManufacture, StandardCost
                    ) 'pvt|pivot([avg(StandardCost) as AverageCost],DaysToManufacture,[,,]'
                `;
                try {
                    await queryResultTester({
                        queryString: queryText,
                        casePath: 'pivot.case1',
                        mode,
                        outputPipeline: false,
                    });
                    assert.fail('should not pass');
                } catch (err) {
                    assert(err.message.indexOf(formatErrorSearchString) !== -1);
                }
            });
        });

        describe('UNPIVOT', () => {
            it('should unpivot employee columns to rows', async () => {
                const queryText = `
                    SELECT VendorID, Employee, Orders
                    FROM (
                        SELECT VendorID, Emp1, Emp2, Emp3, Emp4, Emp5, unset(_id)
                        FROM pvt
                    ) 'unpvt|unpivot(Orders,Employee,[Emp1, Emp2, Emp3, Emp4, Emp5])'
                    ORDER BY VendorID, Employee
                `;

                await queryResultTester({
                    queryString: queryText,
                    casePath: 'unpivot.case1',
                    mode,
                    outputPipeline: false,
                });
            });
            it('should support multiple unpivots', async () => {
                // See https://dba.stackexchange.com/a/222745
                const queryText = `
                    SELECT SalesID,
                    ROW_NUMBER() OVER (
                            ORDER BY OrderName
                        ) OrderNum,
                    OrderName,
                    OrderDate,
                    OrderAmt
                    FROM (
                        SELECT SalesID, Order1Name, Order2Name, Order1Date, Order2Date, Order1Amt, Order2Amt, unset(_id)
                        FROM multiple-unpivot
                    ) 'unpvt|unpivot(OrderName,OrderNames,[Order1Name, Order2Name])|unpivot(OrderDate,OrderDates,[Order1Date, Order2Date])|unpivot(OrderAmt,OrderAmts,[Order1Amt, Order2Amt])'
                `;
                await queryResultTester({
                    queryString: queryText,
                    casePath: 'unpivot.case2',
                    mode,
                    outputPipeline: false,
                });
            });
        });
    });
});
