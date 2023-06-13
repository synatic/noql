const assert = require('assert');
const SQLParser = require('../../lib/SQLParser.js');
const supportsArraySort = false;
const {setup, disconnect, dbName} = require('../utils/mongo-client.js');

describe('Individual tests', function () {
    this.timeout(90000);
    const fileName = 'individual-test-cases';
    const mode = 'test';
    const dirname = __dirname;
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    before(function (done) {
        const run = async () => {
            try {
                const {client, db} = await setup();
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

    it('should be able to sort an array', async (done) => {
        if (!supportsArraySort) {
            return done();
        }
        const queryText =
            'SELECT id, (select * from Rentals order by `Rental Date` desc) AS OrderedRentals FROM `customers`';
        const parsedQuery = SQLParser.makeMongoAggregate(queryText);
        try {
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
            done();
        } catch (err) {
            return done(err);
        }
    });

    it('should be able to do a multipart-binary expression', async () => {
        const queryText =
            'select `Replacement Cost`, (log10(3) * floor(`Replacement Cost`) + 1) as S from films limit 1';
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

    it('should be able to do a select * computed', async () => {
        const queryText = `SELECT *, convert(id, 'string') as idConv FROM inventory`;
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

    describe('Root replacement', () => {
        it('should be able to do a basic root unwind', async () => {
            const queryText = `select unwind('Address') as \`$$ROOT\` from customers LIMIT 1`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length > 0);
                assert(results[0].Address);
                assert(results[0].City);
                assert(results[0].Country);
                assert(results[0].District);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do a basic root replacement', async () => {
            const queryText = `select 'Address' as \`$$ROOT\` from customers LIMIT 1`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length > 0);
                assert(results[0].Address);
                assert(results[0].City);
                assert(results[0].Country);
                assert(results[0].District);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
    });

    describe('sub query', () => {
        it('should result in a query type when the where is not needed to be a pipeline', async () => {
            const queryText =
                "select * from `customers` where `Address.City` in ('Japan','Pakistan') limit 10";
            const parsedQuery = SQLParser.parseSQL(queryText);
            assert(parsedQuery.type === 'query');
        });
        it('should result in an aggregate type when the where is not a simple query', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id=1)`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            assert(parsedQuery.type === 'aggregate');
        });
        // join
        // group by
        it('should be able to support subquery syntax', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id=1)`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);

            try {
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 2);
                assert(results[0].id === 1);
                assert(results[0].item === 'almonds');
                assert(results[0].price === 12);
                assert(results[0].quantity === 2);
                assert(results[0].customerId === 1);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do n level sub queries', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) order by id`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 3);
                assert(results[0].id === 1);
                assert(results[0].item === 'almonds');
                assert(results[0].price === 12);
                assert(results[0].quantity === 2);
                assert(results[0].customerId === 1);
                assert(results[1].id === 2);
                assert(results[1].item === 'pecans');
                assert(results[1].price === 20);
                assert(results[1].quantity === 1);
                assert(results[1].customerId === 2);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do n level sub queries that are selects', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id in (select id from orders))`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                let results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline);
                results = await results.toArray();
                assert(results.length === 3);
                assert(results[0].id === 1);
                assert(results[0].item === 'almonds');
                assert(results[0].price === 12);
                assert(results[0].quantity === 2);
                assert(results[0].customerId === 1);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to support table aliases', async () => {
            const queryText = `select c.item as Product from orders c where item in (select sku from inventory where id in (select id from orders))`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 3);
                assert(results[0].Product === 'almonds');
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to support it with a sort', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) ORDER BY id DESC`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 3);
                assert(results[0].item === 'almonds');
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to support it with a limit', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) ORDER BY id DESC limit 1`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 1);
                assert(results[0].item === 'almonds');
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to support multiple where clauses', async () => {
            const queryText = `select * from orders where item in (select sku from inventory where id in (1,4)) and price=12`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 2);
                assert(results[0].item === 'almonds');
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to support multiple where clauses in any order', async () => {
            const queryText = `select * from orders where price=12 and item in (select sku from inventory where id in (1,4))`;
            try {
                const parsedQuery = SQLParser.makeMongoAggregate(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 2);
                assert(results[0].item === 'almonds');
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
    });

    describe('count', () => {
        it('should be able to do a count * with a group by', async () => {
            const queryText =
                'select item, count(1) as countVal from `orders` where id in (1,2,4) group by `item`';
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 2);
                const pecans = results.find((r) => r.item === 'pecans');
                assert(pecans);
                assert(pecans.countVal === 1);
                const almonds = results.find((r) => r.item === 'almonds');
                assert(almonds);
                assert(almonds.countVal === 2);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do a count * without a group by', async () => {
            const queryText = `select count(1) as countVal from orders where id in (1,2,4)`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 1);
                assert(results[0].countVal === 3);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do a count * without a group by with a where item=almonds', async () => {
            const queryText = `select count(1) as countVal from orders where item=almonds`;
            const parsedQuery = SQLParser.parseSQL(queryText);
            try {
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();
                assert(results.length === 1);
                assert(results[0].countVal === 2);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
    });

    describe('unset', () => {
        it('should be able to do a basic unset in a query', async () => {
            const queryText =
                'SELECT unset(_id),id,item FROM `orders` where id in (1,2,4)';
            try {
                const parsedQuery = SQLParser.parseSQL(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collection)
                    .find(parsedQuery.query || null, {
                        projection: parsedQuery.projection,
                    })
                    .sort()
                    .toArray();

                assert(results.length === 3);
                assert(results[0]._id === undefined);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
        it('should be able to do a basic unset in an aggregation', async () => {
            const queryText =
                'SELECT unset(_id),id,item FROM orders where item in (select sku from inventory where id=1)';
            try {
                const parsedQuery = SQLParser.parseSQL(queryText);
                const results = await mongoClient
                    .db(dbName)
                    .collection(parsedQuery.collections[0])
                    .aggregate(parsedQuery.pipeline)
                    .toArray();

                assert(results.length === 2);
                assert(results[0]._id === undefined);
                return;
            } catch (err) {
                console.error(err);
                throw err;
            }
        });
    });

    describe('Union', () => {
        describe('Union all', () => {
            it('should be able to do a basic union all', async () => {
                const queryText =
                    'SELECT unset(_id), o.id, o.item as product FROM `orders` o where id in (1,2,4) UNION ALL select unset(_id),i.id, i.sku as product from inventory i';
                try {
                    const parsedQuery = SQLParser.parseSQL(queryText);
                    const results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();
                    assert(results.length === 8);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
        describe('Union', () => {
            it('should be able to do a basic union', async () => {
                const queryText =
                    'SELECT unset(_id), o.id, o.item as product FROM `orders` o where id in (1,2,4) UNION select unset(_id),i.id, i.sku as product from inventory i';
                try {
                    const parsedQuery = SQLParser.parseSQL(queryText);
                    const results = await mongoClient
                        .db(dbName)
                        .collection(parsedQuery.collections[0])
                        .aggregate(parsedQuery.pipeline)
                        .toArray();
                    assert(results.length === 7);
                    return;
                } catch (err) {
                    console.error(err);
                    throw err;
                }
            });
        });
    });

    describe('table alias', () => {
        it.skip("should allow you to alias a table without an 'as' statement", async () => {
            const queryText =
                'SELECT ord.specialChars FROM `orders` ord limit 4';
            const parsedQuery = SQLParser.parseSQL(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collection)
                .find(parsedQuery.query || null, {
                    projection: parsedQuery.projection,
                })
                .limit(parsedQuery.limit)
                .toArray();
            assert(results.length === 1);
            assert(results[0].specialChars);
        });
    });

    describe('Case statement with functions', () => {
        it.skip('Should work when the case statement does not have backticks', async () => {
            const queryText = `
            SELECT
                o.item,
                o.notes,
                o.specialChars,
                (case
                    WHEN o.item = "pecans"
                        THEN 'Y'
                    ELSE 'N'
                END)
                as OriginalExecutive
            FROM orders o
            where id=2`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const pipeline = [
                {$match: {id: {$eq: 2}}},
                {$project: {o: '$$ROOT'}},
                {
                    $project: {
                        item: '$o.item',
                        notes: '$o.notes',
                        specialChars: '$o.specialChars',
                        OriginalExecutive: {
                            $switch: {
                                branches: [
                                    {
                                        case: {$eq: ['$o.item', 'pecans']},
                                        then: 'Y',
                                    },
                                ],
                                default: {$literal: 'N'},
                            },
                        },
                    },
                },
            ];
            assert(
                parsedQuery.pipeline[2].$project.OriginalExecutive.$switch
                    .branches[0].case.$eq[0] === '$o.item'
            );
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
            assert(results[0].OriginalExecutive === 'Y');
        });
    });

    describe('select in select', () => {
        it('Should work without an order by on a local field', async () => {
            const queryText = `
            SELECT Address,
                SELECT \`Film Title\` from Inventory where inventoryId=1 as latestFilm
            FROM stores
            where _id=1`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
        });
        it('Should work without an order by on another table', async () => {
            const queryText = `SELECT c.*,cn.* FROM customers c inner join (select * from \`customer-notes\` where id > 2) cn on cn.id=c.id`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
        });
        it('Should work with an order by on another table', async () => {
            const queryText = `SELECT c.*,cn.*
                FROM customers c
                inner join (select * from \`customer-notes\` ORDER BY id DESC limit 1) cn on cn.id=c.id`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
            assert(results[0].cn[0].id === 5);
        });
        it('Should do the inner select query correctly', async () => {
            const queryText =
                'select `PolId`,max(`ChangedDate`) as ChangedDate from `ams360-powerbi-policytranpremium` group by `PolId`';
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
            assert(results[0].ChangedDate);
        });

        it('Should do the full select query correctly', async () => {
            const queryText = `
            SELECT pol.*
                ,prem.*
            FROM \`ams360-powerbi-basicpolinfo\` pol
            left join (select \`PolId\`,max(\`ChangedDate\`) as ChangedDate from \`ams360-powerbi-policytranpremium\` group by \`PolId\`) prem
                on prem.PolId = pol.PolId`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);

            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                // .aggregate(manualPipeline)
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results);
            assert(results[0].prem[0].ChangedDate);
        });
        it('Should not matter which order you use for the on clause', async () => {
            const queryText1 = `SELECT c.*,cn.* FROM customers c inner join (select * from \`customer-notes\` where id > 2) cn on cn.id=c.id`;
            const parsedQuery1 = SQLParser.makeMongoAggregate(queryText1);
            const results1 = await mongoClient
                .db(dbName)
                .collection(parsedQuery1.collections[0])
                .aggregate(parsedQuery1.pipeline)
                .toArray();
            const queryText2 = `SELECT c.*,cn.* FROM customers c inner join (select * from \`customer-notes\` where id > 2) cn on c.id=cn.id`;
            const parsedQuery2 = SQLParser.makeMongoAggregate(queryText2);
            const results2 = await mongoClient
                .db(dbName)
                .collection(parsedQuery2.collections[0])
                .aggregate(parsedQuery2.pipeline)
                .toArray();
            assert.deepEqual(results1, results2);
        });
        it('Should work with reverse order of the on', async () => {
            const queryText = `SELECT c.*,cn.* FROM customers c inner join (select * from \`customer-notes\` where id > 2) cn on c.id=cn.id`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
        });
    });

    describe('literals', () => {
        it('Should use the literal value if it is not a table alias', async () => {
            const queryText = `select "Name" as Id, Description from "films"`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
            assert(results[0].Id === 'Name');
        });

        it('Should use the alias for the table if one is provided', async () => {
            const queryText = `
            select "___Table.id" as "TableId",
                "___Table.item" as "TableItem"
            from "orders" "___Table"`;
            // order by "___Ordered.id"
            // limit 4096`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText);
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
            assert(results[0].TableId === 1);
        });

        it('should work for column names in double quotes - postgresql.', async () => {
            const queryText = `
            select "AccountType",
                "CalcDate"
            from "orders"`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText, {
                database: 'PostgresQL',
            });
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
            assert(results.length === 4);
        });
    });

    describe('not in & in', () => {
        it('should work for in subclause', async () => {
            const queryText = `
                        select distinct item
                        from "orders"
                        where id in (select id from orders where item=almonds)`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText, {
                database: 'PostgresQL',
            });
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
            assert(results.length === 1);
            assert(results[0].item === 'almonds');
        });
        it('should throw an error when the column is missing', async () => {
            const queryText = `
                        select item
                        from "orders"
                        where id not in (select * from orders where item=almonds)`;
            assert.throws(() => {
                SQLParser.makeMongoAggregate(queryText, {
                    database: 'PostgresQL',
                });
            });
        });
        it('should throw an error when more than 1 column is specified', async () => {
            const queryText = `
                        select item
                        from "orders"
                        where id not in (select item,id from orders where item=almonds)`;
            assert.throws(() => {
                SQLParser.makeMongoAggregate(queryText, {
                    database: 'PostgresQL',
                });
            });
        });
        it('should work for a valid not in query', async () => {
            const queryText = `
                        select item
                        from "orders"
                        where id not in (select id from orders where item=almonds)`;
            const parsedQuery = SQLParser.makeMongoAggregate(queryText, {
                database: 'PostgresQL',
            });
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
            assert(results.length === 2);
            assert(results[0].item === 'pecans');
            assert(results[1].item === 'potatoes');
        });
        it('should work for the example query in bug from production', async () => {
            const queryText = `
            SELECT  "_"."StageName",
                    "_"."ICP",
                    "_"."a0"
            FROM
            (SELECT "rows"."StageName" AS "StageName",
                    "rows"."ICP" AS "ICP",
                    sum(cast("rows"."AnnualLicenseRevenueUSD" AS decimal)) AS "a0"
                FROM
                (SELECT "_"."_id",
                        "_"."OpportunityId",
                        "_"."CloseYear",
                        "_"."CloseMonth",
                        "_"."AnnualLicenseRevenueUSD",
                        "_"."DaysSinceQualification",
                        "_"."StageName",
                        "_"."AccountRecordType",
                        "_"."OpportunityRecordType",
                        "_"."RecordType",
                        "_"."SaleType",
                        "_"."DateSort",
                        "_"."DateID",
                        "_"."CalcDate",
                        "_"."Industry",
                        "_"."AddressCity",
                        "_"."AddressCountry",
                        "_"."OpportunityName",
                        "_"."AccountName",
                        "_"."LeadSource",
                        "_"."NumberOfEmployees",
                        "_"."ICP"
                FROM "public"."opportunities" "_"
                WHERE (("_"."StageName" in ('Business Discovery',
                                            'Discovery',
                                            'Needs Analysis',
                                            'Negotiation',
                                            'Proposal',
                                            'Qualification',
                                            'Technical Discovery'))
                        AND cast("_"."CloseYear" AS decimal) = cast(2023 AS decimal))
                    AND (NOT ("_"."RecordType" in ('Channel Customer'))
                        OR "_"."RecordType" IS NULL) ) "rows"
                GROUP BY "StageName",
                        "ICP") "_"
            WHERE NOT "_"."a0" IS NULL
            LIMIT 1000001`;

            const parsedQuery = SQLParser.makeMongoAggregate(queryText, {
                database: 'PostgresQL',
            });
            const results = await mongoClient
                .db(dbName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
            assert(results.length);
            assert(results.length === 1);
        });
    });
});
