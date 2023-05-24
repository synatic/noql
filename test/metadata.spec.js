const {getResultSchema} = require('../lib/metadata');
// @ts-ignore
const {parseSQLtoAST} = require('../lib/SQLParser');
const {
    ColumnDoesNotExistError,
    TableDoesNotExistError,
} = require('../lib/errors');
const assert = require('assert');
const {setup, disconnect} = require('./mongo-client');

describe('metadata', () => {
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    /** @type {import('mongodb').Db} */
    let database;
    before(function (done) {
        this.timeout(90000);
        const run = async () => {
            try {
                const {client, db} = await setup();
                mongoClient = client;
                database = db;
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

    describe('no joins', () => {
        it('should be able to generate a schema for a * query', async () => {
            const queryString = 'select * from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 9);
            const _idColumn = schema[7];
            assert.deepStrictEqual(_idColumn.path, '_id');
        });
        it('should be able to generate a schema for a specific field', async () => {
            const queryString = 'select _id from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 1);
            const _idField = schema[0];
            assert.ok(!_idField.as);
            assert.deepStrictEqual(_idField.collectionName, 'orders');
            assert.deepStrictEqual(_idField.format, 'mongoid');
            assert.deepStrictEqual(_idField.isArray, false);
            assert.deepStrictEqual(_idField.order, 0);
            assert.deepStrictEqual(_idField.path, '_id');
            assert.deepStrictEqual(_idField.required, false);
            assert.deepStrictEqual(_idField.type, 'string');
        });
        // it('should include the _id column even if not selected', async () => {
        //     const queryString = 'select id from orders';
        //     const ast = parseSQLtoAST(queryString, {
        //         database: 'PostgresQL',
        //     });
        //     const schema = await getResultSchema(ast, queryString, getSchema);
        //     assert.deepStrictEqual(schema.length, 2);
        //     const _idField = schema[0];
        //     assert.ok(!_idField.as);
        //     assert.deepStrictEqual(_idField.collectionName, 'orders');
        //     assert.deepStrictEqual(_idField.format, 'mongoid');
        //     assert.deepStrictEqual(_idField.isArray, false);
        //     assert.deepStrictEqual(_idField.order, 0);
        //     assert.deepStrictEqual(_idField.path, '_id');
        //     assert.deepStrictEqual(_idField.required, false);
        //     assert.deepStrictEqual(_idField.type, 'string');
        // });
        // it('should not include the _id if it is explicitly unset', async () => {
        //     const queryString = 'select id, unset(_id) from orders';
        //     const ast = parseSQLtoAST(queryString, {
        //         database: 'PostgresQL',
        //     });
        //     const schema = await getResultSchema(ast, queryString, getSchema);
        //     assert.deepStrictEqual(schema.length, 1);
        //     const _idField = schema[0];
        //     assert.ok(!_idField.as);
        //     assert.deepStrictEqual(_idField.collectionName, 'orders');
        //     assert.deepStrictEqual(_idField.format, 'mongoid');
        //     assert.deepStrictEqual(_idField.isArray, false);
        //     assert.deepStrictEqual(_idField.order, 0);
        //     assert.deepStrictEqual(_idField.path, '_id');
        //     assert.deepStrictEqual(_idField.required, false);
        //     assert.deepStrictEqual(_idField.type, 'string');
        // });
        // TODO TEST for table.*, table2.*
        it('should be able to generate a schema for a * query with field specified', async () => {
            const queryString = 'select _id, * from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 10);
            const _idColumn = schema[8];
            assert.deepStrictEqual(_idColumn.path, '_id');
        });
        it('should be able to generate a schema for multiple specific fields', async () => {
            const queryString = 'select _id,item from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 2);
        });
        it('should be able to generate a schema for multiple specific fields with a select *', async () => {
            const queryString = 'select _id,item,* from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 11);
        });
        it('should throw an error if there is no such column', async () => {
            const queryString = 'select _id,bob from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });

            try {
                await getResultSchema(ast, queryString, getSchema);
            } catch (err) {
                assert(err instanceof ColumnDoesNotExistError);
                return;
            }
            assert.fail('did not throw');
        });
        it('should throw an error if there is no such table with * specified', async () => {
            const queryString = 'select * from orderss';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            try {
                await getResultSchema(ast, queryString, getSchema);
            } catch (err) {
                assert(err instanceof TableDoesNotExistError);
                return;
            }
            assert.fail('did not throw');
        });
        it('should throw an error if there is no such table with columns specified', async () => {
            const queryString = 'select _id from orderss';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            try {
                await getResultSchema(ast, queryString, getSchema);
            } catch (err) {
                assert(err instanceof TableDoesNotExistError);
                return;
            }
            assert.fail('did not throw');
        });
        it('should be able to generate a schema for a specific field with an alias', async () => {
            const queryString = 'select _id as mongoId from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 1);
            const _idField = schema[0];
            assert.deepStrictEqual(_idField.as, 'mongoId');
            assert.deepStrictEqual(_idField.collectionName, 'orders');
            assert.deepStrictEqual(_idField.format, 'mongoid');
            assert.deepStrictEqual(_idField.isArray, false);
            assert.deepStrictEqual(_idField.order, 0);
            assert.deepStrictEqual(_idField.path, '_id');
            assert.deepStrictEqual(_idField.required, false);
            assert.deepStrictEqual(_idField.type, 'string');
        });
        describe('functions', () => {
            it('trim', async () => {
                const queryString =
                    'select _id, TRIM(`item`) as trimmedItem from orders';
                const ast = parseSQLtoAST(queryString, {
                    database: 'PostgresQL',
                });
                const schema = await getResultSchema(
                    ast,
                    queryString,
                    getSchema
                );
                assert.deepStrictEqual(schema.length, 2);
                const trimmedItem = schema[1];
                assert.deepStrictEqual(trimmedItem.as, 'trimmedItem');
                assert.deepStrictEqual(trimmedItem.type, 'string');
                assert.deepStrictEqual(trimmedItem.isArray, false);
            });
            describe('firstn', () => {
                it('object result, no field set', async () => {
                    const queryString =
                        'select _id, firstn(1) as itemSplit from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
                it('object result, field set', async () => {
                    const queryString =
                        'select _id, firstn(1,"rentals") as itemSplit from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
                it('number result, field set', async () => {
                    const queryString =
                        'select _id, firstn(1,"numberArray") as itemSplit from function-test-data';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                    assert.deepStrictEqual(itemSplit.type, 'integer');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            it('split', async () => {
                const queryString =
                    'select _id, SPLIT(`item`,",") as itemSplit from orders';
                const ast = parseSQLtoAST(queryString, {
                    database: 'PostgresQL',
                });
                const schema = await getResultSchema(
                    ast,
                    queryString,
                    getSchema
                );
                assert.deepStrictEqual(schema.length, 2);
                const itemSplit = schema[1];
                assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                assert.deepStrictEqual(itemSplit.type, 'string');
                assert.deepStrictEqual(itemSplit.isArray, true);
            });
            describe('lastn', () => {
                it('object result, no field set', async () => {
                    const queryString =
                        'select _id, lastn(1) as itemSplit from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
                it('object result, field set', async () => {
                    const queryString =
                        'select _id, lastn(1,"rentals") as itemSplit from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
                it('number result, field set', async () => {
                    const queryString =
                        'select _id, lastn(1,"numberArray") as itemSplit from function-test-data';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'itemSplit');
                    assert.deepStrictEqual(itemSplit.type, 'integer');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('convert', () => {
                it('to int', async () => {
                    const queryString =
                        'select _id, convert(Phone,"int") as converted from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'converted');
                    assert.deepStrictEqual(itemSplit.type, 'number');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
            });
            describe('ifNull', async () => {
                it('should work if a static value is set', async () => {
                    const queryString =
                        'select _id, ifNull(Phone,"555-555-5555") as nullGuard from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'nullGuard');
                    assert.deepStrictEqual(itemSplit.type, 'string');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
                it('should work if a select value is set', async () => {
                    const queryString =
                        'select _id, ifNull(Phone,(SELECT "A" as val)) as nullGuard from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'nullGuard');
                    assert.deepStrictEqual(itemSplit.type, 'string');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
            });
            describe('first_in_array', () => {
                it('should work for a basic first_in_array', async () => {
                    const queryString =
                        'select _id,FIRST_IN_ARRAY(`Rentals`) as Rental from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'Rental');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
            });
            describe('last_in_array', () => {
                it('should work for a basic last_in_array', async () => {
                    const queryString =
                        'select _id,last_in_array(`Rentals`) as Rental from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'Rental');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
            });
            describe('reverse_array', () => {
                it('should work for a basic reverse_array', async () => {
                    const queryString =
                        'select _id,reverse_array(`Rentals`) as Rental from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'Rental');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('array_elem_at', () => {
                it('should work for a basic array_elem_at', async () => {
                    const queryString =
                        'select _id,array_elem_at(`Rentals`,0) as Rental from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'Rental');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
            });
            describe('array_range', () => {
                it('should work for a basic array_elem_at', async () => {
                    const queryString =
                        'select _id,ARRAY_RANGE(0,10,2) as test from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'number');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('zip_array', () => {
                it('should work for a basic zip_array', async () => {
                    const queryString =
                        'select _id, ZIP_ARRAY((select `Film Title` as "$$ROOT" from `Rentals`),ARRAY_RANGE(0,10,2)) as test from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('concat_arrays', () => {
                it('should work for a basic concat_arrays', async () => {
                    const queryString =
                        'select _id, concat_arrays((select `Film Title` as "$$ROOT" from `Rentals`),ARRAY_RANGE(0,10,2)) as test from customers';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('object_to_array', () => {
                it('should work for a basic object_to_array', async () => {
                    const queryString =
                        'select id,OBJECT_TO_ARRAY(`Address`) as test from `customers`';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('array_to_object', () => {
                it('should work for a basic array_to_object', async () => {
                    const queryString =
                        'select id,ARRAY_TO_OBJECT(PARSE_JSON(\'[{"k":"val","v":1}]\')) as test from `customers`';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, false);
                });
            });
            describe('set_union', () => {
                it('should work for a basic array_to_object', async () => {
                    const queryString =
                        "select id,SET_UNION((select filmId as '$$ROOT' from `Rentals`),PARSE_JSON('[ 1,2,3,4] ')) as test from `customers`";
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 2);
                    const itemSplit = schema[1];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            //
        });

        // functions
        // aliases
        // nested fields
        /**
         * multiple functions "select id,ARRAY_TO_OBJECT(PARSE_JSON('[{\"k\":\"val\",\"v\":1}]')) as test from `customers`"
         * replacing root complex "select (select id,`First Name` as Name) as t1, (select id,`Last Name` as LastName) as t2,MERGE_OBJECTS(t1,t2) as `$$ROOT`
         * gets field select SPLIT(`First Name`,',') as exprVal from `customers`
         * $unset
         */
    });

    describe('with joins', () => {
        it('should be able to generate a schema for a * query', async () => {
            const queryString =
                'select * from orders inner join `inventory` on sku=item';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 15);
            const _idColumn = schema[7];
            assert.deepStrictEqual(_idColumn.path, '_id');
        });
        // it('should return the first tables column when there are two columns with the same name', async () => {
        //     const queryString =
        //         'select id from orders inner join `inventory` on sku=item';
        //     const ast = parseSQLtoAST(queryString, {
        //         database: 'PostgresQL',
        //     });
        //     const schema = await getResultSchema(ast, queryString, getSchema);
        //     assert.deepStrictEqual(schema.length, 1);
        //     const _idColumn = schema[0];
        //     assert.deepStrictEqual(_idColumn.path, 'id');
        //     assert.deepStrictEqual(_idColumn.collectionName, 'orders');
        // });
    });
    async function getSchema(collectionName) {
        const doc = await database
            .collection('schemas')
            .findOne({collectionName});
        return doc.flattenedSchema;
    }
});
