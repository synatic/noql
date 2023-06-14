const {getResultSchema} = require('../lib/metadata');
// @ts-ignore
const {parseSQLtoAST, makeMongoAggregate} = require('../lib/SQLParser');
const {
    ColumnDoesNotExistError,
    TableDoesNotExistError,
} = require('../lib/errors');
const assert = require('assert');
const {setup, disconnect} = require('./utils/mongo-client');
// @ts-ignore
const {_jsonSchemaTypeMapping} = require('../lib/MongoFunctions');

/**
 * @typedef {import('../lib/types').ResultSchema} ResultSchema
 * @typedef {import('mongodb').Document} Document
 */
describe('metadata', () => {
    /** @type {import('mongodb').MongoClient} */
    let mongoClient;
    /** @type {import('mongodb').Db} */
    let database;
    /** @type {string} */
    let databaseName;
    before(function (done) {
        this.timeout(90000);
        const run = async () => {
            try {
                const {client, db, dbName} = await setup();
                mongoClient = client;
                database = db;
                databaseName = dbName;
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
        it('should include the _id column even if not selected', async () => {
            const queryString = 'select id from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 2);
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
        it('should not include the _id if it is explicitly unset', async () => {
            const queryString = 'select id, unset(_id) from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 1);
            const _idField = schema.find((s) => s.path === '_id');
            assert.ok(!_idField);
        });
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
        it('should be able to do a replace root query', async () => {
            const queryString =
                'select t as `$$ROOT` from (select id, `First Name` from customers limit 1) as t';
            const {schema, results} = await getEstimatedSchemaAndResults(
                queryString
            );
            compareSchemaWithResults(schema, results);
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
                it('should work with a query that hard codes a null and returns an object', async () => {
                    const queryString =
                        "select IFNULL(NULL,(select 'a' as val,1 as num)) as `conv` from `customers` limit 1";
                    const {schema, results} =
                        await getEstimatedSchemaAndResults(queryString);
                    compareSchemaWithResults(schema, results);
                });
                it('should work with a query that hard codes a null and returns an single string in a query', async () => {
                    const queryString =
                        "select IFNULL(NULL,'aaaa') as `conv` from `customers` limit 1";
                    const {schema, results} =
                        await getEstimatedSchemaAndResults(queryString);
                    compareSchemaWithResults(schema, results);
                });
                it('should work with a query that hard codes a null and returns an single number in a query', async () => {
                    const queryString =
                        'select IFNULL(NULL,1) as `conv` from `customers` limit 1';
                    const {schema, results} =
                        await getEstimatedSchemaAndResults(queryString);
                    compareSchemaWithResults(schema, results);
                });
                it('should work with a query that hard codes a null and returns an single boolean in a query', async () => {
                    const queryString =
                        'select IFNULL(NULL,true) as `conv` from `customers` limit 1';
                    const {schema, results} =
                        await getEstimatedSchemaAndResults(queryString);
                    compareSchemaWithResults(schema, results);
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
                    assert.deepStrictEqual(schema.length, 3);
                    const itemSplit = schema[2];
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
                    assert.deepStrictEqual(schema.length, 3);
                    const itemSplit = schema[2];
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
                    assert.deepStrictEqual(schema.length, 3);
                    const itemSplit = schema[2];
                    assert.deepStrictEqual(itemSplit.as, 'test');
                    assert.deepStrictEqual(itemSplit.type, 'object');
                    assert.deepStrictEqual(itemSplit.isArray, true);
                });
            });
            describe('unset', () => {
                it('should work when unsetting a single field even if it would not have been in the result set', async () => {
                    const queryString = 'select _id, unset(`item`) from orders';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 1);
                    const trimmedItem = schema[0];
                    assert.deepStrictEqual(trimmedItem.path, '_id');
                    assert.deepStrictEqual(trimmedItem.type, 'string');
                    assert.deepStrictEqual(trimmedItem.isArray, false);
                });
                it('should work when unsetting multiple fields not in the result set', async () => {
                    const queryString =
                        'select _id, unset(`item,notes`) from orders';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 1);
                    const trimmedItem = schema[0];
                    assert.deepStrictEqual(trimmedItem.path, '_id');
                    assert.deepStrictEqual(trimmedItem.type, 'string');
                    assert.deepStrictEqual(trimmedItem.isArray, false);
                });
                it('should work when unsetting a single field in the result set', async () => {
                    const queryString = 'select *, unset(`item`) from orders';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 8);
                    const itemField = schema.find((s) => s.path === 'item');
                    assert.ok(!itemField);
                });
                it('should work when unsetting multiple fields in the result set', async () => {
                    const queryString =
                        'select *, unset(`item`,`price`) from orders';
                    const ast = parseSQLtoAST(queryString, {
                        database: 'PostgresQL',
                    });
                    const schema = await getResultSchema(
                        ast,
                        queryString,
                        getSchema
                    );
                    assert.deepStrictEqual(schema.length, 7);
                    const itemField = schema.find((s) => s.path === 'item');
                    assert.ok(!itemField);
                    const priceField = schema.find((s) => s.path === 'price');
                    assert.ok(!priceField);
                    assert.deepStrictEqual(schema[6].order, 6);
                });
            });
            describe('merge_objects', () => {
                it('should be able to generate a schema for a a merge_objects and replace root query', async () => {
                    const queryString =
                        'select (select id,`First Name` as Name) as t1, (select id,`Last Name` as LastName) as t2,MERGE_OBJECTS(t1,t2) as `$$ROOT` from customers limit 1';
                    const {schema, results} =
                        await getEstimatedSchemaAndResults(queryString);
                    compareSchemaWithResults(schema, results);
                });
            });
        });

        // functions
        // aliases
        // nested fields
        /**
         * multiple functions "select id,ARRAY_TO_OBJECT(PARSE_JSON('[{\"k\":\"val\",\"v\":1}]')) as test from `customers`"
         * replacing root complex "
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
        it('should return the first tables column when there are two columns with the same name', async () => {
            const queryString =
                'select id from orders inner join `inventory` on sku=item limit 1';
            const {schema, results} = await getEstimatedSchemaAndResults(
                queryString
            );
            compareSchemaWithResults(schema, results);
            assert.deepStrictEqual(schema.length, 2);
            const _idColumn = schema[1];
            assert.deepStrictEqual(_idColumn.path, 'id');
            assert.deepStrictEqual(_idColumn.collectionName, 'orders');
        });
    });

    async function getSchema(collectionName) {
        const doc = await database
            .collection('schemas')
            .findOne({collectionName});
        return doc.flattenedSchema;
    }

    /**
     *
     * @param {string} queryString
     * @returns {Promise<{schema:ResultSchema[],results:Document[],errors:Error[]}>}
     */
    async function getEstimatedSchemaAndResults(queryString) {
        /** @type {Error[]} */
        const errors = [];
        /** @type {ResultSchema[]} */
        let schema = [];
        try {
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            schema = await getResultSchema(ast, queryString, getSchema);
        } catch (err) {
            console.error(err);
            errors.push(err);
        }
        /** @type {Document[]} */
        let results = [];
        try {
            const parsedQuery = makeMongoAggregate(queryString, {
                database: 'PostgresQL',
            });
            results = await mongoClient
                .db(databaseName)
                .collection(parsedQuery.collections[0])
                .aggregate(parsedQuery.pipeline)
                .toArray();
        } catch (err) {
            console.error(err);
            errors.push(err);
        }
        return {results, schema, errors};
    }

    /**
     *
     * @param {ResultSchema[]} schema
     * @param {Document[]} results
     */
    function compareSchemaWithResults(schema, results) {
        if (results.length === 0) {
            throw new Error('No results to compare with');
        }
        if (schema.length === 0) {
            throw new Error('No schema to compare with');
        }
        for (const result of results) {
            for (const [key, value] of Object.entries(result)) {
                const fieldSchema = schema.find(
                    (s) => s.as === key || s.path === key
                );
                if (!fieldSchema) {
                    throw new Error(`No schema found for key "${key}"`);
                }
                let valueType = typeof value;
                if (
                    fieldSchema.format === 'mongoid' &&
                    fieldSchema.type === 'string'
                ) {
                    valueType = 'string';
                }
                const mappedJsonSchemaType =
                    // @ts-ignore
                    _jsonSchemaTypeMapping[fieldSchema.type];
                if (!mappedJsonSchemaType) {
                    throw new Error(
                        `No _jsonSchemaTypeMapping for type "${fieldSchema.type}"`
                    );
                }
                if (valueType !== mappedJsonSchemaType) {
                    throw new Error(
                        `Type of value "${valueType}" does not equal schema type ${fieldSchema.type} for field "${key}".`
                    );
                }
            }
        }
    }
});
