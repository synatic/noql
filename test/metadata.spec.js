const {getResultSchema} = require('../lib/metadata');
// @ts-ignore
const {parseSQLtoAST} = require('../lib/SQLParser');
const {
    ColumnDoesNotExistError,
    TableDoesNotExistError,
} = require('../lib/errors');
const assert = require('assert');

describe('metadata', () => {
    describe('no joins', () => {
        it('should be able to generate a schema for a * query', async () => {
            const queryString = 'select * from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 9);
            const _idColumn = schema[0];
            assert.deepStrictEqual(_idColumn.path, '_id');
        });
        it('should be able to generate a schema for a specific field', async () => {
            const queryString = 'select _id from orders';
            const ast = parseSQLtoAST(queryString, {
                database: 'PostgresQL',
            });
            const schema = await getResultSchema(ast, queryString, getSchema);
            assert.deepStrictEqual(schema.length, 1);
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
        });
        describe('functions', () => {
            describe('static return types', () => {
                it('should be able to generate a schema for a specific field that is a function with an alias', async () => {
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
                });
            });
            describe('parameter based return types', () => {
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
                    // not object
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
            });
        });

        // functions
        // aliases
        // nested fields
        /**
         * multiple functions "select id,ARRAY_TO_OBJECT(PARSE_JSON('[{\"k\":\"val\",\"v\":1}]')) as test from `customers`"
         * replacing root complex "select (select id,`First Name` as Name) as t1, (select id,`Last Name` as LastName) as t2,MERGE_OBJECTS(t1,t2) as `$$ROOT`
         * gets field select SPLIT(`First Name`,',') as exprVal from `customers`
         * wtf select id,ZIP_ARRAY((select `Film Title` as '$$ROOT' from `Rentals`),ARRAY_RANGE(0,10,2)) as test from `customers`
         * $unset
         */
    });
});

/**
 * @param {string} collectionName;
 * @param collectionName
 * @returns {Promise<import('../lib/types').FlattenedSchema[]>}
 */
async function getSchema(collectionName) {
    // todo RK get schemas from db once this is all figured out;

    const schemas = {
        customers: {
            schema: {
                type: 'object',
                properties: {
                    _id: {
                        type: 'string',
                        format: 'mongoid',
                    },
                    Address: {
                        type: 'object',
                        properties: {
                            Address: {
                                type: 'string',
                                stringLength: 24,
                            },
                            City: {
                                type: 'string',
                                stringLength: 15,
                            },
                            Country: {
                                type: 'string',
                                stringLength: 12,
                            },
                            District: {
                                type: 'string',
                                stringLength: 16,
                            },
                        },
                    },
                    'First Name': {
                        type: 'string',
                        stringLength: 10,
                    },
                    'Last Name': {
                        type: 'string',
                        stringLength: 9,
                    },
                    Phone: {
                        type: 'string',
                        stringLength: 12,
                    },
                    Rentals: {
                        type: 'array',
                        items: {
                            type: 'object',
                            properties: {
                                'Film Title': {
                                    type: 'string',
                                    stringLength: 23,
                                },
                                Payments: {
                                    type: 'array',
                                    items: {
                                        type: 'object',
                                        properties: {
                                            Amount: {
                                                type: 'number',
                                            },
                                            'Payment Date': {
                                                type: 'string',
                                                stringLength: 21,
                                            },
                                            'Payment Id': {
                                                type: 'integer',
                                            },
                                        },
                                    },
                                },
                                'Rental Date': {
                                    type: 'string',
                                    stringLength: 21,
                                },
                                'Return Date': {
                                    type: ['string', 'null'],
                                    stringLength: 21,
                                },
                                filmId: {
                                    type: 'integer',
                                },
                                rentalId: {
                                    type: 'integer',
                                },
                                staffId: {
                                    type: 'integer',
                                },
                            },
                        },
                    },
                    id: {
                        type: 'integer',
                    },
                },
            },
            flattenedSchema: [
                {
                    path: '_id',
                    type: 'string',
                    format: 'mongoid',
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Address.Address',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Address.City',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Address.Country',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Address.District',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'First Name',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Last Name',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Phone',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'Rentals.n.Film Title',
                    type: 'string',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.Payments.n.Amount',
                    type: 'number',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.Payments.n.Payment Date',
                    type: 'string',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.Payments.n.Payment Id',
                    type: 'integer',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.Rental Date',
                    type: 'string',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.Return Date',
                    type: ['string', 'null'],
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.filmId',
                    type: 'integer',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.rentalId',
                    type: 'integer',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'Rentals.n.staffId',
                    type: 'integer',
                    format: undefined,
                    isArray: true,
                    required: false,
                },
                {
                    path: 'id',
                    type: 'integer',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
            ],
        },
        inventory: {
            schema: {
                type: 'object',
                properties: {
                    _id: {
                        type: 'string',
                        format: 'mongoid',
                    },
                    id: {
                        type: 'integer',
                    },
                    sku: {
                        type: ['string', 'null'],
                        stringLength: 7,
                    },
                    description: {
                        type: 'string',
                        stringLength: 10,
                    },
                    instock: {
                        type: 'integer',
                    },
                    specialChars: {
                        type: 'string',
                        stringLength: 3,
                    },
                },
            },
            flattenedSchema: [
                {
                    path: '_id',
                    type: 'string',
                    format: 'mongoid',
                    isArray: false,
                    required: false,
                },
                {
                    path: 'id',
                    type: 'integer',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'sku',
                    type: ['string', 'null'],
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'description',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'instock',
                    type: 'integer',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'specialChars',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
            ],
        },
        orders: {
            schema: {
                type: 'object',
                properties: {
                    _id: {
                        type: 'string',
                        format: 'mongoid',
                    },
                    id: {
                        type: 'integer',
                    },
                    item: {
                        type: 'string',
                        stringLength: 25,
                    },
                    price: {
                        type: 'number',
                    },
                    quantity: {
                        type: 'integer',
                    },
                    customerId: {
                        type: 'integer',
                    },
                    specialChars: {
                        type: 'string',
                        stringLength: 25,
                    },
                    notes: {
                        type: 'string',
                        stringLength: 145,
                    },
                    orderDate: {
                        type: 'string',
                        format: 'date-time',
                    },
                },
            },
            flattenedSchema: [
                {
                    path: '_id',
                    type: 'string',
                    format: 'mongoid',
                    isArray: false,
                    required: false,
                },
                {
                    path: 'id',
                    type: 'integer',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'item',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'price',
                    type: 'number',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'quantity',
                    type: 'integer',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'customerId',
                    type: 'integer',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'specialChars',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'notes',
                    type: 'string',
                    format: undefined,
                    isArray: false,
                    required: false,
                },
                {
                    path: 'orderDate',
                    type: 'string',
                    format: 'date-time',
                    isArray: false,
                    required: false,
                },
            ],
        },
    };
    const result = schemas[collectionName];
    if (!result) {
        return null;
    }
    return result.flattenedSchema;
}
