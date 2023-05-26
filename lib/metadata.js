const {ColumnDoesNotExistError, TableDoesNotExistError} = require('./errors');
// @ts-ignore
const {functionByName} = require('./MongoFunctions');
const $check = require('check-types');

module.exports = {getResultSchema};

/**
 *
 * @param {import('./types').TableColumnAst} tableColumnAst
 * @param {string} statement
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @returns {Promise<import('./types').ResultSchema[]>}
 */
async function getResultSchema(tableColumnAst, statement, getSchemaFunction) {
    const {ast} = tableColumnAst;
    if (ast.columns === '*') {
        return await processSelectAllStatement(
            ast,
            getSchemaFunction,
            statement
        );
    }
    if (ast.columns.length === 0) {
        throw new Error(`No columns specified in statement : ${statement}`);
    }

    const tables = await getTables(ast, getSchemaFunction, statement);
    /** @type {import('./types').ResultSchema[]} */
    let results = [];
    for (const column of ast.columns) {
        if (await addSchemaForAllColumns(tables, column, results)) {
            continue;
        }
        if (
            await addSchemaForNormalColumn(tables, column, results, statement)
        ) {
            continue;
        }
        if (
            await addSchemaForFunctionColumn(tables, column, results, statement)
        ) {
            continue;
        }
        throw new Error(`Not Implemented:\n${JSON.stringify(column)}`);
    }
    if (!results.some((r) => r.path === '_id')) {
        // think this should be for each table
        const [tableName] = Object.entries(tables)[0];
        /** @type{import('./types').ResultSchema} */
        const idCol = {
            collectionName: tableName,
            isArray: false,
            order: -1,
            path: '_id',
            required: false,
            type: 'string',
            format: 'mongoid',
        };
        results = [idCol].concat(results);
    }
    const unsets = results.filter((r) => r.collectionName === 'unset()');
    for (const unset of unsets) {
        results = results.filter((r) => r.path !== unset.path);
    }

    // ensures fields in order with the correct order index
    results.sort((a, b) => (a.order > b.order ? 1 : -1));
    results = results.map((r, index) => {
        r.order = index;
        return r;
    });

    return results;
}

/**
 *
 * @param {import('./types').AST} ast
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @param {string} statement
 * @returns {Promise<import('./types').ResultSchema[]>}
 */
async function processSelectAllStatement(ast, getSchemaFunction, statement) {
    const tables = await getTables(ast, getSchemaFunction, statement);
    /** @type {import('./types').ResultSchema[]} */
    const results = [];
    let counter = -1;
    // eslint-disable-next-line guard-for-in
    for (const tableName in tables) {
        const columns = tables[tableName];
        const mapped = columns.map((c) => {
            counter++;
            return {...c, order: counter, collectionName: tableName};
        });
        results.push(...mapped);
    }
    return results;
}

/**
 *
 * @param {import('./types').AST} ast
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @param {string} statement
 * @returns {Promise<import('./types').FlattenedSchemas>}
 */
async function getTables(ast, getSchemaFunction, statement) {
    /** @type {import('./types').FlattenedSchemas} */
    const results = {};
    for (const from of ast.from) {
        if (!from.table) {
            continue;
        }
        try {
            const schemas = await getSchemaFunction(from.table);
            results[from.table] = schemas;
        } catch (err) {
            console.error(err);
            throw new TableDoesNotExistError(from.table, statement);
        }
    }
    if (Object.keys(results).length === 0) {
        throw new Error(`No tables specified in statement: ${statement}`);
    }
    return results;
}
/**
 *
 * @param {import('./types').FlattenedSchemas} tables
 * @param {import('./types').Column} column
 * @param {import('./types').ResultSchema[]} results
 * @returns {Promise<boolean>}
 */
async function addSchemaForAllColumns(tables, column, results) {
    if (column.expr.type !== 'column_ref' || column.expr.column !== '*') {
        return false;
    }
    const [tableName, table] = Object.entries(tables)[0];
    for (const t of table) {
        results.push({
            ...t,
            order: results.length,
            collectionName: tableName,
        });
    }
    return true;
}

/**
 *
 * @param {import('./types').FlattenedSchemas} tables
 * @param {import('./types').Column} column
 * @param {import('./types').ResultSchema[]} results
 * @param {string} statement
 * @returns {Promise<boolean>}
 */
async function addSchemaForNormalColumn(tables, column, results, statement) {
    if (column.expr.type !== 'column_ref' || column.expr.column === '*') {
        return false;
    }
    const [tableName, table] = Object.entries(tables)[0];
    const found = table.find(
        (t) => t.path.toLowerCase() === column.expr.column.toLowerCase()
    );
    if (!found) {
        throw new ColumnDoesNotExistError(
            column.expr.column,
            tableName,
            statement
        );
    }
    results.push({
        ...found,
        order: results.length,
        collectionName: tableName,
        as: column.as,
    });
    return true;
}

const functionCollectionName = 'function()';
/**
 *
 * @param {import('./types').FlattenedSchemas} tables
 * @param {import('./types').Column} column
 * @param {import('./types').ResultSchema[]} results
 * @param {string} statement
 * @returns {Promise<boolean>}
 */
async function addSchemaForFunctionColumn(tables, column, results, statement) {
    if (column.expr.type !== 'function' && column.expr.type !== 'aggr_func') {
        return false;
    }
    const [, table] = Object.entries(tables)[0];
    const foundFunction = functionByName(column.expr.name);
    const {jsonSchemaReturnType} = foundFunction;
    if (!$check.function(jsonSchemaReturnType)) {
        // Has a static return type, so just use the result.
        results.push({
            path: '',
            type: jsonSchemaReturnType,
            isArray: false,
            required: false,
            order: results.length,
            collectionName: `${foundFunction.name}()`,
            as: column.as,
        });
        return true;
    }
    // @ts-ignore
    const fnResult = jsonSchemaReturnType(column.expr.args.value);
    if (Array.isArray(fnResult)) {
        for (const res of fnResult) {
            if (res.type !== 'unset') {
                throw new Error(
                    'Not implemented for array return type that is not unset, type: ' +
                        res.type
                );
            }
            results.push({
                path: res.fieldName,
                type: null,
                isArray: false,
                required: false,
                order: results.length,
                collectionName: 'unset()',
                as: null,
            });
        }
        return true;
    }
    if (fnResult.type === 'jsonSchemaValue') {
        // Has a static result type, so just use the result + additional params.
        results.push({
            path: '',
            type: fnResult.jsonSchemaValue,
            isArray: fnResult.isArray || false,
            required: false,
            order: results.length,
            collectionName: functionCollectionName,
            as: column.as,
        });
        return true;
    }

    const foundCol = table.find(
        (c) => c.path.toLowerCase() === fnResult.fieldName.toLowerCase()
    );
    if (foundCol) {
        results.push({
            path: foundCol.path,
            type: foundCol.type,
            isArray: fnResult.isArray || false,
            required: foundCol.required,
            order: results.length,
            collectionName: functionCollectionName,
            as: column.as,
            format: foundCol.format,
        });

        return true;
    }
    const isObjectArray = table.some((c) =>
        c.path
            .toLowerCase()
            .startsWith(`${fnResult.fieldName.toLowerCase()}.n.`)
    );
    if (isObjectArray) {
        results.push({
            path: fnResult.fieldName,
            type: 'object',
            isArray: $check.assigned(fnResult.isArray)
                ? fnResult.isArray
                : true,
            required: false,
            order: results.length,
            collectionName: functionCollectionName,
            as: column.as,
        });
        return true;
    }
    const primitiveArrayCol = table.find(
        (c) => c.path.toLowerCase() === `${fnResult.fieldName.toLowerCase()}.n`
    );
    if (primitiveArrayCol) {
        results.push({
            path: primitiveArrayCol.path,
            type: primitiveArrayCol.type,
            isArray: true,
            required: primitiveArrayCol.required,
            order: results.length,
            collectionName: functionCollectionName,
            as: column.as,
            format: primitiveArrayCol.format,
        });
        return true;
    }
    throw new Error(
        `Not supported:\n${JSON.stringify(column)}\n${JSON.stringify(fnResult)}`
    );
}
