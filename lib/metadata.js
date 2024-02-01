const {ColumnDoesNotExistError, TableDoesNotExistError} = require('./errors');
// @ts-ignore
const {functionByName} = require('./MongoFunctions');
const $check = require('check-types');
const {parseSQLtoAST} = require('./parseSQLtoAST');

module.exports = {getResultSchema, getResultSchemaForStatement};

/**
 * @param {string} statement
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @param {import('./types').ParserOptions} [options] - the AST options
 * @returns {Promise<import('./types').ResultSchema[]>}
 */
async function getResultSchemaForStatement(
    statement,
    getSchemaFunction,
    options
) {
    options = options || {};
    const {parsedAst} = parseSQLtoAST(statement, options);
    return await getResultSchema(parsedAst, statement, getSchemaFunction);
}
/**
 * @param {import('./types').TableColumnAst } tableColumnAst
 * @param {string} statement
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @param {import('./types').FlattenedSchemas} [contextTables]
 * @returns {Promise<import('./types').ResultSchema[]>}
 */
async function getResultSchema(
    tableColumnAst,
    statement,
    getSchemaFunction,
    contextTables
) {
    const {ast} = tableColumnAst;
    if (
        ast.columns === '*' ||
        (ast.columns.length === 1 && ast.columns[0].expr.column === '*')
    ) {
        return await processSelectAllStatement(
            ast,
            getSchemaFunction,
            statement
        );
    }
    if (ast.columns.length === 0) {
        throw new Error(`No columns specified in statement : ${statement}`);
    }

    let tables = await getTables(ast, getSchemaFunction, statement);
    if (contextTables) {
        tables = {
            ...contextTables,
            ...tables,
        };
    }
    /** @type {import('./types').ResultSchema[]} */
    let results = [];
    let hadReplaceRoot = false;
    for (const column of ast.columns) {
        if (
            await processColumnSubQuery(
                tables,
                column,
                results,
                statement,
                getSchemaFunction
            )
        ) {
            continue;
        }
        if (await addReplaceRouteSchema(tables, column, results, statement)) {
            hadReplaceRoot = true;
            continue;
        }
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
    if (
        !contextTables &&
        !hadReplaceRoot &&
        !results.some((r) => r.path === '_id')
    ) {
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
 * @param {import('./types').AST} ast
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @param {string} statement
 * @returns {Promise<import('./types').FlattenedSchemas>}
 */
async function getTables(ast, getSchemaFunction, statement) {
    /** @type {import('./types').FlattenedSchemas} */
    const results = {};
    if (!ast.from) {
        // sub-query, just return
        return results;
    }
    for (const from of ast.from) {
        if (!from.table) {
            if (from.expr) {
                results[from.as] = await getResultSchema(
                    // @ts-ignore
                    from.expr,
                    statement,
                    getSchemaFunction
                );
            }
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
 * @param {import('./types').FlattenedSchemas} tables
 * @param {import('./types').Column} column
 * @param {import('./types').ResultSchema[]} results
 * @param {string} statement
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @returns {Promise<boolean>}
 */
async function processColumnSubQuery(
    tables,
    column,
    results,
    statement,
    getSchemaFunction
) {
    if (column.type !== 'expr' || column.as === '$$ROOT' || column.expr.type) {
        return false;
    }
    const subQueryResults = await getResultSchema(
        // @ts-ignore
        column.expr,
        statement,
        getSchemaFunction,
        tables
    );
    for (const result of subQueryResults) {
        result.path = `${column.as}.${result.path}`;
        results.push(result);
    }
    return true;
}

/**
 * @param {import('./types').FlattenedSchemas} tables
 * @param {import('./types').Column} column
 * @param {import('./types').ResultSchema[]} results
 * @param {string} statement
 * @returns {Promise<boolean>}
 */
async function addReplaceRouteSchema(tables, column, results, statement) {
    if (column.as !== '$$ROOT' || column.type !== 'expr') {
        return false;
    }
    if (column.expr.type === 'function') {
        if (column.expr.name.toLowerCase() === 'merge_objects') {
            const obj1 = column.expr.args.value[0];
            if (!obj1) {
                throw new Error(
                    `Two parameters must be provided when using the merge_objects function`
                );
            }
            const obj2 = column.expr.args.value[1];
            if (!obj2) {
                throw new Error(
                    `Two parameters must be provided when using the merge_objects function, only one was provided`
                );
            }
            const firstObj = results
                .filter((r) => r.path.startsWith(obj1.column))
                .map((r) => {
                    r.path = r.path.replace(obj1.column + '.', '');
                    return r;
                });
            const secondObj = results
                .filter((r) => r.path.startsWith(obj2.column))
                .map((r) => {
                    r.path = r.path.replace(obj2.column + '.', '');
                    return r;
                });
            results.length = 0;
            for (const col of firstObj) {
                results.push(col);
            }
            for (const col of secondObj) {
                const alreadyExists = results.some(
                    (r) =>
                        r.path === col.path &&
                        r.as === col.as &&
                        r.collectionName === col.collectionName
                );
                if (!alreadyExists) {
                    results.push(col);
                }
            }
            return true;
        }
        throw new Error(`Unsuported replace root function ${column.expr.name}`);
    }
    const tableName = column.expr.column;
    const table = tables[tableName];
    if (!table) {
        throw new Error(`No table with name "${tableName}" was found`);
    }
    results.length = 0;
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
