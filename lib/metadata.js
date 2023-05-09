const {ColumnDoesNotExistError, TableDoesNotExistError} = require('./errors');
const {functionByName} = require('./MongoFunctions');
const $check = require('check-types');

module.exports = {getResultSchema};

/**
 *
 * @param {string} tableName
 * @param {import('./types').GetSchemaFunction} getSchemaFunction
 * @returns {Promise<import('./types').FlattenedSchema[]>}
 */
async function getTable(tableName, getSchemaFunction) {
    try {
        return await getSchemaFunction(tableName);
    } catch (err) {
        console.error(err);
        return null;
    }
}
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
        const tableName = ast.from[0].table;
        const table = await getTable(tableName, getSchemaFunction);
        if (!table) {
            throw new TableDoesNotExistError(tableName, statement);
        }
        return table.map((s, index) => {
            return {...s, order: index, collectionName: tableName};
        });
    }
    if (ast.columns.length > 0) {
        const tableName = ast.from[0].table;
        const table = await getTable(tableName, getSchemaFunction);
        if (!table) {
            throw new TableDoesNotExistError(tableName, statement);
        }
        /** @type {import('./types').ResultSchema[]} */
        const results = [];
        let counter = 0;
        for (const column of ast.columns) {
            if (
                column.expr.type === 'column_ref' &&
                column.expr.column === '*'
            ) {
                for (const t of table) {
                    results.push({
                        ...t,
                        order: counter,
                        collectionName: tableName,
                    });
                    counter++;
                }
                continue;
            }
            if (column.expr.type === 'column_ref') {
                const found = table.find((t) => t.path === column.expr.column);
                if (found) {
                    results.push({
                        ...found,
                        order: counter,
                        collectionName: tableName,
                        as: column.as,
                    });
                    counter++;
                    continue;
                } else {
                    throw new ColumnDoesNotExistError(
                        column.expr.column,
                        ast.from[0].table,
                        statement
                    );
                }
            }
            if (
                column.expr.type === 'function' ||
                column.expr.type === 'aggr_func'
            ) {
                const foundFunction = functionByName(column.expr.name);
                const {jsonSchemaReturnType} = foundFunction;
                if ($check.function(jsonSchemaReturnType)) {
                    const result = jsonSchemaReturnType(column.expr.args.value);
                    if (result.type === 'fieldName') {
                        const foundCol = table.find(
                            (c) => c.path === result.fieldName
                        );
                        if (foundCol) {
                            results.push({
                                path: foundCol.path,
                                type: foundCol.type,
                                isArray: result.isArray || false,
                                required: foundCol.required,
                                order: counter,
                                collectionName: 'function()',
                                as: column.as,
                                format: foundCol.format,
                            });
                            counter++;
                            continue;
                        }
                        const isObjectArray = table.some((c) =>
                            c.path
                                .toLowerCase()
                                .startsWith(`${result.fieldName}.n.`)
                        );
                        if (isObjectArray) {
                            results.push({
                                path: result.fieldName,
                                type: 'object',
                                isArray: true,
                                required: false,
                                order: counter,
                                collectionName: 'function()',
                                as: column.as,
                            });
                            counter++;
                            continue;
                        }
                        // could be a collection, e.g. rentals.n.<props>
                    }
                    results.push({
                        path: '',
                        type: result.jsonSchemaValue,
                        isArray: result.isArray || false,
                        required: false,
                        order: counter,
                        collectionName: 'function()',
                        as: column.as,
                    });
                    counter++;
                    continue;
                }
                results.push({
                    path: '',
                    type: jsonSchemaReturnType,
                    isArray: false,
                    required: false,
                    order: counter,
                    collectionName: 'function()',
                    as: column.as,
                });
                counter++;
                continue;

                // const columns = column.expr.args.value.filter(
                //     (val) => val.type === 'column_ref'
                // );
                // const found = table.find((t) => t.path === column.expr.column);
                // if (found) {
                //     results.push({
                //         ...found,
                //         order: counter,
                //         collectionName: tableName,
                //         as: column.as,
                //     });
                //     counter++;
                // } else {
                //     throw new ColumnDoesNotExistError(
                //         column.expr.column,
                //         ast.from[0].table,
                //         statement
                //     );
                // }
            }
            throw new Error(`Not Implemented:\n${JSON.stringify(column)}`);
        }
        return results;
    }
    throw new Error('not impl');
}
