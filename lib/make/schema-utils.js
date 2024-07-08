const $check = require('check-types');

module.exports = {findSchema};
/**
 * @param {string} column
 * @param {import("../types").NoqlContext}context
 * @returns {import("../types").JSONSchema6}
 */
function findSchema(column, context) {
    if (!context.schemas) {
        return null;
    }
    // const tableAliases = context.fullAst.ast.from;
    const parts = column.split('.');
    // const colAlias = parts.length > 1 ? parts[0] : null;
    const colName = parts.length > 1 ? parts[1] : column;
    // const foundTableAlias = colAlias
    //     ? tableAliases.find(
    //           (ta) => ta.as && ta.as.toLowerCase() === colAlias.toLowerCase()
    //       )
    //     : null;
    // eslint-disable-next-line guard-for-in
    const filteredSchemas = Object.keys(context.schemas).filter(
        (name) => context.tables.indexOf(name) >= 0
    );
    for (const collectionName of filteredSchemas) {
        const schema = context.schemas[collectionName];
        const result = findSubSchema(schema, colName);
        if (result) {
            return result;
        }
    }
    return null;
}

/**
 * @param {import("../types").JSONSchema6} schema
 * @param {string} column
 * @returns {import("../types").JSONSchema6}
 */
function findSubSchema(schema, column) {
    if (!schema.properties) {
        return null;
    }
    // eslint-disable-next-line guard-for-in
    for (const key in schema.properties) {
        const value = schema.properties[key];
        if ($check.boolean(value)) {
            continue;
        }
        if (key.toLowerCase() === column.toLowerCase()) {
            return value;
        }
        if (
            value.type === 'object' ||
            (Array.isArray(value.type) && value.type.indexOf('object') >= 0)
        ) {
            const res = findSubSchema(value, column);
            if (res) {
                return res;
            }
        }
    }
    return null;
}
