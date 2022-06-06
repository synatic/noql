const SQLParser = require('../SQLParser');

/**
 * Parses the provided AST for projections of fields to be performed
 *
 * @param {import('node-sql-parser').Select} ast
 * @returns {import('../types').Projection}
 */
function parseQueryForProjections(ast) {
    if (ast.columns && !SQLParser._isSelectAll(ast.columns) && ast.columns.length > 0) {
        return;
    }
    const projection = {};
    /** @type {import('node-sql-parser').Column[]}*/
    // @ts-ignore
    const columns = ast.columns;

    columns.forEach((column) => {
        if (column.expr.type === 'column_ref') {
            if (column.as) {
                projection[column.as] = `$${column.expr.column}`;
            } else {
                projection[column.expr.column] = `$${column.expr.column}`;
            }
        } else if ((column.expr.type === 'function' || column.expr.type === 'aggr_func') && column.as) {
            const parsedExpr = SQLParser._makeProjectionExpressionPart(column.expr);
            projection[column.as] = parsedExpr;
        } else if (column.expr.type === 'binary_expr' && column.as) {
            projection[column.as] = SQLParser._getParsedValueFromBinaryExpression(column.expr);
        } else if (column.expr.type === 'case' && column.as) {
            projection[column.as] = SQLParser._makeCaseCondition(column.expr);
        } else if (column.expr.type === 'cast' && column.as) {
            projection[column.as] = SQLParser._makeCastPart(column.expr);
        } else if (column.expr.type === 'select' && column.as && column.as && column.expr.from) {
            projection[column.as] = SQLParser._makeArraySubSelectPart(column.expr);
        } else if (column.expr.type === 'select' && column.as && !column.expr.from) {
            projection[column.as] = SQLParser._makeObjectFromSelect(column.expr);
        } else if (column.expr.type && column.as) {
            projection[column.as] = {$literal: column.expr.value};
        } else if (!column.as) {
            throw new Error(`Require as for calculation:${column.expr.name}`);
        } else {
            throw new Error(`Not Supported:${column.expr.type}`);
        }
    });
    return projection;
}

module.exports = {parseQueryForProjections};
