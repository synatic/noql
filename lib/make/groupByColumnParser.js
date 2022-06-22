const _allowableFunctions = require('../MongoFunctions');
const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeCastPartModule = require('./makeCastPart');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.groupByColumnParser = groupByColumnParser;

/**
 * @param {import('../types').Column} column The column to parse
 * @param {import('../types').ColumnParseResult} result the result object
 * @returns {void}
 */
function groupByColumnParser(column, result) {
    if (column.expr.type === 'column_ref') {
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {
                $replaceRoot: {newRoot: `$${column.expr.column}`},
            };
            return;
        }
        if (column.as) {
            result.asMapping.push({column: column.expr.column, as: column.as});
            result.groupBy.$group._id[column.as] = `$${
                column.expr.table ? column.expr.table + '.' : ''
            }${column.expr.column}`;
            return;
        }
        result.groupBy.$group._id[column.expr.column] = `$${
            column.expr.table ? column.expr.table + '.' : ''
        }${column.expr.column}`;
        return;
    }
    if (
        column.expr.type === 'function' &&
        column.as &&
        column.expr.name &&
        column.expr.name.toLowerCase() === 'unwind'
    ) {
        throw new Error('Unwind not allowed with group by');
    }
    if (column.expr.type === 'function' && column.as) {
        const parsedExpr =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr
            );
        if (column.expr.name.toLowerCase() === 'count') {
            result.groupBy.$group[column.as] = parsedExpr;
            return; // count values can't go in the groupBy
        }
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
            return;
        }
        result.groupBy.$group._id[column.as] = parsedExpr;
        return;
    }
    if (column.expr.type === 'aggr_func' && column.as) {
        const aggregateFunction = _allowableFunctions.functionMappings.find(
            (f) =>
                f.name &&
                f.name.toLowerCase() === column.expr.name.toLowerCase() &&
                (!f.type || f.type === 'aggr_func')
        );
        if (!aggregateFunction) {
            throw new Error(`Function not found:${column.expr.name}`);
        }
        result.groupBy.$group[column.as] =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr
            );
        return;
    }
    if (column.expr.type === 'binary_expr' && column.as) {
        result.groupBy.$group._id[column.as] =
            getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                column.expr
            );
        return;
    }
    if (column.expr.type === 'case' && column.as) {
        result.groupBy.$group._id[column.as] =
            makeCaseConditionModule.makeCaseCondition(column.expr);
        return;
    }
    if (column.expr.type === 'cast' && column.as) {
        result.groupBy.$group._id[column.as] = makeCastPartModule.makeCastPart(
            column.expr
        );
        return;
    }
    if (column.expr.type === 'select' && column.as && column.expr.from) {
        result.groupBy.$group._id[column.as] =
            makeArraySubSelectPartModule.makeArraySubSelectPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && !column.expr.from) {
        result.groupBy.$group._id[column.as] =
            makeObjectFromSelectModule.makeObjectFromSelect(column.expr);
        return;
    }
    if (column.expr.type && column.as) {
        result.groupBy.$group._id[column.as] = {$literal: column.expr.value};
        return;
    }
    if (!column.as) {
        throw new Error(`Require as for calculation:${column.expr.name}`);
    }
    throw new Error(`Not Supported:${column.expr.type}`);
}
