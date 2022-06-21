const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeCastPartModule = require('./makeCastPart');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeObjectFromSelect = makeObjectFromSelect;

/**
 * Creates an object from a select without a from cause
 *
 * @param {import('../types').AST} ast - the ast for the select statement
 * @returns {any}
 */
function makeObjectFromSelect(ast) {
    const toParse = {};
    /** @type {import('../types').Column[]} */
    // @ts-ignore
    const columns = ast.columns;
    columns.forEach((column) => {
        if (column.expr.type === 'column_ref') {
            toParse[`${column.as || column.expr.column}`] = `$${
                column.expr.table ? column.expr.table + '.' : ''
            }${column.expr.column}`;
        } else if (column.expr.type === 'function' && column.as) {
            const parsedExpr =
                makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                    column.expr
                );
            toParse[`${column.as}`] = parsedExpr;
        } else if (column.expr.type === 'binary_expr' && column.as) {
            toParse[`${column.as}`] =
                getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                    column.expr
                );
        } else if (column.expr.type === 'case' && column.as) {
            toParse[`${column.as}`] = makeCaseConditionModule.makeCaseCondition(
                column.expr
            );
        } else if (column.expr.type === 'cast' && column.as) {
            toParse[`${column.as}`] = makeCastPartModule.makeCastPart(
                column.expr
            );
        } else if (
            column.expr.type === 'select' &&
            column.as &&
            column.expr.from
        ) {
            toParse[`${column.as}`] =
                makeArraySubSelectPartModule.makeArraySubSelectPart(
                    column.expr
                );
        } else if (
            column.expr.type === 'select' &&
            column.as &&
            !column.expr.from
        ) {
            toParse[`${column.as}`] = makeObjectFromSelect(column.expr);
        } else if (column.expr.type && column.as) {
            toParse[`${column.as}`] = {$literal: column.expr.value};
        } else if (!column.as) {
            throw new Error(`Require as for calculation:${column.expr.name}`);
        } else {
            throw new Error(`Not Supported:${column.expr.type}`);
        }
    });

    return {
        $arrayToObject: {$concatArrays: [{$objectToArray: toParse}]},
    };
}
