const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeCastPartModule = require('./makeCastPart');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const $check = require('check-types');

exports.projectColumnParser = projectColumnParser;

/**
 * @param {import('../types').Column} column The column to parse
 * @param {import('../types').ColumnParseResult} result the result object
 * @returns {void}
 */
function projectColumnParser(column, result) {
    if (column.expr.type === 'column_ref') {
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {
                $replaceRoot: {newRoot: `$${column.expr.column}`},
            };
            return;
        }
        if (column.expr.column === '*' && column.expr.table) {
            result.parsedProject.$project[
                column.as || column.expr.table
            ] = `$${column.expr.table}`;
            return;
        }
        if (column.expr.column === '*') {
            result.exprToMerge.push('$$ROOT');
            return;
        }
        result.parsedProject.$project[column.as || column.expr.column] = `$${
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
        if (
            column.expr.args &&
            column.expr.args.value &&
            $check.array(column.expr.args.value) &&
            column.expr.args.value[0] &&
            column.expr.args.value[0].column &&
            column.expr.args.value[0].column !== column.as
        ) {
            result.unwind.push({
                $unset: column.expr.args.value[0].column,
            });
        }
        if (column.as && column.as === '$$ROOT') {
            result.replaceRoot = {
                $replaceRoot: {newRoot: `$${column.expr.args.value[0].value}`},
            };
            return;
        }
        result.parsedProject.$project[column.as] =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr.args.value[0]
            );
        result.unwind.push({
            $unwind: {
                path: `$${column.as}`,
                preserveNullAndEmptyArrays: true,
            },
        });
        return;
    }
    if (
        column.expr.type === 'function' &&
        column.expr.name &&
        column.expr.name.toLowerCase() === 'unset'
    ) {
        if (!column.expr.args || !column.expr.args.value) {
            throw new Error('Unset requires the field names to be passed in');
        }
        const fieldsToUnset = column.expr.args.value
            .map((v) => v.column)
            .reduce((obj, col) => {
                return {...obj, [col]: 0};
            }, {});
        result.parsedProject.$project = {
            ...result.parsedProject.$project,
            ...fieldsToUnset,
        };
        return;
    }
    if (column.expr.type === 'function' && column.as) {
        if (column.expr.name.toLowerCase() === 'count') {
            result.count.push({$count: column.as});
            return;
        }
        const parsedExpr =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr
            );
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
            return;
        }
        result.parsedProject.$project[column.as] = parsedExpr;
        return;
    }
    if (column.expr.type === 'aggr_func') {
        result.parsedProject.$project[column.as] =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr
            );
        return;
    }
    if (column.expr.type === 'binary_expr' && column.as) {
        result.parsedProject.$project[column.as] =
            getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                column.expr
            );
        return;
    }
    if (column.expr.type === 'case' && column.as) {
        result.parsedProject.$project[column.as] =
            makeCaseConditionModule.makeCaseCondition(column.expr);
        return;
    }
    if (column.expr.type === 'cast' && column.as) {
        result.parsedProject.$project[column.as] =
            makeCastPartModule.makeCastPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && column.expr.from) {
        result.parsedProject.$project[column.as] =
            makeArraySubSelectPartModule.makeArraySubSelectPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && !column.expr.from) {
        result.parsedProject.$project[column.as] =
            makeObjectFromSelectModule.makeObjectFromSelect(column.expr);
        return;
    }
    if (column.expr.type && column.as) {
        if (column.as === '$$ROOT') {
            result.replaceRoot = {
                $replaceRoot: {newRoot: `$${column.expr.value}`},
            };
            return;
        }
        result.parsedProject.$project[column.as] = {
            $literal: column.expr.value,
        };
        return;
    }
    if (!column.as) {
        throw new Error(`Require as for calculation:${column.expr.name}`);
    }
    throw new Error(`Not Supported:${column.expr.type}`);
}
