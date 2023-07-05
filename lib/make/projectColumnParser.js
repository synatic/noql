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
 * @param {string} [tableAlias] - a table alias to check if it hasn't been specified
 * @returns {void}
 */
function projectColumnParser(column, result, tableAlias = '') {
    if (column.expr.type === 'column_ref') {
        const columnTable = column.expr.table || tableAlias;

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
            columnTable ? columnTable + '.' : ''
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
        const parsedExpr =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr
            );
        result.unset = parsedExpr;
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
        if (column.expr.name.toLowerCase() === 'count') {
            result.count.push({$count: column.as});
            return;
        }
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
    if (column.as && column.as === '$$ROOT') {
        result.replaceRoot = {
            $replaceRoot: {newRoot: `$${column.expr.value}`},
        };
        return;
    }
    if (
        column.expr.type === 'double_quote_string' ||
        column.expr.type === 'string' ||
        column.expr.type === 'single_quote_string'
    ) {
        const parts = column.expr.value.split('.');
        if (!column.as) {
            if (parts.length !== 2) {
                throw new Error(
                    `Require as for calculation or <table>.<column>:${
                        column.expr.name || column.expr.value
                    }`
                );
            }
            const colName = parts[1];
            result.parsedProject.$project[colName] = `$${column.expr.value}`;
            return;
        }
        if (parts.length !== 2) {
            result.parsedProject.$project[column.as] = {
                $literal: column.expr.value,
            };
            return;
        }
        result.parsedProject.$project[column.as] = `$${column.expr.value}`;
        return;
    }

    if (!column.as) {
        throw new Error(
            `Require as for calculation:${
                column.expr.name || column.expr.value
            }`
        );
    }

    if (column.expr.type === 'number' || column.expr.type === 'bool') {
        result.parsedProject.$project[column.as] = {
            $literal: column.expr.value,
        };
        return;
    }

    if (
        column.expr.type === 'extract' &&
        column.expr.args &&
        column.expr.args.source
    ) {
        const parsedExpr =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr.args.source
            );

        let fieldFunction = null;
        if (column.expr.args.field === 'year') {
            fieldFunction = '$year';
        } else if (column.expr.args.field === 'month') {
            fieldFunction = '$month';
        } else if (column.expr.args.field === 'day') {
            fieldFunction = '$dayOfMonth';
        } else if (column.expr.args.field === 'hour') {
            fieldFunction = '$hour';
        } else if (column.expr.args.field === 'minute') {
            fieldFunction = '$minute';
        } else if (column.expr.args.field === 'second') {
            fieldFunction = '$second';
        } else if (column.expr.args.field === 'milliseconds') {
            fieldFunction = '$millisecond';
        } else if (column.expr.args.field === 'week') {
            fieldFunction = '$week';
        } else if (column.expr.args.field === 'dow') {
            fieldFunction = '$dayOfWeek';
        }
        // todo cater for
        // century	Uses the Gregorian calendar where the first century starts at '0001-01-01 00:00:00 AD'
        // decade	Year divided by 10
        // epoch	Number of seconds since '1970-01-01 00:00:00 UTC', if date value. Number of seconds in an interval, if interval value
        // isodow	Day of the week (1=Monday, 2=Tuesday, 3=Wednesday, ... 7=Sunday)
        // isoyear	ISO 8601 year value (where the year begins on the Monday of the week that contains January 4th)
        // microseconds	Seconds (and fractional seconds) multiplied by 1,000,000
        // millennium	Millennium value
        // quarter	Quarter (1 to 4)
        // timezone	Time zone offset from UTC, expressed in seconds
        // timezone_hour	Hour portion of the time zone offset from UTC
        // timezone_minute	Minute portion of the time zone offset from UTC

        if (!fieldFunction) {
            throw new Error('Cannot extract: ' + column.expr.args.field);
        }

        result.parsedProject.$project[column.as] = {
            [fieldFunction]: parsedExpr,
        };
        return;
    }
    if (column.expr.type === 'window_func') {
        const {orderby, partitionby} =
            column.expr.over.as_window_specification.window_specification;
        const setWindowFunction = {
            sortBy: {},
            output: {},
        };
        if (column.expr.name === 'RANK') {
            setWindowFunction.output[column.as] = {$rank: {}};
        } else if (column.expr.name === 'ROW_NUMBER') {
            setWindowFunction.output[column.as] = {$documentNumber: {}};
        } else if (column.expr.name === 'DENSE_RANK') {
            setWindowFunction.output[column.as] = {$denseRank: {}};
        } else {
            throw new Error(`Unsupported window function:${column.expr.name}`);
        }
        if (partitionby) {
            if (partitionby.length !== 1) {
                throw new Error(`Multiple partition bys are not supported`);
            }
            const res =
                makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                    partitionby[0].expr
                );
            setWindowFunction.partitionBy = res;
        }
        for (const order of orderby) {
            const res = makeProjectionExpressionPartModule
                .makeProjectionExpressionPart(order.expr)
                .replace(/\$/g, '');
            const direction = order.type === 'ASC' ? 1 : -1;
            setWindowFunction.sortBy[res] = direction;
        }
        result.windowFields.push(setWindowFunction);
        result.parsedProject.$project[column.as] = '$' + column.as;
        return;
    }

    if (column.expr.type) {
        throw new Error(`Unsupported expression type:${column.expr.type}`);
    }
    if (column.expr.ast) {
        return projectColumnParser({...column, expr: column.expr.ast}, result);
    }
    throw new Error(
        `Column not supported:\n${JSON.stringify(column, null, 4)}`
    );
}
