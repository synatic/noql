const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeCastPartModule = require('./makeCastPart');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const $check = require('check-types');
const {functionByName} = require('../MongoFunctions');

exports.projectColumnParser = projectColumnParser;

/**
 * @param {import('../types').Column} column The column to parse
 * @param {import('../types').ColumnParseResult} result the result object
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {string} [tableAlias] - a table alias to check if it hasn't been specified
 * @returns {void}
 */
function projectColumnParser(column, result, context, tableAlias = '') {
    if (column.expr.type === 'column_ref') {
        const columnTable = column.expr.table || tableAlias;
        const columnValue = column.expr.column;
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {
                $replaceRoot: {newRoot: `$${columnValue}`},
            };
            return;
        }
        if (columnValue === '*' && column.expr.table) {
            result.parsedProject.$project[column.as || column.expr.table] =
                `$${column.expr.table}`;
            return;
        }
        if (columnValue === '*') {
            result.exprToMerge.push('$$ROOT');
            return;
        }
        const expression =
            columnTable === columnValue.split('.')[0]
                ? `$${columnValue}`
                : `$${columnTable ? columnTable + '.' : ''}${
                      column.expr.column
                  }`;
        result.parsedProject.$project[column.as || column.expr.column] =
            expression;
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
                column.expr.args.value[0],
                context
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
                column.expr,
                context
            );
        if (!result.unset) {
            result.unset = parsedExpr;
        } else if (result.unset.$unset && result.unset.$unset.length) {
            result.unset.$unset.push(...parsedExpr.$unset);
        } else {
            result.unset = parsedExpr;
        }
        return;
    }
    if (column.expr.type === 'function' && column.as) {
        if (column.expr.name.toLowerCase() === 'count') {
            result.count.push({$count: column.as});
            return;
        }
        const parsedExpr =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr,
                context
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
                column.expr,
                context
            );
        return;
    }
    if (column.expr.type === 'binary_expr' && column.as) {
        result.parsedProject.$project[column.as] =
            getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                column.expr,
                context
            );
        return;
    }
    if (column.expr.type === 'case' && column.as) {
        result.parsedProject.$project[column.as] =
            makeCaseConditionModule.makeCaseCondition(column.expr, context);
        return;
    }
    if (column.expr.type === 'cast' && column.as) {
        result.parsedProject.$project[column.as] =
            makeCastPartModule.makeCastPart(column.expr, context);
        return;
    }
    if (column.expr.type === 'select' && column.as && column.expr.from) {
        result.parsedProject.$project[column.as] =
            makeArraySubSelectPartModule.makeArraySubSelectPart(
                column.expr,
                context
            );
        return;
    }
    if (column.expr.type === 'select' && column.as && !column.expr.from) {
        result.parsedProject.$project[column.as] =
            makeObjectFromSelectModule.makeObjectFromSelect(
                column.expr,
                context
            );
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
    if (column.expr.type === 'function' && !column.as) {
        const fn = functionByName(column.expr.name || column.expr.value);
        if (fn.requiresAs) {
            throw new Error(
                `Require as for calculation:${
                    column.expr.name || column.expr.value
                }`
            );
        }
        const parsedExpr =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr,
                context
            );
        let applied = 0;
        if (parsedExpr.$replaceRoot) {
            result.replaceRoot = {$replaceRoot: parsedExpr.$replaceRoot};
            applied++;
        }
        if (parsedExpr.$unset) {
            result.unset = {$unset: parsedExpr.$unset};
            applied++;
        }
        const keys = Object.keys(parsedExpr);
        if (applied === 0) {
            throw new Error(
                `Logic Not implemented for function that does not requireAs and returned ${keys.join(', ')}`
            );
        }
        if (keys.length !== applied) {
            throw new Error(
                `Logic only partially implemented for function that does not requireAs and returned ${keys.join(', ')}`
            );
        }
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
                column.expr.args.source,
                context
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
                    partitionby[0].expr,
                    context
                );
            setWindowFunction.partitionBy = res;
        }
        for (const order of orderby) {
            const res = makeProjectionExpressionPartModule
                .makeProjectionExpressionPart(order.expr, context)
                .replace(/\$/g, '');
            order.type = order.type || 'ASC';
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
        return projectColumnParser(
            {...column, expr: column.expr.ast},
            result,
            context
        );
    }
    throw new Error(
        `Column not supported:\n${JSON.stringify(column, null, 4)}`
    );
}
