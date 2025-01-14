const _allowableFunctions = require('../MongoFunctions');
const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeCastPartModule = require('./makeCastPart');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const $json = require('@synatic/json-magic');

exports.groupByColumnParser = groupByColumnParser;
exports.getAggrFunctionsForColumn = getAggrFunctionsForColumn;

/**
 *
 * @param column
 * @param {import('../types').NoqlContext} _context - The Noql context to use when generating the output
 */
function getAggrFunctionsForColumn(column, _context) {
    const potentialFuncs = [];
    const aggrFunctions = [];
    $json.walk(column, (val, path) => {
        const pathParts = path.split('/').slice(1);
        if (val === 'aggr_func') {
            potentialFuncs.push(
                pathParts.slice(0, pathParts.length - 1).join('.')
            );
        }
    });

    for (const potentialFunc of potentialFuncs) {
        aggrFunctions.push({
            path: potentialFunc,
            expr: $json.get(column, potentialFunc),
        });
    }

    return aggrFunctions;
}

// todo there are more
const expressionNamesToExcludeFromId = ['count', 'sum', 'min', 'max'];
const expressionsToExpand = [{name: 'round', expressionKey: '$round'}];
/**
 * @param {import('../types').Column} column The column to parse
 * @param {import('../types').ColumnParseResult} result the result object
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {number} [depth] - the depth of the expression allowing for dynamic field names
 * @returns {void}
 */
function groupByColumnParser(column, result, context, depth = 0) {
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
                column.expr,
                context
            );
        const lowerColumnName = column.expr.name.toLowerCase();
        if (expressionNamesToExcludeFromId.indexOf(lowerColumnName) >= 0) {
            result.groupBy.$group[column.as] = parsedExpr;
            return; // count values can't go in the groupBy
        }
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
            return;
        }
        const expand = expressionsToExpand.find(
            (e) => e.name === lowerColumnName
        );
        if (expand) {
            const [colExpression, precision] = parsedExpr[expand.expressionKey];
            if (typeof colExpression !== 'string') {
                result.groupBy.$group[column.as] = colExpression;
                result.groupByProject = result.groupByProject || {};
                result.groupByProject[column.as] = {
                    [expand.expressionKey]: precision
                        ? [`$${column.as}`, precision]
                        : [`$${column.as}`],
                };
                return;
            }
        }
        result.groupBy.$group._id[column.as] = parsedExpr;
        return;
    }

    if (
        column.expr.type === 'aggr_func' &&
        column.as &&
        column.expr.name &&
        column.expr.name.toUpperCase() === 'COUNT' &&
        column.expr.args &&
        column.expr.args.distinct === 'DISTINCT'
    ) {
        if (
            !column.expr.args.expr ||
            column.expr.args.expr.type !== 'column_ref'
        ) {
            throw new Error(
                'COUNT DISTINCT requires a column ref :' +
                    (column.expr.args.expr
                        ? column.expr.args.expr.type
                        : 'No Expression provided')
            );
        }

        // todo dont override existing group
        const countDistinctField =
            (column.expr.args.expr.table
                ? column.expr.args.expr.table + '.'
                : '') + column.expr.args.expr.column;
        result.countDistinct = column.as;
        result.groupBy.$group._id._countDistinctTemp = `$${countDistinctField}`;
    }

    if (column.expr.type === 'aggr_func' && column.as) {
        const aggregateFunction = _allowableFunctions.functionByNameAndType(
            column.expr.name,
            'aggr_func'
        );
        if (!aggregateFunction) {
            throw new Error(`Function not found:${column.expr.name}`);
        }
        result.groupBy.$group[column.as] =
            makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                column.expr,
                context
            );
        return;
    }

    if (column.expr.type === 'binary_expr' && column.as) {
        const aggrFunctions = getAggrFunctionsForColumn(column, context);
        if (aggrFunctions.length > 0) {
            result.groupByProject = {
                [column.as]:
                    getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                        column.expr,
                        context,
                        (col, depth, aggrName) => {
                            groupByColumnParser(
                                {expr: col, as: aggrName},
                                result,
                                context,
                                depth + 1
                            );
                        },
                        0
                    ),
            };
        } else {
            result.groupBy.$group._id[column.as] =
                getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                    column.expr,
                    context
                );
        }

        return;
    }

    if (column.expr.type === 'case' && column.as) {
        result.groupBy.$group._id[column.as] =
            makeCaseConditionModule.makeCaseCondition(column.expr, context);
        return;
    }

    if (column.expr.type === 'cast' && column.as) {
        result.groupBy.$group._id[column.as] = makeCastPartModule.makeCastPart(
            column.expr,
            context
        );
        return;
    }

    if (column.expr.type === 'select' && column.as && column.expr.from) {
        result.groupBy.$group._id[column.as] =
            makeArraySubSelectPartModule.makeArraySubSelectPart(
                column.expr,
                context
            );
        return;
    }

    if (column.expr.type === 'select' && column.as && !column.expr.from) {
        result.groupBy.$group._id[column.as] =
            makeObjectFromSelectModule.makeObjectFromSelect(
                column.expr,
                context
            );
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
