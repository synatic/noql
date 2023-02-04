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

function getAggrFunctionsForColumn(column) {
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

/**
 * @param {import('../types').Column} column The column to parse
 * @param {import('../types').ColumnParseResult} result the result object
 * @param {number} [depth] - the depth of the expression allowing for dynamic field names
 * @returns {void}
 */
function groupByColumnParser(column, result, depth = 0) {
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

    if (
        column.expr.type === 'aggr_func' &&
        column.as &&
        column.expr.name === 'COUNT' &&
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
        const aggrFunctions = getAggrFunctionsForColumn(column);
        if (aggrFunctions.length > 0) {
            result.groupByProject = {
                [column.as]:
                    getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                        column.expr,
                        (col, depth, aggrName) => {
                            groupByColumnParser(
                                {expr: col, as: aggrName},
                                result,
                                depth + 1
                            );
                        },
                        0
                    ),
            };
        } else {
            result.groupBy.$group._id[column.as] =
                getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                    column.expr
                );
        }

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
