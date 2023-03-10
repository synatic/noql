const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.getParsedValueFromBinaryExpression = getParsedValueFromBinaryExpression;

/**
 * Get the value from a binary expression
 *
 * @param {import('../types').Expression} expressionPart - the expression to turn into a value
 * @param {Function} [groupByColumnParserFn] - the group by parser function passed to manage group by clauses
 * @param {number} [depth] - the depth of the expression allowing for dynamic field names
 * @returns {string|undefined|*}
 */
function getParsedValueFromBinaryExpression(
    expressionPart,
    groupByColumnParserFn,
    depth = 0
) {
    depth = depth + 1;

    if (expressionPart.type === 'binary_expr') {
        return makeBinaryExpressionPart(
            expressionPart,
            groupByColumnParserFn,
            depth
        );
    }
    if (expressionPart.type === 'column_ref') {
        return `$${expressionPart.table ? expressionPart.table + '.' : ''}${
            expressionPart.column
        }`;
    }
    if (['single_quote_string', 'string'].includes(expressionPart.type)) {
        return expressionPart.value;
    }
    if (['number'].includes(expressionPart.type)) {
        return expressionPart.value;
    }
    if (expressionPart.type === 'function') {
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            expressionPart
        );
    }
    if (expressionPart.type === 'aggr_func' && groupByColumnParserFn) {
        const aggrName = `_tempAggregateCol_${depth}`;
        groupByColumnParserFn(expressionPart, depth, aggrName);
        return `$${aggrName}`;
    }
    throw new Error(
        `Unable to make binary expression part:${expressionPart.type}`
    );
}

/**
 * Translates a binary expression into a mongo usable part
 *
 * @param {import('../types').Expression} expr - the ast expression
 * @param {Function} [groupByColumnParserFn] - the group by parser function passed to manage group by clauses
 * @param {number} [depth] - the depth of the expression allowing for dynamic field names
 * @returns {*}
 */
function makeBinaryExpressionPart(expr, groupByColumnParserFn, depth = 0) {
    depth = depth + 1;
    let operator;
    if (expr.expr) {
        operator = expr.expr.operator;
    } else {
        operator = expr.operator;
    }

    const exprFunction = _allowableFunctions.functionMappings.find(
        (f) => f.name === operator.toLowerCase()
    );
    let exprResult;
    if (!exprFunction) throw new Error(`Expression not found:${operator}`);

    if (expr.expr && expr.expr.left && expr.expr.right) {
        const leftPartValue = getParsedValueFromBinaryExpression(
            expr.expr.left,
            groupByColumnParserFn,
            depth + 1
        );
        const rightPartValue = getParsedValueFromBinaryExpression(
            expr.expr.right,
            groupByColumnParserFn,
            depth + 2
        );

        exprResult = exprFunction.parse(leftPartValue, rightPartValue);
    } else if (expr.left && expr.right) {
        const leftPartValue = getParsedValueFromBinaryExpression(
            expr.left,
            groupByColumnParserFn,
            depth + 1
        );
        const rightPartValue = getParsedValueFromBinaryExpression(
            expr.right,
            groupByColumnParserFn,
            depth + 2
        );

        exprResult = exprFunction.parse(leftPartValue, rightPartValue);
    }

    return exprResult;
}
