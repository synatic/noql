const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.getParsedValueFromBinaryExpression = getParsedValueFromBinaryExpression;

/**
 * Get the value from a binary expression
 *
 * @param {import('../types').Expression} expressionPart - the expression to turn into a value
 * @returns {string|undefined|*}
 */
function getParsedValueFromBinaryExpression(expressionPart) {
    if (expressionPart.type === 'binary_expr') {
        return makeBinaryExpressionPart(expressionPart);
    }
    if (expressionPart.type === 'column_ref') {
        return `$${expressionPart.column}`;
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

    throw new Error(
        `Unable to make binary expression part:${expressionPart.type}`
    );
}

/**
 * Translates a binary expression into a mongo usable part
 *
 * @param {import('../types').Expression} expr - the ast expression
 * @returns {*}
 */
function makeBinaryExpressionPart(expr) {
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
            expr.expr.left
        );
        const rightPartValue = getParsedValueFromBinaryExpression(
            expr.expr.right
        );

        exprResult = exprFunction.parse(leftPartValue, rightPartValue);
    } else if (expr.left && expr.right) {
        const leftPartValue = getParsedValueFromBinaryExpression(expr.left);
        const rightPartValue = getParsedValueFromBinaryExpression(expr.right);

        exprResult = exprFunction.parse(leftPartValue, rightPartValue);
    }

    return exprResult;
}
