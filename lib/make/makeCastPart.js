const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeCastPart = makeCastPart;

/**
 * Makes an mongo expression tree from the cast statement
 *
 * @param {import('../types').Expression} expr - the AST expression that is a cast
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @returns {*}
 */
function makeCastPart(expr, context) {
    if (expr.type !== 'cast') {
        throw new Error(`Invalid type for cast:${expr.type}`);
    }
    const convertFunction = _allowableFunctions.functionByName('convert');
    if (!convertFunction) {
        throw new Error('No conversion function found');
    }
    const to = expr.target.dataType.toLowerCase();

    if (expr.expr.column) {
        return convertFunction.parse([
            `$${expr.expr.table ? expr.expr.table + '.' : ''}${
                expr.expr.column
            }`,
            to,
        ]);
    }
    if (expr.expr.value) {
        return convertFunction.parse([expr.expr.value, to]);
    }
    return convertFunction.parse([
        makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            expr.expr,
            context
        ),
        to,
    ]);
}
