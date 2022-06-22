const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeCastPart = makeCastPart;

/**
 * Makes an mongo expression tree from the cast statement
 *
 * @param {import('../types').Expression} expr - the AST expression that is a cast
 * @returns {*}
 */
function makeCastPart(expr) {
    if (expr.type !== 'cast') {
        throw new Error(`Invalid type for cast:${expr.type}`);
    }
    const convertFunction = _allowableFunctions.functionMappings.find(
        (f) => f.name === 'convert'
    );
    if (!convertFunction) {
        throw new Error('No conversion function found');
    }
    const to = expr.target.dataType.toLowerCase();

    if (expr.expr.column) {
        return convertFunction.parse([`$${expr.expr.column}`, to]);
    }
    if (expr.expr.value) {
        return convertFunction.parse([expr.expr.value, to]);
    }
    return convertFunction.parse([
        makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            expr.expr
        ),
        to,
    ]);
}
