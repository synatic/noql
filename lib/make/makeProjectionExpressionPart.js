const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const $check = require('check-types');

const _allowableFunctions = require('../MongoFunctions');

exports.makeProjectionExpressionPart = makeProjectionExpressionPart;

/**
 * Makes a projection expression sub part.
 *
 * @param {import('../types').Expression} expr - the expression to make a projection from
 * @param {number} [depth] - the current recursive depth
 * @param {boolean} [forceLiteralParse] - Forces parsing of literal expressions
 * @returns {undefined|*}
 */
function makeProjectionExpressionPart(
    expr,
    depth = 0,
    forceLiteralParse = false
) {
    if (!expr.name && !expr.operator) {
        return makeArg(expr, depth);
    }

    // if(expr.type==="number"||expr.type==="string")return expr.value;
    // if(expr.type==="column_ref")return `$${expr.column}`;
    // if(expr.type==="type")return `$${expr.column}`;
    const fn = _allowableFunctions.functionMappings.find(
        (f) =>
            f.name &&
            f.name.toLowerCase() ===
                (expr.name || expr.operator).toLowerCase() &&
            (!f.type || f.type === expr.type)
    );
    if (!fn) {
        throw new Error(`Function:${expr.name} not available`);
    }

    if (expr.args && expr.args.value) {
        const args = $check.array(expr.args.value)
            ? expr.args.value
            : [expr.args.value];
        return fn.parse(
            args.map((a) => makeArg(a, depth)),
            depth,
            forceLiteralParse
        );
    } else if (expr.left && expr.right) {
        return getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
            expr
        );
    } else if (expr.args && expr.args.expr) {
        return fn.parse(
            makeArg(expr.args.expr, depth),
            depth,
            forceLiteralParse
        );
    } else {
        return makeArg(expr, depth);
        // throw new Error('Unable to parse expression');
    }
}

/**
 *
 * @param {import('../types').Expression} expr
 * @param {number} depth
 */
function makeArg(expr, depth) {
    // todo check all these types
    if (expr.type === 'function') {
        return makeProjectionExpressionPart(expr);
    } else if (expr.type === 'column_ref') {
        return `$${expr.table ? expr.table + '.' : ''}${expr.column}`;
    } else if (expr.type === 'binary_expr') {
        return getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
            expr
        );
    } else if (expr.type === 'select' && expr.from) {
        return makeArraySubSelectPartModule.makeArraySubSelectPart(expr, depth);
    } else if (expr.type === 'select' && !expr.from) {
        return makeObjectFromSelectModule.makeObjectFromSelect(expr);
    } else if (expr.type === 'unary_expr') {
        if (expr.operator === '-') {
            return {$multiply: [-1, makeProjectionExpressionPart(expr.expr)]};
        } else {
            throw new Error(
                `Unable to parse unary expression:${expr.operator}`
            );
        }
    } else if (expr.type === 'case') {
        return makeCaseConditionModule.makeCaseCondition(expr);
    } else if (expr.value !== undefined) {
        return {$literal: expr.value};
    } else {
        throw new Error(`Unable to parse expression type:${expr.type}`);
    }
}
