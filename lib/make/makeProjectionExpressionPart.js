const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeArraySubSelectPartModule = require('./makeArraySubSelectPart');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const makeCastPartModule = require('./makeCastPart');
const $check = require('check-types');

const _allowableFunctions = require('../MongoFunctions');

exports.makeProjectionExpressionPart = makeProjectionExpressionPart;

/**
 * Makes a projection expression sub part.
 *
 * @param {import('../types').Expression} expr - the expression to make a projection from
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {number} [depth] - the current recursive depth
 * @param {boolean} [forceLiteralParse] - Forces parsing of literal expressions
 * @returns {undefined|*}
 */
function makeProjectionExpressionPart(
    expr,
    context,
    depth = 0,
    forceLiteralParse = false
) {
    if (!expr.name && !expr.operator) {
        return makeArg(expr, depth, context);
    }

    // if(expr.type==="number"||expr.type==="string")return expr.value;
    // if(expr.type==="column_ref")return `$${expr.column}`;
    // if(expr.type==="type")return `$${expr.column}`;
    const fn = _allowableFunctions.functionByNameAndType(
        expr.name || expr.operator,
        expr.type
    );
    // const fn = _allowableFunctions.functionMappings.find(
    //     (f) =>
    //         f.name &&
    //         f.name.toLowerCase() ===
    //             (expr.name || expr.operator).toLowerCase() &&
    //         (!f.type || f.type === expr.type)
    // );
    if (!fn) {
        throw new Error(`Function:${expr.name} not available`);
    }

    if ((expr.args && expr.args.value) || fn.doesNotNeedArgs) {
        const argsVal = expr.args ? expr.args.value : [];
        const args = $check.array(argsVal) ? argsVal : [argsVal];
        return fn.parse(
            args.map((a) => makeArg(a, depth, context)),
            depth,
            forceLiteralParse
        );
    } else if (expr.left && expr.right) {
        return getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
            expr,
            context
        );
    } else if (expr.args && expr.args.expr) {
        return fn.parse(
            makeArg(expr.args.expr, depth, context),
            depth,
            forceLiteralParse
        );
    } else {
        return makeArg(expr, depth, context);
        // throw new Error('Unable to parse expression');
    }
}

/**
 *
 * @param {import('../types').Expression} expr
 * @param {number} depth
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 */
function makeArg(expr, depth, context) {
    if (expr.type === 'function' || expr.type === 'aggr_func') {
        return makeProjectionExpressionPart(expr, context, depth);
    }

    if (expr.type === 'column_ref') {
        return `$${expr.table ? expr.table + '.' : ''}${expr.column}`;
    }
    if (expr.type === 'binary_expr') {
        return getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
            expr,
            context,
            null,
            depth
        );
    }
    if (expr.type === 'select' && expr.from) {
        return makeArraySubSelectPartModule.makeArraySubSelectPart(
            expr,
            context,
            depth
        );
    }
    if (expr.type === 'select' && !expr.from) {
        return makeObjectFromSelectModule.makeObjectFromSelect(expr, context);
    }
    if (expr.type === 'unary_expr') {
        if (expr.operator === '-') {
            return {
                $multiply: [
                    -1,
                    makeProjectionExpressionPart(expr.expr, context, depth),
                ],
            };
        } else {
            throw new Error(
                `Unable to parse unary expression:${expr.operator}`
            );
        }
    }
    if (expr.type === 'cast') {
        return makeCastPartModule.makeCastPart(expr, context);
    }

    if (expr.type === 'case') {
        return makeCaseConditionModule.makeCaseCondition(expr, context);
    }
    if (expr.value !== undefined) {
        return {$literal: expr.value};
    }
    if (!expr.type && expr.ast) {
        return makeArg(expr.ast, depth, context);
    }
    throw new Error(`Unable to parse expression type:${expr.type}`);
}
