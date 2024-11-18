const makeFilterConditionModule = require('./makeFilterCondition');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeCaseCondition = makeCaseCondition;

/**
 * Makes a $cond from a case statement
 * @param {import('../types').Expression} expr - the expression object to turn into a case
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @returns {any}
 */
function makeCaseCondition(expr, context) {
    if (expr.type !== 'case') {
        throw new Error(`Expression is not case`);
    }

    const elseExpr = expr.args.find((a) => a.type === 'else');
    const whens = expr.args.filter((a) => a.type === 'when');

    return {
        $switch: {
            branches: whens.map((w) => {
                return {
                    case: makeFilterConditionModule.makeFilterCondition(
                        w.cond,
                        context,
                        false,
                        false,
                        null,
                        false,
                        true,
                        undefined,
                        true
                    ),
                    then: makeFilterConditionModule.makeFilterCondition(
                        w.result,
                        context,
                        false,
                        false,
                        null,
                        false,
                        true
                    ),
                };
            }),
            default:
                makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                    elseExpr.result,
                    context
                ),
        },
    };
}
