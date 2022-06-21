const makeFilterConditionModule = require('./makeFilterCondition');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeCaseCondition = makeCaseCondition;

/**
 * Makes a $cond from a case statement
 *
 * @param {import('../types').Expression} expr - the expression object to turn into a case
 * @returns {any}
 */
function makeCaseCondition(expr) {
    if (expr.type !== 'case') {
        throw new Error(`Expression is not case`);
    }

    const elseExpr = expr.args.find((a) => a.type === 'else');
    const whens = expr.args.filter((a) => a.type === 'when');

    return {
        $switch: {
            branches: whens.map((w) => {
                return {
                    case: makeFilterConditionModule.makeFilterCondition(w.cond),
                    then: makeFilterConditionModule.makeFilterCondition(
                        w.result
                    ),
                };
            }),
            default:
                makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                    elseExpr.result
                ),
        },
    };
}
