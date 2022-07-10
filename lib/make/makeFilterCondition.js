const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const {sqlStringToRegex} = require('./sqlStringToRegex');

exports.makeFilterCondition = makeFilterCondition;

const operatorMap = {
    '=': '$eq',
    '>': '$gt',
    '<': '$lt',
    '>=': '$gte',
    '<=': '$lte',
    '!=': '$ne',
    AND: '$and',
    OR: '$or',
};
/**
 * Creates a filter expression from a query part
 *
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @returns {any} - the filter expression
 */
function makeFilterCondition(
    queryPart,
    includeThis = false,
    prefixRight = false,
    side = 'left',
    prefixLeft = false
) {
    if (queryPart.type === 'binary_expr') {
        if (queryPart.operator === 'LIKE') {
            const likeVal = queryPart.right.value;
            const regex = sqlStringToRegex(likeVal);

            return {
                $regexMatch: {
                    input: makeFilterCondition(
                        queryPart.left,
                        includeThis,
                        prefixRight,
                        'left',
                        prefixLeft
                    ),
                    regex: regex,
                    options: 'i',
                },
            };
        }
        const operation = operatorMap[queryPart.operator];
        if (!operation) {
            throw new Error(`Unsupported operator:${queryPart.operator}`);
        }
        return {
            [operation]: [
                makeFilterCondition(
                    queryPart.left,
                    includeThis,
                    prefixRight,
                    'left',
                    prefixLeft
                ),
                makeFilterCondition(
                    queryPart.right,
                    includeThis,
                    prefixRight,
                    'right',
                    prefixLeft
                ),
            ],
        };
    }

    if (queryPart.type === 'unary_expr') {
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            queryPart
        );
    }

    if (queryPart.type === 'function') {
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            queryPart
        );
    }

    if (queryPart.type === 'column_ref') {
        let prefix;
        if (prefixRight && side === 'right') {
            prefix = `$${queryPart.table ? queryPart.table + '.' : ''}`;
        } else if (prefixLeft && side === 'left') {
            prefix = `$${queryPart.table ? queryPart.table + '.' : ''}`;
        } else {
            prefix = '';
        }
        if (includeThis) {
            prefix = `$$this.${prefix}`;
        } else {
            prefix = `$${prefix}`;
        }
        return `${prefix}${queryPart.column}`;
    }

    if (['number', 'string', 'single_quote_string'].includes(queryPart.type)) {
        return queryPart.value;
    }

    throw new Error(
        `invalid expression type for array sub select:${queryPart.type}`
    );
}
