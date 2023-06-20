const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const {sqlStringToRegex} = require('./sqlStringToRegex');
const makeQueryPart = require('./makeQueryPart');
const $check = require('check-types');

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
    IS: '$eq',
    'IS NOT': '$ne',
};

const queryOperatorMap = {
    '-': '$subtract',
    '+': '$add',
    '/': '$divide',
    '*': '$multiply',
    IN: '$in',
    'NOT IN': '$nin',
};
/**
 * Creates a filter expression from a query part
 *
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @returns {any} - the filter expression
 */
function makeFilterCondition(
    queryPart,
    includeThis = false,
    prefixRight = false,
    side = 'left',
    prefixLeft = false,
    prefixTable = false,
    aliases = []
) {
    const binaryResult = processBinaryExpression(
        queryPart,
        includeThis,
        prefixRight,
        side,
        prefixLeft,
        prefixTable,
        aliases
    );
    if (binaryResult) {
        return binaryResult;
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
        const foundAlias = aliases.find((a) => a === queryPart.table);
        let prefix = '';
        if (aliases.length) {
            if (foundAlias) {
                prefix = foundAlias + '.';
            }
        } else {
            if (prefixRight && side === 'right') {
                prefix = `$${queryPart.table ? queryPart.table + '.' : ''}`;
            } else if (prefixLeft && side === 'left') {
                prefix = `$${queryPart.table ? queryPart.table + '.' : ''}`;
            } else if (prefixTable) {
                prefix = `${queryPart.table ? queryPart.table + '.' : ''}`;
            }
        }

        if (includeThis) {
            prefix = `$$this.${prefix}`;
        } else {
            prefix = `$${prefix}`;
        }
        return `${prefix}${queryPart.column}`;
    }

    if (
        ['bool', 'number', 'string', 'single_quote_string'].includes(
            queryPart.type
        )
    ) {
        return queryPart.value;
    }

    if (queryPart.type === 'null') {
        return null;
    }

    throw new Error(
        `invalid expression type for array sub select:${queryPart.type}`
    );
}

/**
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @returns {any} - the filter expression
 */
function processBinaryExpression(
    queryPart,
    includeThis,
    prefixRight,
    side,
    prefixLeft,
    prefixTable,
    aliases
) {
    if (queryPart.type !== 'binary_expr') {
        return;
    }
    let result;
    result = processLikeExpression(
        queryPart,
        includeThis,
        prefixRight,
        side,
        prefixLeft,
        prefixTable,
        aliases
    );
    if (result) {
        return result;
    }
    result = processNotLikeExpression(
        queryPart,
        includeThis,
        prefixRight,
        side,
        prefixLeft,
        prefixTable,
        aliases
    );
    if (result) {
        return result;
    }

    result = processQueryOperator(
        queryPart,
        includeThis,
        prefixRight,
        side,
        prefixLeft,
        prefixTable,
        aliases
    );
    if (result) {
        return result;
    }
    return processOperator(
        queryPart,
        includeThis,
        prefixRight,
        side,
        prefixLeft,
        prefixTable,
        aliases
    );
}

/**
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @returns {any} - the filter expression
 */
function processLikeExpression(
    queryPart,
    includeThis,
    prefixRight,
    side,
    prefixLeft,
    prefixTable,
    aliases
) {
    if (queryPart.operator !== 'LIKE') {
        return;
    }
    const likeVal = queryPart.right.value;
    const regex = sqlStringToRegex(likeVal);

    return {
        $regexMatch: {
            input: makeFilterCondition(
                queryPart.left,
                includeThis,
                prefixRight,
                'left',
                prefixLeft,
                prefixTable,
                aliases
            ),
            regex: regex,
            options: 'i',
        },
    };
}

/**
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @returns {any} - the filter expression
 */
function processNotLikeExpression(
    queryPart,
    includeThis,
    prefixRight,
    side,
    prefixLeft,
    prefixTable,
    aliases
) {
    if (queryPart.operator !== 'NOT LIKE') {
        return;
    }
    const likeVal = queryPart.right.value;
    const regexString = sqlStringToRegex(likeVal);
    const input = makeFilterCondition(
        queryPart.left,
        includeThis,
        prefixRight,
        'left',
        prefixLeft,
        prefixTable,
        aliases
    );
    return {
        $not: [
            {
                $regexMatch: {
                    input,
                    regex: regexString,
                    options: 'i',
                },
            },
        ],
    };
}

/**
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @returns {any} - the filter expression
 */
function processQueryOperator(
    queryPart,
    includeThis,
    prefixRight,
    side,
    prefixLeft,
    prefixTable,
    aliases
) {
    const queryOperator = queryOperatorMap[queryPart.operator];
    if (!queryOperator) {
        return;
    }
    const left = makeQueryPart.makeQueryPart(
        queryPart.left,
        false,
        [],
        includeThis
    );
    return {
        [queryOperator]: [
            $check.string(left) ? `$${left}` : left,
            makeQueryPart.makeQueryPart(
                queryPart.right,
                false,
                [],
                includeThis
            ),
        ],
    };
}

/**
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @returns {any} - the filter expression
 */
function processOperator(
    queryPart,
    includeThis,
    prefixRight,
    side,
    prefixLeft,
    prefixTable,
    aliases
) {
    const operation = operatorMap[queryPart.operator];
    if (!operation) {
        throw new Error(`Unsupported operator:${queryPart.operator}`);
    }
    const firstFilter = makeFilterCondition(
        queryPart.left,
        includeThis,
        prefixRight,
        'left',
        prefixLeft,
        prefixTable,
        aliases
    );
    const secondFilter = makeFilterCondition(
        queryPart.right,
        includeThis,
        prefixRight,
        'right',
        prefixLeft,
        prefixTable,
        aliases
    );
    return {
        [operation]: [firstFilter, secondFilter],
    };
}
