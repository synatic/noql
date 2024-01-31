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
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
    context,
    includeThis = false,
    prefixRight = false,
    side = 'left',
    prefixLeft = false,
    prefixTable = false,
    aliases = []
) {
    const binaryResult = processBinaryExpression(
        queryPart,
        context,
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
            queryPart,
            context
        );
    }

    if (queryPart.type === 'function') {
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            queryPart,
            context
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
        [
            'bool',
            'number',
            'string',
            'single_quote_string',
            'double_quote_string',
        ].includes(queryPart.type)
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
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
    context,
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
        context,
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
        context,
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
        context,
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
        context,
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
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
    context,
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
    /** @type {string} */
    let regex;
    if (queryPart.right.value) {
        regex = sqlStringToRegex(queryPart.right.value);
    } else {
        regex = queryPart.right.table
            ? `$${queryPart.right.table}.${queryPart.right.column}`
            : queryPart.right.column;
    }

    return {
        $regexMatch: {
            input: makeFilterCondition(
                queryPart.left,
                context,
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
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
    context,
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
        context,
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
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
    context,
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
        context,
        false,
        [],
        includeThis
    );
    return {
        [queryOperator]: [
            $check.string(left) ? `$${left}` : left,
            makeQueryPart.makeQueryPart(
                queryPart.right,
                context,
                false,
                [],
                includeThis
            ),
        ],
    };
}

/**
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
    context,
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
        context,
        includeThis,
        prefixRight,
        'left',
        prefixLeft,
        prefixTable,
        aliases
    );
    const secondFilter = makeFilterCondition(
        queryPart.right,
        context,
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
