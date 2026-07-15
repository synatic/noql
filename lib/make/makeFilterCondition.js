const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const {sqlStringToRegex} = require('./sqlStringToRegex');
const makeQueryPart = require('./makeQueryPart');
const $check = require('check-types');
const makeCaseConditionModule = require('./makeCaseCondition');

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
 * @param {import('../types').Expression} queryPart - The query part to create filter
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @param {boolean} [prefixLeft] - include $$ for inner variables
 * @param {boolean} [prefixTable] - include the table in the prefix
 * @param {string[]} [aliases] - the aliases used in the joins
 * @param {boolean} [inSwitchStatement] - specifies if we are in a switch for special conditions, default false
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
    aliases = [],
    inSwitchStatement = false
) {
    const binaryResult = processBinaryExpression(
        queryPart,
        context,
        includeThis,
        prefixRight,
        side,
        prefixLeft,
        prefixTable,
        aliases,
        inSwitchStatement
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
        let fieldPath = queryPart.column;
        if (aliases.length) {
            // Join ON-condition resolution against known join aliases: keep
            // this untouched. An extra dotted segment here often carries a
            // leftover qualifier from before a sub-select flattened its
            // output columns (e.g. `alias.originalTable.column`), not real
            // nested document structure, so `schema` is intentionally not
            // consulted in this branch.
            if (foundAlias) {
                prefix = foundAlias + '.';
            }
        } else {
            // `schema` holds the true table alias for a nested (3+ level)
            // dotted path, e.g. `s.a.b` parses as
            // {schema:'s', table:'a', column:'b'}.
            fieldPath = queryPart.schema
                ? `${queryPart.table}.${queryPart.column}`
                : queryPart.column;
            const tableOrAlias = queryPart.schema || queryPart.table;
            if (prefixRight && side === 'right') {
                prefix = `$${tableOrAlias ? tableOrAlias + '.' : ''}`;
            } else if (prefixLeft && side === 'left') {
                prefix = `$${tableOrAlias ? tableOrAlias + '.' : ''}`;
            } else if (prefixTable) {
                prefix = `${tableOrAlias ? tableOrAlias + '.' : ''}`;
            }
        }

        if (includeThis) {
            prefix = `$$this.${prefix}`;
        } else {
            prefix = `$${prefix}`;
        }
        return `${prefix}${fieldPath}`;
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
    if (queryPart.type === 'case') {
        return makeCaseConditionModule.makeCaseCondition(queryPart, context);
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
 * @param {boolean} [inSwitchStatement] - specifies if we are in a switch for special conditions, default false
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
    aliases,
    inSwitchStatement = false
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
        aliases,
        inSwitchStatement
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
        aliases,
        inSwitchStatement
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
 * @param {boolean} [inSwitchStatement] - specifies if we are in a switch for special conditions, default false
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
    aliases,
    inSwitchStatement = false
) {
    const queryOperator = queryOperatorMap[queryPart.operator];
    if (!queryOperator) {
        return;
    }
    const left = makeQueryOperatorOperand(
        queryPart.left,
        context,
        includeThis,
        prefixRight,
        'left',
        prefixLeft,
        prefixTable,
        aliases,
        inSwitchStatement
    );
    const right = makeQueryOperatorOperand(
        queryPart.right,
        context,
        includeThis,
        prefixRight,
        'right',
        prefixLeft,
        prefixTable,
        aliases,
        inSwitchStatement
    );
    if (inSwitchStatement && queryOperator === '$nin') {
        return {
            $not: {
                $in: [
                    formatQueryOperatorValue(left),
                    formatQueryOperatorValue(right),
                ],
            },
        };
    }
    return {
        [queryOperator]: [
            formatQueryOperatorValue(left),
            $check.array(right) ? right : formatQueryOperatorValue(right),
        ],
    };
}

/**
 * @param {import('../types').Expression} expr
 * @param {import('../types').NoqlContext} context
 * @param {boolean} includeThis
 * @param {boolean} prefixRight
 * @param {string} side
 * @param {boolean} prefixLeft
 * @param {boolean} prefixTable
 * @param {string[]} aliases
 * @param {boolean} inSwitchStatement
 * @returns {any}
 */
function makeQueryOperatorOperand(
    expr,
    context,
    includeThis,
    prefixRight,
    side,
    prefixLeft,
    prefixTable,
    aliases,
    inSwitchStatement
) {
    if (expr.type === 'expr_list') {
        return expr.value.map((value) =>
            makeQueryOperatorOperand(
                value,
                context,
                includeThis,
                prefixRight,
                side,
                prefixLeft,
                prefixTable,
                aliases,
                inSwitchStatement
            )
        );
    }
    if (expr.type === 'column_ref' && aliases && aliases.length > 0) {
        return makeFilterCondition(
            expr,
            context,
            includeThis,
            prefixRight,
            side,
            prefixLeft,
            prefixTable,
            aliases,
            inSwitchStatement
        );
    }
    if (queryOperatorMap[expr.operator]) {
        return makeFilterCondition(expr, context, false);
    }
    return makeQueryPart.makeQueryPart(
        expr,
        context,
        false,
        [],
        includeThis
    );
}

/**
 * @param {any} value
 * @returns {any}
 */
function formatQueryOperatorValue(value) {
    if (!$check.string(value)) {
        return value;
    }
    if (value.startsWith('$')) {
        return value;
    }
    return `$${value}`;
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
 * @param {boolean} [inSwitchStatement] - specifies if we are in a switch for special conditions, default false
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
    aliases,
    inSwitchStatement = false
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
        aliases,
        inSwitchStatement
    );
    const secondFilter = makeFilterCondition(
        queryPart.right,
        context,
        includeThis,
        prefixRight,
        'right',
        prefixLeft,
        prefixTable,
        aliases,
        inSwitchStatement
    );
    return {
        [operation]: [firstFilter, secondFilter],
    };
}
