/**
 * Finds all the queries in a an AST where statement that are AST's themselves
 *
 * @param {import('../types').Expression} where
 * @returns {{column:string,ast:import('../types').AST[]}[]}
 */
function getWhereAstQueries(where) {
    if (where.right.value) {
        return [
            {
                column: where.left.column,
                ast: where.right.value.filter(notAStandardValue),
            },
        ];
    }
    if (where.left.value) {
        return [
            {
                column: where.right.column,
                ast: where.left.value.filter(notAStandardValue),
            },
        ];
    }
    if (where.left.left || where.right.left) {
        let queries = [];
        if (where.left.left.value && isAstQuery(where.left.left)) {
            queries = queries.concat(getWhereAstQueries(where.left));
        }
        if (where.left.right.value && isAstQuery(where.left.right)) {
            queries = queries.concat(getWhereAstQueries(where.left));
        }
        if (where.right.left.value && isAstQuery(where.right.left)) {
            queries = queries.concat(getWhereAstQueries(where.right));
        }
        if (where.right.right.value && isAstQuery(where.right.right)) {
            queries = queries.concat(getWhereAstQueries(where.right));
        }
        return queries;
    }
    throw new Error('Not implemented');
}

/**
 * Finds all the queries in a an AST where statement that are not AST's themselves
 *
 * @param {import('../types').Expression} where
 * @returns {import('../types').Expression[]}
 */
function getWhereStandardQueries(where) {
    if (where.left.left || where.right.left) {
        let queries = [];
        if (where.left.left) {
            queries = queries.concat(getWhereStandardQueries(where.left));
        }
        if (where.right.left) {
            queries = queries.concat(getWhereStandardQueries(where.right));
        }
        return queries;
    }
    if (notAnAstQuery(where.left) && notAnAstQuery(where.right)) {
        return [where];
    }
    return [];
}

/**
 *
 * @param {import('../types').Expression} expr
 * @returns {boolean}
 */
function notAnAstQuery(expr) {
    return (
        !expr.value ||
        !Array.isArray(expr.value) ||
        (Array.isArray(expr.value) && expr.value.every(isStandardValue))
    );
}

/**
 *
 * @param {import('../types').Expression} expr
 * @returns {boolean}
 */
function isAstQuery(expr) {
    return expr.value.every && !expr.value.every(isStandardValue);
}
/**
 *
 * @param {{type:string}} val
 * @returns {boolean}
 */
function isStandardValue(val) {
    return ['number', 'string', 'single_quote_string'].includes(val.type);
}
/**
 *
 * @param {{type:string}} val
 * @returns {boolean}
 */
function notAStandardValue(val) {
    return !isStandardValue(val);
}

module.exports = {getWhereStandardQueries, getWhereAstQueries};
