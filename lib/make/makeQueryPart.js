const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const $check = require('check-types');
const {sqlStringToRegex} = require('./sqlStringToRegex');

exports.makeQueryPart = makeQueryPart;

/**
 * Parses a AST QueryPart into a Mongo Query/Match
 *
 * @param {import('../types').Expression} queryPart - The AST query part
 * @param {boolean} [ignorePrefix] - Ignore the table prefix
 * @param {Array}  [allowedTypes] - Expression types to allow
 * @param {boolean} [includeThis] - include $$this in expresions
 * @returns {any} - the mongo query/match
 */
function makeQueryPart(
    queryPart,
    ignorePrefix,
    allowedTypes = [],
    includeThis = false
) {
    if (allowedTypes.length > 0 && !allowedTypes.includes(queryPart.type)) {
        throw new Error(`Type not allowed for query:${queryPart.type}`);
    }

    const getColumnNameOrVal = (queryPart) => {
        let queryPartToUse = queryPart;
        if (queryPart.left) {
            queryPartToUse = queryPart.left;
        }

        if (queryPartToUse.column) {
            return (
                (includeThis ? '$$this.' : '') +
                (queryPartToUse.table && !ignorePrefix
                    ? `${queryPartToUse.table}.${queryPartToUse.column}`
                    : queryPartToUse.column)
            );
        } else {
            return queryPartToUse.value;
        }
    };

    const makeOperator = (op) => {
        const left = makeQueryPart(
            queryPart.left,
            ignorePrefix,
            allowedTypes,
            includeThis
        );
        const right = makeQueryPart(
            queryPart.right,
            ignorePrefix,
            allowedTypes,
            includeThis
        );
        if ($check.string(left) && !left.startsWith('$')) {
            return {[left]: {[op]: right}};
        } else {
            return {$expr: {[op]: [left, right]}};
        }
    };

    if (queryPart.type === 'binary_expr') {
        if (queryPart.operator === '=') return makeOperator('$eq');
        if (queryPart.operator === '>') return makeOperator('$gt');
        if (queryPart.operator === '<') return makeOperator('$lt');
        if (queryPart.operator === '>=') return makeOperator('$gte');
        if (queryPart.operator === '<=') return makeOperator('$lte');
        if (queryPart.operator === '!=') return makeOperator('$ne');
        if (queryPart.operator === 'AND') {
            return {
                $and: [
                    makeQueryPart(
                        queryPart.left,
                        ignorePrefix,
                        allowedTypes,
                        includeThis
                    ),
                    makeQueryPart(
                        queryPart.right,
                        ignorePrefix,
                        allowedTypes,
                        includeThis
                    ),
                ],
            };
        }
        if (queryPart.operator === 'OR') {
            return {
                $or: [
                    makeQueryPart(
                        queryPart.left,
                        ignorePrefix,
                        allowedTypes,
                        includeThis
                    ),
                    makeQueryPart(
                        queryPart.right,
                        ignorePrefix,
                        allowedTypes,
                        includeThis
                    ),
                ],
            };
        }
        if (queryPart.operator === 'IN') {
            return makeOperator('$in');
            // return {$in: [makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
        }
        if (queryPart.operator === 'NOT IN') {
            return makeOperator('$nin');
            // return {$in: [makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
        }
        if (queryPart.operator === 'LIKE') {
            const likeVal = queryPart.right.value;
            const regex = sqlStringToRegex(likeVal);
            // if(isWrappedExpr){
            //     return {$and:[{[getColumnNameOrVal(queryPart.left)]: {$regex: regex, $options: 'i'}}]};
            // }else{
            return {
                [getColumnNameOrVal(queryPart.left)]: {
                    $regex: regex,
                    $options: 'i',
                },
            };
            // }
        }
        if (queryPart.operator === 'IS NOT') {
            return makeOperator('$ne');
        }
        throw new Error(`Unsupported operator:${queryPart.operator}`);
    }

    if (queryPart.type === 'function' || queryPart.type === 'select')
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            queryPart,
            0,
            true
        );
    if (queryPart.type === 'expr_list') {
        return queryPart.value.map((v) => makeQueryPart(v));
    }

    return getColumnNameOrVal(queryPart);
}
