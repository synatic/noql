const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const {sqlStringToRegex} = require('./sqlStringToRegex');

exports.makeFilterCondition = makeFilterCondition;

/**
 * Creates a filter expression from a query part
 *
 * @param {object} queryPart - The query part to create filter
 * @param {boolean} [includeThis] - include the $$this prefix on sub selects
 * @param {boolean} [prefixRight] - include $$ for inner variables
 * @param {string} [side] - which side of the expression we're working with: left or right
 * @returns {any} - the filter expression
 */
function makeFilterCondition(queryPart, includeThis = false, prefixRight = false, side = 'left') {
    if (queryPart.type === 'binary_expr') {
        if (queryPart.operator === '=')
            return {
                $eq: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === '>')
            return {
                $gt: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === '<')
            return {
                $lt: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === '>=')
            return {
                $gte: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === '<=')
            return {
                $lte: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === '!=')
            return {
                $ne: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === 'AND')
            return {
                $and: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === 'OR')
            return {
                $or: [
                    makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                ],
            };
        if (queryPart.operator === 'LIKE') {
            const likeVal = queryPart.right.value;
            const regex = sqlStringToRegex(likeVal);

            return {
                $regexMatch: {
                    input: makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                    regex: regex,
                    options: 'i',
                },
            };
        }
        throw new Error(`Unsupported operator:${queryPart.operator}`);
    }

    if (queryPart.type === 'unary_expr') {
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(queryPart);
    }

    if (queryPart.type === 'function') {
        return makeProjectionExpressionPartModule.makeProjectionExpressionPart(queryPart);
    }

    if (queryPart.type === 'column_ref')
        return `${includeThis ? '$$this.' : '$'}${
            prefixRight && side === 'right' ? '$' + (queryPart.table ? queryPart.table + '.' : '') : ''
        }${queryPart.column}`;

    if (['number', 'string', 'single_quote_string'].includes(queryPart.type)) {
        return queryPart.value;
    }

    throw new Error(`invalid expression type for array sub select:${queryPart.type}`);
}
