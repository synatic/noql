const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const $check = require('check-types');
const {sqlStringToRegex} = require('./sqlStringToRegex');
const makeCastPartModule = require('./makeCastPart');
const {makeCaseCondition} = require('./makeCaseCondition');
const {isValueType} = require('./isValueType');

exports.makeQueryPart = makeQueryPart;

/**
 * Parses a AST QueryPart into a Mongo Query/Match
 *
 * @param {import("../types").Expression} queryPart - The AST query part
 * @param {boolean} [ignorePrefix] - Ignore the table prefix
 * @param {Array}  [allowedTypes] - Expression types to allow
 * @param {boolean} [includeThis] - include $$this in expresions
 * @param {string} [tableAlias] - a table alias to check if it hasn't been specified
 * @returns {any} - the mongo query/match
 */
function makeQueryPart(
    queryPart,
    ignorePrefix,
    allowedTypes = [],
    includeThis = false,
    tableAlias = ''
) {
    if (allowedTypes.length > 0 && !allowedTypes.includes(queryPart.type)) {
        throw new Error(`Type not allowed for query:${queryPart.type}`);
    }

    const getColumnNameOrVal = (queryPart) => {
        let queryPartToUse = queryPart;
        if (queryPart.left) {
            queryPartToUse = queryPart.left;
        }

        const table = queryPartToUse.table || tableAlias;
        if (queryPartToUse.column) {
            return (
                (includeThis ? '$$this.' : '') +
                (table && !ignorePrefix
                    ? `${table}.${queryPartToUse.column}`
                    : `${queryPartToUse.column}`)
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
            includeThis,
            tableAlias
        );
        const right = makeQueryPart(
            queryPart.right,
            ignorePrefix,
            allowedTypes,
            includeThis,
            tableAlias
        );

        if ($check.string(left) && isValueType(queryPart.right.type, right)) {
            return {[left]: {[op]: right}};
        } else {
            return {
                $expr: {[op]: [$check.string(left) ? `$${left}` : left, right]},
            };
        }
    };
    const upperOperator = queryPart.operator || ''.toUpperCase();
    if (queryPart.type === 'binary_expr') {
        if (upperOperator === '=') return makeOperator('$eq');
        if (upperOperator === '>') return makeOperator('$gt');
        if (upperOperator === '<') return makeOperator('$lt');
        if (upperOperator === '>=') return makeOperator('$gte');
        if (upperOperator === '<=') return makeOperator('$lte');
        if (upperOperator === '!=') return makeOperator('$ne');
        if (upperOperator === 'AND') {
            return {
                $and: [
                    makeQueryPart(
                        queryPart.left,
                        ignorePrefix,
                        allowedTypes,
                        includeThis,
                        tableAlias
                    ),
                    makeQueryPart(
                        queryPart.right,
                        ignorePrefix,
                        allowedTypes,
                        includeThis,
                        tableAlias
                    ),
                ],
            };
        }
        if (upperOperator === 'OR') {
            return {
                $or: [
                    makeQueryPart(
                        queryPart.left,
                        ignorePrefix,
                        allowedTypes,
                        includeThis,
                        tableAlias
                    ),
                    makeQueryPart(
                        queryPart.right,
                        ignorePrefix,
                        allowedTypes,
                        includeThis,
                        tableAlias
                    ),
                ],
            };
        }
        if (upperOperator === 'IN') {
            return makeOperator('$in');
            // return {$in: [makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
        }
        if (upperOperator === 'NOT IN') {
            return makeOperator('$nin');
            // return {$in: [makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
        }
        if (upperOperator === 'LIKE' || upperOperator === 'ILIKE') {
            const likeVal = queryPart.right.value || queryPart.right.column;
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
        if (upperOperator === 'NOT LIKE' || upperOperator === 'NOT ILIKE') {
            const likeVal = queryPart.right.value;
            const regexString = sqlStringToRegex(likeVal);
            // eslint-disable-next-line security/detect-non-literal-regexp
            // const regex = new RegExp(regexString, 'i');
            return {
                [getColumnNameOrVal(queryPart.left)]: {
                    $not: {$regex: regexString, $options: 'i'},
                },
            };
        }
        if (upperOperator === 'IS NOT') {
            return makeOperator('$ne');
        }

        if (upperOperator === 'IS') {
            return makeOperator('$eq');
        }

        throw new Error(`Unsupported operator: ${upperOperator}`);
    }

    if (
        queryPart.type === 'function' &&
        queryPart.name &&
        queryPart.name.toUpperCase() === 'NOT'
    ) {
        return {
            $nor: makeQueryPart(
                queryPart.args,
                ignorePrefix,
                allowedTypes,
                includeThis,
                tableAlias
            ),
        };
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

    // NOT col IS NULL
    if (
        queryPart.type === 'unary_expr' &&
        upperOperator === 'NOT' &&
        queryPart.expr &&
        queryPart.expr.type === 'binary_expr' &&
        queryPart.expr.operator &&
        queryPart.expr.operator.toUpperCase() === 'IS' &&
        queryPart.expr.left &&
        queryPart.expr.left.type === 'column_ref' &&
        queryPart.expr.right &&
        queryPart.expr.right.type === 'null'
    ) {
        return {
            [`${
                queryPart.expr.left.table ? queryPart.expr.left.table + '.' : ''
            }${queryPart.expr.left.column}`]: {$ne: null},
        };
    }

    // NOT Expression
    if (
        queryPart.type === 'unary_expr' &&
        upperOperator === 'NOT' &&
        queryPart.expr
    ) {
        const exprQuery = makeQueryPart(
            queryPart.expr,
            ignorePrefix,
            allowedTypes,
            includeThis,
            tableAlias
        );

        return {
            $nor: $check.array(exprQuery) ? exprQuery : [exprQuery],
        };
    }

    // todo add not

    if (queryPart.type === 'aggr_func') {
        throw new Error(
            `Aggregate function not allowed in where:${queryPart.name}`
        );
    }

    // cast
    if (queryPart.type === 'cast') {
        return makeCastPartModule.makeCastPart(queryPart);
    }

    // case
    if (queryPart.type === 'case') {
        return makeCaseCondition(queryPart);
    }

    const columnNameOrValue = getColumnNameOrVal(queryPart);
    if (queryPart.type !== 'null' && !$check.assigned(columnNameOrValue)) {
        throw new Error('Unable to make query part for:' + queryPart.type);
    }
    return columnNameOrValue;
}
