const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const $check = require('check-types');
const {sqlStringToRegex} = require('./sqlStringToRegex');
const makeCastPartModule = require('./makeCastPart');
const {makeCaseCondition} = require('./makeCaseCondition');
const {isValueType} = require('./isValueType');
const {findSchema} = require('./schema-utils');
const {dayjs} = require('../day');

exports.makeQueryPart = makeQueryPart;

/**
 * Parses a AST QueryPart into a Mongo Query/Match
 * @param {import("../types").Expression} queryPart - The AST query part
 * @param {import("../types").NoqlContext} context - The Noql context to use when generating the output
 * @param {boolean} [ignorePrefix] - Ignore the table prefix
 * @param {Array}  [allowedTypes] - Expression types to allow
 * @param {boolean} [includeThis] - include $$this in expresions
 * @param {string} [tableAlias] - a table alias to check if it hasn't been specified
 * @returns {any} - the mongo query/match
 */
function makeQueryPart(
    queryPart,
    context,
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
            context,
            ignorePrefix,
            allowedTypes,
            includeThis,
            tableAlias
        );
        const right = makeQueryPart(
            queryPart.right,
            context,
            ignorePrefix,
            allowedTypes,
            includeThis,
            tableAlias
        );
        const leftSchema = $check.string(left)
            ? findSchema(left, context)
            : null;
        const rightSchema = $check.string(right)
            ? findSchema(right, context)
            : null;
        if (
            $check.string(left) &&
            isValueType(queryPart.right.type, right, context)
        ) {
            if (leftSchema && leftSchema.format === 'date-time') {
                return {
                    $expr: {
                        $and: [
                            {
                                [op]: [
                                    {$toDate: `$${left}`},
                                    {
                                        $toDate: {
                                            $literal: dayjs
                                                .utc(right)
                                                .toISOString(),
                                        },
                                    },
                                ],
                            },
                            {$ne: [{$type: `$${left}`}, 'null']},
                            {$ne: [{$type: `$${left}`}, 'missing']},
                        ],
                    },
                };
            }
            return {[left]: {[op]: right}};
        } else {
            const leftIsValueType = isValueType(
                queryPart.left.type,
                left,
                context
            );
            let leftToUse =
                $check.string(left) && !leftIsValueType ? `$${left}` : left;
            const rightIsValueType = isValueType(
                queryPart.right.type,
                right,
                context
            );
            let rightToUse = right;
            if (
                (leftSchema && leftSchema.format === 'date-time') ||
                (rightSchema && rightSchema.format === 'date-time')
            ) {
                leftToUse = $check.string(leftToUse)
                    ? {$toDate: leftToUse}
                    : leftToUse;
                rightToUse = $check.string(right)
                    ? {$toDate: `${rightIsValueType ? '' : '$'}${right}`}
                    : right;
                const stage = {
                    $expr: {
                        $and: [
                            {
                                [op]: [leftToUse, rightToUse],
                            },
                        ],
                    },
                };
                if ($check.string(left) && !leftIsValueType) {
                    stage.$expr.$and.push({
                        $ne: [
                            {
                                $type: `$${left}`,
                            },
                            'null',
                        ],
                    });
                    stage.$expr.$and.push({
                        $ne: [
                            {
                                $type: `$${left}`,
                            },
                            'missing',
                        ],
                    });
                }
                if ($check.string(right) && !rightIsValueType) {
                    stage.$expr.$and.push({
                        $ne: [
                            {
                                $type: `$${right}`,
                            },
                            'null',
                        ],
                    });
                    stage.$expr.$and.push({
                        $ne: [
                            {
                                $type: `$${right}`,
                            },
                            'missing',
                        ],
                    });
                }
                return stage;
            }
            return {
                $expr: {[op]: [leftToUse, rightToUse]},
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
                        context,
                        ignorePrefix,
                        allowedTypes,
                        includeThis,
                        tableAlias
                    ),
                    makeQueryPart(
                        queryPart.right,
                        context,
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
                        context,
                        ignorePrefix,
                        allowedTypes,
                        includeThis,
                        tableAlias
                    ),
                    makeQueryPart(
                        queryPart.right,
                        context,
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
            let likeVal = queryPart.right.value || queryPart.right.column;
            if (!likeVal && queryPart.right.type === 'function') {
                const right = makeQueryPart(
                    queryPart.right,
                    context,
                    ignorePrefix,
                    allowedTypes,
                    includeThis,
                    tableAlias
                );
                if (typeof right !== 'string') {
                    throw new Error(
                        `Regex result must be a string but was ${JSON.stringify(
                            right
                        )}`
                    );
                }
                likeVal = right;
            }
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
            let likeVal = queryPart.right.value;
            if (!likeVal && queryPart.right.type === 'function') {
                const right = makeQueryPart(
                    queryPart.right,
                    context,
                    ignorePrefix,
                    allowedTypes,
                    includeThis,
                    tableAlias
                );
                if (typeof right !== 'string') {
                    throw new Error(
                        `Regex result must be a string but was ${JSON.stringify(
                            right
                        )}`
                    );
                }
                likeVal = right;
            }
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
                context,
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
            context,
            0,
            true
        );

    if (queryPart.type === 'expr_list') {
        return queryPart.value.map((v) => makeQueryPart(v, context));
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
            context,
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
        return makeCastPartModule.makeCastPart(queryPart, context);
    }

    // case
    if (queryPart.type === 'case') {
        return makeCaseCondition(queryPart, context);
    }

    const columnNameOrValue = getColumnNameOrVal(queryPart);
    if (queryPart.type !== 'null' && !$check.assigned(columnNameOrValue)) {
        throw new Error('Unable to make query part for:' + queryPart.type);
    }
    if (queryPart.type === 'timestamp') {
        return new Date(columnNameOrValue);
    }
    return columnNameOrValue;
}
