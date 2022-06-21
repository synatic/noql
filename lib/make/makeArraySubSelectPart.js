const getParsedValueFromBinaryExpressionModule = require('./getParsedValueFromBinaryExpression');
const makeCaseConditionModule = require('./makeCaseCondition');
const makeFilterConditionModule = require('./makeFilterCondition');
const makeObjectFromSelectModule = require('./makeObjectFromSelect');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const {canQuery} = require('../canQuery');
const {isSelectAll} = require('../isSelectAll');

exports.makeArraySubSelectPart = makeArraySubSelectPart;

/**
 * Makes an array expression from a sub select
 *
 * @param {import('../types').AST} ast - the ast to create a sub select from
 * @param {number} [depth] - the depth of the query, automatically set
 * @returns {*}
 */
function makeArraySubSelectPart(ast, depth = 0) {
    if (!ast || !ast.from || !ast.from.length || ast.from.length === 0) {
        throw new Error('Invalid array sub select');
    }
    if (!canQuery({ast: ast}, {isArray: true})) {
        throw new Error(
            'Array sub select does not support aggregation methods'
        );
    }

    let mapIn = '$$this';
    if (ast.columns && !isSelectAll(ast.columns) && ast.columns.length > 0) {
        // @ts-ignore
        mapIn = {};
        /** @type {import('../types').Column[]} */
        // @ts-ignore
        const columns = ast.columns;
        columns.forEach((column) => {
            if (column.expr.type === 'column_ref') {
                mapIn[column.as || column.expr.column] = `$$this.${
                    column.expr.table ? column.expr.table + '.' : ''
                }${column.expr.column}`;
            } else if (
                column.expr.type === 'function' ||
                (column.expr.type === 'aggr_func' && column.as)
            ) {
                mapIn[column.as] =
                    makeProjectionExpressionPartModule.makeProjectionExpressionPart(
                        column.expr,
                        depth + 1
                    );
            } else if (column.expr.type === 'binary_expr' && column.as) {
                mapIn[column.as] =
                    getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(
                        column.expr
                    );
            } else if (column.expr.type === 'case' && column.as) {
                mapIn[column.as] = makeCaseConditionModule.makeCaseCondition(
                    column.expr
                );
            } else if (
                column.expr.type === 'select' &&
                column.as &&
                column.expr.from
            ) {
                mapIn[column.as] = makeArraySubSelectPart(
                    column.expr,
                    depth + 1
                );
            } else if (
                column.expr.type === 'select' &&
                column.as &&
                !column.expr.from
            ) {
                mapIn[column.as] =
                    makeObjectFromSelectModule.makeObjectFromSelect(
                        column.expr
                    );
            } else if (column.expr.type && column.as) {
                mapIn[column.as] = {$literal: column.expr.value};
            } else if (!column.as) {
                throw new Error(
                    `Require as for array subselect calculation:${column.expr.name}`
                );
            } else {
                throw new Error(`Not Supported:${column.expr.type}`);
            }
        });
    }

    let mapInput = null;

    if (mapIn['$$ROOT']) {
        mapIn = mapIn['$$ROOT'];
    }

    if (ast.where) {
        mapInput = {
            $filter: {
                input: `$${depth > 0 ? '$this.' : ''}${ast.from[0].table}`,
                cond: {
                    $and: [
                        makeFilterConditionModule.makeFilterCondition(
                            ast.where,
                            true
                        ),
                    ],
                },
            },
        };
    } else if (ast.from[0].table) {
        mapInput = `$${depth > 0 ? '$this.' : ''}${ast.from[0].table}`;
    } else {
        throw new Error('No table specified for sub array select');
    }

    let parsedQuery = {
        $map: {
            input: mapInput,
            in: mapIn,
        },
    };

    if (ast.limit) {
        if (
            ast.limit.seperator &&
            ast.limit.seperator === 'offset' &&
            ast.limit.value[1] &&
            ast.limit.value[1].value
        ) {
            parsedQuery = {
                $slice: [
                    parsedQuery,
                    ast.limit.value[1].value,
                    ast.limit.value[0].value,
                ],
            };
        } else if (
            ast.limit.value &&
            ast.limit.value[0] &&
            ast.limit.value[0].value
        ) {
            parsedQuery = {$slice: [parsedQuery, 0, ast.limit.value[0].value]};
        }
    }
    if (ast.orderby) {
        const sortBy = ast.orderby.reduce((obj, value) => {
            obj[value.expr.column] = value.type === 'DESC' ? -1 : 1;
            return obj;
        }, {});
        parsedQuery = {
            $sortArray: {
                input: mapInput,
                sortBy,
            },
        };
    }

    return parsedQuery;
}
