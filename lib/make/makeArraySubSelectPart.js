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
        throw new Error('Array sub select does not support aggregation methods');
    }

    let mapIn = '$$this';
    if (ast.columns && !isSelectAll(ast.columns) && ast.columns.length > 0) {
        mapIn = {};
        /** @type {import('../types').Column[]} */
        const columns = ast.columns;
        columns.forEach((v) => {
            if (v.expr.type === 'column_ref') {
                mapIn[v.as || v.expr.column] = `$$this.${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
            } else if (v.expr.type === 'function' && v.as) {
                mapIn[v.as] = makeProjectionExpressionPartModule.makeProjectionExpressionPart(v.expr, depth + 1);
            } else if (v.expr.type === 'aggr_func' && v.as) {
                mapIn[v.as] = makeProjectionExpressionPartModule.makeProjectionExpressionPart(v.expr, depth + 1);
            } else if (v.expr.type === 'binary_expr' && v.as) {
                mapIn[v.as] = getParsedValueFromBinaryExpressionModule.getParsedValueFromBinaryExpression(v.expr);
            } else if (v.expr.type === 'case' && v.as) {
                mapIn[v.as] = makeCaseConditionModule.makeCaseCondition(v.expr);
            } else if (v.expr.type === 'select' && v.as && v.expr.from) {
                mapIn[v.as] = makeArraySubSelectPart(v.expr, depth + 1);
            } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                mapIn[v.as] = makeObjectFromSelectModule.makeObjectFromSelect(v.expr);
            } else if (v.expr.type && v.as) {
                mapIn[v.as] = {$literal: v.expr.value};
            } else if (!v.as) {
                throw new Error(`Require as for array subselect calculation:${v.expr.name}`);
            } else {
                throw new Error(`Not Supported:${v.expr.type}`);
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
                cond: {$and: [makeFilterConditionModule.makeFilterCondition(ast.where, true)]},
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
        if (ast.limit.seperator && ast.limit.seperator === 'offset' && ast.limit.value[1] && ast.limit.value[1].value) {
            parsedQuery = {$slice: [parsedQuery, ast.limit.value[1].value, ast.limit.value[0].value]};
        } else if (ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value) {
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
