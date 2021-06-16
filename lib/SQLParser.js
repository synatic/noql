const {Parser} = require('node-sql-parser');
const $check = require('check-types');
const $merge = require('deepmerge');
const $json = require('@synatic/json-magic');
const _allowableFunctions = require('./MongoFunctions');

class SQLParser {
    /** Checks whether a columns is a select *
     *
     * @param {any} columns - the columns to check
     * @return {boolean}
     * @private
     */
    static _isSelectAll(columns) {
        if (!columns) return false;
        if (columns === '*') return true;
        return false;
    }

    /** Create a regex from a sql like command
     *
     * @param {string }likeVal - the like value to turn into a regex
     * @return {string|null} - the regex equivalent
     * @private
     */
    static _makeRegex(likeVal) {
        if (!likeVal) return null;
        let regex = likeVal;
        if (likeVal.startsWith('%') && likeVal.endsWith('%')) {
            regex = new RegExp(`${likeVal.substring(1, likeVal.length - 1)}`);
        } else if (likeVal.startsWith('%')) {
            regex = new RegExp(`${likeVal.substring(1)}$`);
        } else if (likeVal.endsWith('%')) {
            regex = new RegExp(`^${likeVal.substring(0, likeVal.length - 1)}`);
        } else {
            regex = new RegExp(`^${likeVal}$`);
        }
        return regex.source;
    }

    /** Creates a filter expression from a query part
     *
     * @param {object} queryPart - The query part to create filter
     * @param {boolean} [includeThis] - include the $$this prefix on sub selects
     * @param {boolean} [prefixRight] - include $$ for inner variables
     * @param {string} [side] - which side of the expression we're working with: left or right
     * @return {any} - the filter expression
     * @private
     */
    static _makeFilterCondition(queryPart, includeThis = false, prefixRight = false, side = 'left') {
        if (queryPart.type === 'binary_expr') {
            if (queryPart.operator === '=')
                return {
                    $eq: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === '>')
                return {
                    $gt: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === '<')
                return {
                    $lt: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === '>=')
                return {
                    $gte: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === '<=')
                return {
                    $lte: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === '!=')
                return {
                    $ne: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === 'AND')
                return {
                    $and: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === 'OR')
                return {
                    $or: [
                        SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        SQLParser._makeFilterCondition(queryPart.right, includeThis, prefixRight, 'right'),
                    ],
                };
            if (queryPart.operator === 'LIKE') {
                const likeVal = queryPart.right.value;
                const regex = SQLParser._makeRegex(likeVal);

                return {
                    $regexMatch: {
                        input: SQLParser._makeFilterCondition(queryPart.left, includeThis, prefixRight, 'left'),
                        regex: regex,
                        options: 'i',
                    },
                };
            }
            throw new Error(`Unsupported operator:${queryPart.operator}`);
        }

        if (queryPart.type === 'unary_expr') {
            return SQLParser._makeProjectionExpressionPart(queryPart);
        }

        if (queryPart.type === 'function') return SQLParser._makeProjectionExpressionPart(queryPart);

        if (queryPart.type === 'column_ref')
            return `${includeThis ? '$$this.' : '$'}${
                prefixRight && side === 'right' ? '$' + (queryPart.table ? queryPart.table + '.' : '') : ''
            }${queryPart.column}`;

        if (['number', 'string', 'single_quote_string'].includes(queryPart.type)) return queryPart.value;

        throw new Error(`invalid expression type for array sub select:${queryPart.type}`);
    }

    /** Makes a $cond from a case statement
     *
     * @param {object} expr - the expression object to turn into a case
     * @return {any}
     * @private
     */
    static _makeCaseCondition(expr) {
        if (expr.type !== 'case') throw new Error(`Expresion is not case`);

        const elseExpr = expr.args.find((a) => a.type === 'else');
        const whens = expr.args.filter((a) => a.type === 'when');

        return {
            $switch: {
                branches: whens.map((w) => {
                    return {
                        case: SQLParser._makeFilterCondition(w.cond),
                        then: SQLParser._makeFilterCondition(w.result),
                    };
                }),
                default: SQLParser._makeProjectionExpressionPart(elseExpr.result),
            },
        };
    }

    /** Makes an mongo expression tree from the cast statement
     *
     * @param {object} expr - the AST expression that is a cast
     * @return {*}
     * @private
     */
    static _makeCastPart(expr) {
        if (expr.type !== 'cast') throw new Error(`Invalid type for cast:${expr.type}`);
        const convFN = _allowableFunctions.functionMappings.find((f) => f.name === 'convert');
        if (!convFN) throw new Error('No conversion function found');
        const to = expr.target.dataType.toLowerCase();

        if (expr.expr.column) return convFN.parse([`$${expr.expr.column}`, to]);
        if (expr.expr.value) return convFN.parse([expr.expr.value, to]);
        return convFN.parse([SQLParser._makeProjectionExpressionPart(expr.expr), to]);
    }

    /** Parses a AST QueryPart into a Mongo Query/Match
     *
     * @param {object} queryPart - The AST query part
     * @param {boolean} [ignorePrefix] - Ignore the table prefix
     * @param {array}  [allowedTypes] - Expression types to allow
     * @param {boolean} [includeThis] - include $$this in expresions
     *
     * @return {any} - the mongo query/match
     * @private
     */
    static _makeQueryPart(queryPart, ignorePrefix, allowedTypes = [], includeThis = false) {
        if (allowedTypes.length > 0 && !allowedTypes.includes(queryPart.type))
            throw new Error(`Type not allowed for query:${queryPart.type}`);

        const getColumnNameOrVal = (queryPart) => {
            let queryPartToUse = queryPart;
            if (queryPart.left) {
                queryPartToUse = queryPart.left;
            }

            if (queryPartToUse.column) {
                return (
                    (includeThis ? '$$this.' : '') +
                    (queryPartToUse.table && !ignorePrefix ? `${queryPartToUse.table}.${queryPartToUse.column}` : queryPartToUse.column)
                );
            } else {
                return queryPartToUse.value;
            }
        };

        const makeOperator = (op) => {
            const left = SQLParser._makeQueryPart(queryPart.left, ignorePrefix, allowedTypes, includeThis);
            const right = SQLParser._makeQueryPart(queryPart.right, ignorePrefix, allowedTypes, includeThis);
            if ($check.string(left) && !left.startsWith('$')) return {[left]: {[op]: right}};
            else return {$expr: {[op]: [left, right]}};
        };

        if (queryPart.type === 'binary_expr') {
            if (queryPart.operator === '=') return makeOperator('$eq');
            if (queryPart.operator === '>') return makeOperator('$gt');
            if (queryPart.operator === '<') return makeOperator('$lt');
            if (queryPart.operator === '>=') return makeOperator('$gte');
            if (queryPart.operator === '<=') return makeOperator('$lte');
            if (queryPart.operator === '!=') return makeOperator('$ne');
            if (queryPart.operator === 'AND')
                return $merge(
                    SQLParser._makeQueryPart(queryPart.left, ignorePrefix, allowedTypes, includeThis),
                    SQLParser._makeQueryPart(queryPart.right, ignorePrefix, allowedTypes, includeThis)
                );
            if (queryPart.operator === 'OR')
                return {
                    $or: [
                        SQLParser._makeQueryPart(queryPart.left, ignorePrefix, allowedTypes, includeThis),
                        SQLParser._makeQueryPart(queryPart.right, ignorePrefix, allowedTypes, includeThis),
                    ],
                };
            if (queryPart.operator === 'IN') {
                return makeOperator('$in');
                // return {$in: [SQLParser._makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), SQLParser._makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
            }
            if (queryPart.operator === 'LIKE') {
                const likeVal = queryPart.right.value;
                const regex = SQLParser._makeRegex(likeVal);
                return {[getColumnNameOrVal(queryPart.left)]: {$regex: regex, $options: 'i'}};
            }
            if (queryPart.operator === 'IS NOT') {
                return makeOperator('$ne');
            }
            throw new Error(`Unsupported operator:${queryPart.operator}`);
        }

        if (queryPart.type === 'function' || queryPart.type === 'select') return SQLParser._makeProjectionExpressionPart(queryPart);
        if (queryPart.type === 'expr_list') return queryPart.value.map((v) => SQLParser._makeQueryPart(v));

        return getColumnNameOrVal(queryPart);
    }

    /** Makes a projection expression sub part.
     *
     * @param {object} expr - the expression to make a projection from
     * @param {int} [depth] - the current recursive depth
     * @return {undefined|*}
     * @private
     */
    static _makeProjectionExpressionPart(expr, depth = 0) {
        const makeArg = (expr) => {
            // todo check all these types
            if (expr.type === 'function') {
                return SQLParser._makeProjectionExpressionPart(expr);
            } else if (expr.type === 'column_ref') {
                return `$${expr.table ? expr.table + '.' : ''}${expr.column}`;
            } else if (expr.type === 'binary_expr') {
                return SQLParser._getParsedValueFromBinaryExpression(expr);
            } else if (expr.type === 'select' && expr.from) {
                return SQLParser._makeArraySubSelectPart(expr, depth);
            } else if (expr.type === 'select' && !expr.from) {
                return SQLParser._makeObjectFromSelect(expr);
            } else if (expr.type === 'unary_expr') {
                if (expr.operator === '-') {
                    return {$multiply: [-1, SQLParser._makeProjectionExpressionPart(expr.expr)]};
                } else {
                    throw new Error(`Unable to parse unary expression:${expr.operator}`);
                }
            } else if (expr.type === 'case') {
                return SQLParser._makeCaseCondition(expr);
            } else if (expr.value !== undefined) {
                return {$literal: expr.value};
            } else {
                throw new Error(`Unable to parse expression type:${expr.type}`);
            }
        };
        if (!expr.name && !expr.operator) {
            return makeArg(expr);
        }

        // if(expr.type==="number"||expr.type==="string")return expr.value;
        // if(expr.type==="column_ref")return `$${expr.column}`;
        // if(expr.type==="type")return `$${expr.column}`;
        const fn = _allowableFunctions.functionMappings.find(
            (f) => f.name && f.name.toLowerCase() === (expr.name || expr.operator).toLowerCase() && (!f.type || f.type === expr.type)
        );
        if (!fn) {
            throw new Error(`Function:${expr.name} not available`);
        }

        if (expr.args && expr.args.value) {
            const args = $check.array(expr.args.value) ? expr.args.value : [expr.args.value];
            return fn.parse(
                args.map((a) => makeArg(a)),
                depth
            );
        } else if (expr.left && expr.right) {
            return SQLParser._getParsedValueFromBinaryExpression(expr);
        } else if (expr.args && expr.args.expr) {
            return fn.parse(makeArg(expr.args.expr), depth);
        } else {
            return makeArg(expr);
            // throw new Error('Unable to parse expression');
        }
    }

    /** Get the value from a binary expression
     *
     * @param {object} expressionPart - the expression to turn into a value
     * @return {string|undefined|*}
     * @private
     */
    static _getParsedValueFromBinaryExpression(expressionPart) {
        if (expressionPart.type === 'binary_expr') return SQLParser._makeBinaryExpresionPart(expressionPart);
        if (expressionPart.type === 'column_ref') return `$${expressionPart.column}`;
        if (['single_quote_string', 'string'].includes(expressionPart.type)) return expressionPart.value;
        if (['number'].includes(expressionPart.type)) return expressionPart.value;
        if (expressionPart.type === 'function') return SQLParser._makeProjectionExpressionPart(expressionPart);

        throw new Error(`Unable to make binary expression part:${expressionPart.type}`);
    }

    /** Makes an array expression from a sub select
     *
     * @param {object} ast - the ast to create a sub select from
     * @param {int} [depth] - the depth of the query, automatically set
     * @return {*}
     * @private
     */
    static _makeArraySubSelectPart(ast, depth = 0) {
        if (!ast || !ast.from || !ast.from.length || ast.from.length === 0) throw new Error('Invalid array sub select');
        if (!SQLParser.canQuery({ast: ast})) throw new Error('Array sub select does not support aggregation methods');

        let mapIn = '$$this';
        if (ast.columns && !SQLParser._isSelectAll(ast.columns) && ast.columns.length > 0) {
            mapIn = {};

            ast.columns.forEach((v) => {
                if (v.expr.type === 'column_ref') {
                    mapIn[v.as || v.expr.column] = `$$this.${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
                } else if (v.expr.type === 'function' && v.as) {
                    mapIn[v.as] = SQLParser._makeProjectionExpressionPart(v.expr, depth + 1);
                } else if (v.expr.type === 'aggr_func' && v.as) {
                    mapIn[v.as] = SQLParser._makeProjectionExpressionPart(v.expr, depth + 1);
                } else if (v.expr.type === 'binary_expr' && v.as) {
                    mapIn[v.as] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
                } else if (v.expr.type === 'case' && v.as) {
                    mapIn[v.as] = SQLParser._makeCaseCondition(v.expr);
                } else if (v.expr.type === 'select' && v.as && v.expr.from) {
                    mapIn[v.as] = SQLParser._makeArraySubSelectPart(v.expr, depth + 1);
                } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                    mapIn[v.as] = SQLParser._makeObjectFromSelect(v.expr);
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
                    cond: {$and: [SQLParser._makeFilterCondition(ast.where, true)]},
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

        return parsedQuery;
    }

    /** Translates a binary expression into a mongo usable part
     *
     * @param {expr} expr - the ast expression
     * @return {*}
     * @private
     */
    static _makeBinaryExpresionPart(expr) {
        let operator;
        if (expr.expr) {
            operator = expr.expr.operator;
        } else {
            operator = expr.operator;
        }

        const exprFunction = _allowableFunctions.functionMappings.find((f) => f.name === operator.toLowerCase());
        let exprResult;
        if(!exprFunction)throw new Error(`Expression not found:${operator}`);

        if (expr.expr && expr.expr.left && expr.expr.right) {
            const leftPartValue = SQLParser._getParsedValueFromBinaryExpression(expr.expr.left);
            const rightPartValue = SQLParser._getParsedValueFromBinaryExpression(expr.expr.right);

            exprResult = exprFunction.parse(leftPartValue, rightPartValue);
        } else if (expr.left && expr.right) {
            const leftPartValue = SQLParser._getParsedValueFromBinaryExpression(expr.left);
            const rightPartValue = SQLParser._getParsedValueFromBinaryExpression(expr.right);

            exprResult = exprFunction.parse(leftPartValue, rightPartValue);
        }

        return exprResult;
    }

    /** Creates the pipeline components for a join
     *
     * @param {object} ast - the ast that contains the join
     * @return {*[]}
     * @private
     */
    static _makeJoinForPipeline(ast) {
        const pipeline = [];

        const makeJoinPart = (join) => {
            let table = join.table || '';
            let joinHint = null;
            if (table.toLowerCase().endsWith('|first')) {
                joinHint = 'first';
                table = table.substring(0, table.length - 6);
            } else if (table.toLowerCase().endsWith('|last')) {
                joinHint = 'last';
                table = table.substring(0, table.length - 5);
            } else if (table.toLowerCase().endsWith('|unwind')) {
                joinHint = 'unwind';
                table = table.substring(0, table.length - 7);
            }
            const as = join.as || table;

            if (join.table && join.on && join.on.type === 'binary_expr' && join.on.operator === '=') {
                const local =
                    join.on.left.table === as || join.on.left.as === as
                        ? `${join.on.right.table ? join.on.right.table + '.' : ''}${join.on.right.column}`
                        : `${join.on.left.table ? join.on.left.table + '.' : ''}${join.on.left.column}`;
                const foreign = join.on.left.table === as || join.on.left.as === as ? join.on.left.column : join.on.right.column;
                pipeline.push({
                    $lookup: {
                        from: table,
                        as: as,
                        localField: local,
                        foreignField: foreign,
                    },
                });
                if (joinHint) {
                    if (joinHint === 'first') {
                        pipeline.push({$set: {[as]: {$first: `$${as}`}}});
                    } else if (joinHint === 'last') {
                        pipeline.push({$set: {[as]: {$last: `$${as}`}}});
                    } else if (joinHint === 'unwind') {
                        pipeline.push({$unwind: `$${as}`});
                    }
                }
                if (join.join === 'INNER JOIN') {
                    if (joinHint) {
                        pipeline.push({$match: {[as]: {$ne: null}}});
                    } else {
                        pipeline.push({$match: {$expr: {$gt: [{$size: `$${as}`}, 0]}}});
                    }
                } else if (join.join === 'LEFT JOIN') {
                    // dont need anything
                } else {
                    throw new Error(`Join not supported:${join.join}`);
                }
            } else {
                const joinQuery = SQLParser._makeFilterCondition(join.on, false, true);
                const inputVars = {};
                const replacePaths = [];
                $json.walk(joinQuery, (val, path) => {
                    if ($check.string(val) && val.startsWith('$$')) {
                        const varName = val.substring(2).replace(/[.-]/g, '_');
                        inputVars[varName] = `$${val.substring(2)}`;
                        replacePaths.push({path: path, newVal: `$$${varName}`});
                    }
                });
                for (const path of replacePaths) {
                    $json.set(joinQuery, path.path, path.newVal);
                }

                let lookupPipeline = [];

                if (join.expr && join.expr.ast) {
                    lookupPipeline = SQLParser._makeAggregatePipeline(join.expr.ast);
                    if (join.expr.ast.from[0] && join.expr.ast.from[0].table) table = join.expr.ast.from[0].table;
                    else throw new Error('Missing table for join sub query');
                }
                lookupPipeline.push({$match: {$expr: joinQuery}});

                pipeline.push({
                    $lookup: {
                        from: table,
                        as: as,
                        let: inputVars,
                        pipeline: lookupPipeline,
                    },
                });
                if (joinHint) {
                    if (joinHint === 'first') {
                        pipeline.push({$set: {[as]: {$first: `$${as}`}}});
                    } else if (joinHint === 'last') {
                        pipeline.push({$set: {[as]: {$last: `$${as}`}}});
                    } else if (joinHint === 'unwind') {
                        pipeline.push({$unwind: `$${as}`});
                    }
                }
                if (join.join === 'INNER JOIN') {
                    if (joinHint) {
                        pipeline.push({$match: {[as]: {$ne: null}}});
                    } else {
                        pipeline.push({$match: {$expr: {$gt: [{$size: `$${as}`}, 0]}}});
                    }
                } else if (join.join === 'LEFT JOIN') {
                    // dont need anything
                } else {
                    throw new Error(`Join not supported:${join.join}`);
                }
            }
        };

        for (let i = 1; i < ast.from.length; i++) {
            makeJoinPart(ast.from[i]);
        }

        return pipeline;
    }

    /** Creates an object from a select without a from cause
     *
     * @param {object} ast - the ast for the select statement
     * @return {any}
     * @private
     */
    static _makeObjectFromSelect(ast) {
        const toParse = {};
        ast.columns.forEach((v) => {
            if (v.expr.type === 'column_ref') {
                toParse[`${v.as || v.expr.column}`] = `$${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
            } else if (v.expr.type === 'function' && v.as) {
                const parsedExpr = SQLParser._makeProjectionExpressionPart(v.expr);
                toParse[`${v.as}`] = parsedExpr;
            } else if (v.expr.type === 'binary_expr' && v.as) {
                toParse[`${v.as}`] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
            } else if (v.expr.type === 'case' && v.as) {
                toParse[`${v.as}`] = SQLParser._makeCaseCondition(v.expr);
            } else if (v.expr.type === 'cast' && v.as) {
                toParse[`${v.as}`] = SQLParser._makeCastPart(v.expr);
            } else if (v.expr.type === 'select' && v.as && v.expr.from) {
                toParse[`${v.as}`] = SQLParser._makeArraySubSelectPart(v.expr);
            } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                toParse[`${v.as}`] = SQLParser._makeObjectFromSelect(v.expr);
            } else if (v.expr.type && v.as) {
                toParse[`${v.as}`] = {$literal: v.expr.value};
            } else if (!v.as) {
                throw new Error(`Require as for calculation:${v.expr.name}`);
            } else {
                throw new Error(`Not Supported:${v.expr.type}`);
            }
        });

        return {
            $arrayToObject: {$concatArrays: [{$objectToArray: toParse}]},
        };
    }

    /** Creates an mongo aggregation pipeline given an ast
     *
     * @param {object} ast - the ast to make an aggregate pipeline from
     * @param {object} [options] - the options to generate the pipeline
     * @return {*[]}
     * @private
     */
    static _makeAggregatePipeline(ast, options= {}) {
        let pipeline = [];
        const asMapping = [];

        let replaceRoot = null;
        let unwind = null;

        if (ast.from[0].as && ast.from[0].table) {
            pipeline.push({$project: {[ast.from[0].as]: '$$ROOT'}});
        }

        pipeline = pipeline.concat(SQLParser._makeJoinForPipeline(ast));

        if (ast.groupby) {
            if (SQLParser._isSelectAll(ast.columns)) throw new Error(`Select * not allowed with group by`);
            const groupBy = {
                $group: {
                    _id: {},
                },
            };
            ast.columns.forEach((v) => {
                if (v.expr.type === 'column_ref') {
                    if (v.as && v.as === '$$ROOT') {
                        replaceRoot = {$replaceRoot: {newRoot: `$${v.expr.column}`}};
                    } else if (v.as) {
                        asMapping.push({column: v.expr.column, as: v.as});
                        groupBy.$group._id[v.as] = `$${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
                    } else {
                        groupBy.$group._id[v.expr.column] = `$${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
                    }
                } else if (v.expr.type === 'function' && v.as && v.expr.name && v.expr.name.toLowerCase() === 'unwind') {
                    throw new Error('Unwind not allowed with group by');
                } else if (v.expr.type === 'function' && v.as) {
                    const parsedExpr = SQLParser._makeProjectionExpressionPart(v.expr);
                    if (v.as && v.as.toUpperCase() === '$$ROOT') {
                        replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
                    } else {
                        groupBy.$group._id[v.as] = parsedExpr;
                    }
                } else if (v.expr.type === 'aggr_func' && v.as) {
                    const aggregateFunction = _allowableFunctions.functionMappings.find(
                        (f) => f.name && f.name.toLowerCase() === v.expr.name.toLowerCase() && (!f.type || f.type === 'aggr_func')
                    );
                    if (!aggregateFunction) throw new Error(`Function not found:${v.expr.name}`);
                    groupBy.$group[v.as] = SQLParser._makeProjectionExpressionPart(v.expr);
                } else if (v.expr.type === 'binary_expr' && v.as) {
                    groupBy.$group._id[v.as] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
                } else if (v.expr.type === 'case' && v.as) {
                    groupBy.$group._id[v.as] = SQLParser._makeCaseCondition(v.expr);
                } else if (v.expr.type === 'cast' && v.as) {
                    groupBy.$group._id[v.as] = SQLParser._makeCastPart(v.expr);
                } else if (v.expr.type === 'select' && v.as && v.expr.from) {
                    groupBy.$group._id[v.as] = SQLParser._makeArraySubSelectPart(v.expr);
                } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                    groupBy.$group._id[v.as] = SQLParser._makeObjectFromSelect(v.expr);
                } else if (v.expr.type && v.as) {
                    groupBy.$group._id[v.as] = {$literal: v.expr.value};
                } else if (!v.as) {
                    throw new Error(`Require as for calculation:${v.expr.name}`);
                } else {
                    throw new Error(`Not Supported:${v.expr.type}`);
                }
            });

            pipeline.push(groupBy);
            const groupByProject = {};
            Object.keys(groupBy.$group._id).forEach((k) => {
                groupByProject[k] = `$_id.${k}`;
            });
            Object.keys(groupBy.$group).forEach((k) => {
                if (k === '_id') {
                    groupByProject[k] = 0;
                } else {
                    groupByProject[k] = `$${k}`;
                }
            });
            if (!$check.emptyObject(groupByProject)) {
                pipeline.push({$project: groupByProject});
            }
            if (ast.having) {
                pipeline.push({$match: SQLParser._makeQueryPart(ast.having)});
            }
        } else if (ast.columns && !SQLParser._isSelectAll(ast.columns) && ast.columns.length > 0) {
            const parsedProject = {$project: {}};

            ast.columns.forEach((v) => {
                if (v.expr.type === 'column_ref') {
                    if (v.as && v.as.toUpperCase() === '$$ROOT') {
                        replaceRoot = {$replaceRoot: {newRoot: `$${v.expr.column}`}};
                    } else if (v.expr.column === '*' && v.expr.table) {
                        parsedProject.$project[v.as || v.expr.table] = `$${v.expr.table}`;
                    } else {
                        parsedProject.$project[v.as || v.expr.column] = `$${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
                    }
                } else if (v.expr.type === 'function' && v.as && v.expr.name && v.expr.name.toLowerCase() === 'unwind') {
                    unwind = {
                        $unwind: `$${v.as}`,
                    };
                    parsedProject.$project[v.as] = SQLParser._makeProjectionExpressionPart(v.expr.args.value[0]);
                } else if (v.expr.type === 'function' && v.as) {
                    const parsedExpr = SQLParser._makeProjectionExpressionPart(v.expr);
                    if (v.as && v.as.toUpperCase() === '$$ROOT') {
                        replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
                    } else {
                        parsedProject.$project[v.as] = parsedExpr;
                    }
                } else if (v.expr.type === 'aggr_func') {
                    throw new Error(`Aggregate function not allowed with no group by:${v.expr.name}`);
                } else if (v.expr.type === 'binary_expr' && v.as) {
                    parsedProject.$project[v.as] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
                } else if (v.expr.type === 'case' && v.as) {
                    parsedProject.$project[v.as] = SQLParser._makeCaseCondition(v.expr);
                } else if (v.expr.type === 'cast' && v.as) {
                    parsedProject.$project[v.as] = SQLParser._makeCastPart(v.expr);
                } else if (v.expr.type === 'select' && v.as && v.expr.from) {
                    parsedProject.$project[v.as] = SQLParser._makeArraySubSelectPart(v.expr);
                } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                    parsedProject.$project[v.as] = SQLParser._makeObjectFromSelect(v.expr);
                } else if (v.expr.type && v.as) {
                    parsedProject.$project[v.as] = {$literal: v.expr.value};
                } else if (!v.as) {
                    throw new Error(`Require as for calculation:${v.expr.name}`);
                } else {
                    throw new Error(`Not Supported:${v.expr.type}`);
                }
            });
            if (!$check.emptyObject(parsedProject.$project)) {
                pipeline.push(parsedProject);
            }
        }

        if (ast.where) {
            pipeline.unshift({$match: SQLParser._makeQueryPart(ast.where, true, [], false)});
        }

        // for if initial query is subquery
        if (!ast.from[0].table && ast.from[0].expr && ast.from[0].expr.ast) {
            if (!ast.from[0].as) throw new Error(`AS not specified for initial sub query`);
            pipeline = SQLParser._makeAggregatePipeline(ast.from[0].expr.ast, options)
                .concat([{$project: {[ast.from[0].as]: '$$ROOT'}}])
                .concat(pipeline);
        }

        if (replaceRoot) {
            pipeline.push(replaceRoot);
        }

        if (unwind) {
            pipeline.push(unwind);
        }

        if (ast.orderby && ast.orderby.length > 0) {
            pipeline.push({
                $sort: ast.orderby.reduce((a, v) => {
                    const asMapped = asMapping.find((c) => c.column === v.expr.column);
                    a[asMapped ? asMapped.as : v.expr.column || v.expr.value] = v.type === 'DESC' ? -1 : 1;

                    return a;
                }, {}),
            });
        }

        if (ast.limit) {
            if (ast.limit.seperator && ast.limit.seperator === 'offset' && ast.limit.value[1] && ast.limit.value[1].value) {
                pipeline.push({$limit: ast.limit.value[0].value});
                pipeline.push({$skip: ast.limit.value[1].value});
            } else if (ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value) {
                pipeline.push({$limit: ast.limit.value[0].value});
            }
        }

        return pipeline;
    }

    /** Validates an AST to ensure secondary parsing criteria are met
     *
     * @param {object} parsedAST - the parsedAST to validated
     * @private
     */
    static _validateAST(parsedAST) {
        if (!parsedAST) throw new Error('Invalid AST');
        if (!parsedAST.tableList || parsedAST.tableList.length === 0) throw new Error('SQL statement requires at least 1 collection');
        const ast = parsedAST.ast;
        if (!ast.from || !ast.from[0]) throw new Error('No FROM specified');
        // if(!ast.from[0].table)throw new Error('Initial from must be a collection reference');
        if ($check.array(ast.columns)) {
            const errors = [];
            for (const column of ast.columns) {
                if (column.expr && ['function', 'binary_expr', 'aggr_func'].includes(column.expr.type) && !column.as) {
                    errors.push(`Requires as for ${column.expr.type}${column.expr.name ? ':' + column.expr.name : ''}`);
                }
                if (column.expr && ['aggr_func'].includes(column.expr.type) && !ast.groupby) {
                    errors.push(`Requires group by for ${column.expr.type}${column.expr.name ? ':' + column.expr.name : ''}`);
                }
                if (
                    column.expr &&
                    (column.expr.type === 'function' || column.expr.type === 'aggr_func') &&
                    column.expr.name.toLowerCase() === 'unwind'
                ) {
                    continue;
                }
                if (
                    column.expr &&
                    column.expr.type === 'function' &&
                    !_allowableFunctions.functionMappings.find(
                        (f) => f.name === column.expr.name.toLowerCase() && (!f.type || f.type === column.expr.type)
                    )
                ) {
                    errors.push(`Function not found: ${column.expr.name}`);
                }
                if (
                    column.expr &&
                    column.expr.type === 'aggr_func' &&
                    !_allowableFunctions.functionMappings.find(
                        (f) => f.name === column.expr.name.toLowerCase() && (!f.type || f.type === column.expr.type)
                    )
                ) {
                    errors.push(`Aggregate function not found: ${column.expr.name}`);
                }
            }
            if (errors.length > 0) {
                throw new Error(errors.join(','));
            }
        }
    }

    /** Parses a SQL string to an AST
     *
     * @param {string|object} sql - the sql statement to parse
     * @param {object} [options] - the AST options
     * @return {object}
     * @throws
     */
    static parseSQLtoAST(sql, options = {}) {
        if ($check.object(sql) && sql.ast) {
            return sql;
        }
        const parser = new Parser();
        let parsedAST;
        try {
            parsedAST = parser.parse(sql, options);
        } catch (exp) {
            throw new Error(
                `${exp.location && exp.location.start ? exp.location.start.line + ':' + exp.location.start.column + ' - ' : ''}${
                    exp.message
                }`
            );
        }
        SQLParser._validateAST(parsedAST);

        return parsedAST;
    }

    /** Checks whether a mongo query can be performed or an aggregate is required
     *
     *
     *
     * @param {string|object} sqlOrAST - the SQL statement or AST to parse
     * @param {object} [options] - the parser options
     * @return {boolean}
     * @throws
     */
    static canQuery(sqlOrAST, options = {}) {
        const parsedAST = SQLParser.parseSQLtoAST(sqlOrAST, options);
        const ast = parsedAST.ast;

        const checkBinaryExpr = (expr) => {
            let hasFunctions = false;
            if (!expr) return false;
            if (expr.type === 'binary_expr') {
                hasFunctions = hasFunctions || checkBinaryExpr(expr.left);
                hasFunctions = hasFunctions || checkBinaryExpr(expr.right);
            } else {
                hasFunctions = hasFunctions || ['function'].includes(expr.type);
            }
            return hasFunctions;
        };

        const asColumns = SQLParser._isSelectAll(ast.columns) ? [] : ast.columns.map((c) => c.as).filter((c) => !!c);
        const checkAsUsedInWhere = (expr) => {
            if (!expr) return false;
            if (expr.type === 'binary_expr') return checkAsUsedInWhere(expr.left) || checkAsUsedInWhere(expr.left);
            if (expr.type === 'column_ref') return !!asColumns.find((c) => c === expr.column);

            return false;
        };

        return !(
            (
                ast.from.length > 1 ||
                !ast.from[0].table ||
                ast.groupby ||
                ast.distinct ||
                (ast.columns !== '*' &&
                    ast.columns.findIndex(
                        (c) =>
                            c.expr.type === 'aggr_func' &&
                            !_allowableFunctions.functionMappings.find(
                                (f) => f.name === c.expr.name.toLowerCase() && (!f.type || f.type === c.expr.type) && f.allowQuery
                            )
                    ) > -1) ||
                (ast.columns !== '*' &&
                    ast.columns.findIndex(
                        (c) =>
                            c.expr.type === 'function' &&
                            !_allowableFunctions.functionMappings.find(
                                (f) => f.name === c.expr.name.toLowerCase() && (!f.type || f.type === c.expr.type) && f.allowQuery
                            )
                    ) > -1) ||
                (ast.columns !== '*' && ast.columns.findIndex((c) => c.expr.type === 'column_ref' && c.expr.column === '*') > -1) ||
                checkBinaryExpr(ast.where) ||
                ast.from.findIndex((f) => !!f.expr) > -1 ||
                (asColumns.length > 0 && checkAsUsedInWhere(ast.where))
            )
            // || (ast.columns!=="*"&&ast.columns.filter(c => c.expr.type === "function" || c.expr.type === "binary_expr").length>0));
        );
    }

    /** Converts a SQL statement to a mongo query.
     *
     * @param {string|object} sqlOrAST - the SQL statement or AST to parse
     * @param {object} [options] - the parser options
     * @return {{limit:number, collection:string, projection:object?, skip:number?, limit:number?, query:object?, sort:object?, count: boolean? }}
     * @throws
     */
    static makeMongoQuery(sqlOrAST, options = {}) {
        const parsedAST = SQLParser.parseSQLtoAST(sqlOrAST, options);
        if (!SQLParser.canQuery(parsedAST)) {
            throw new Error('Query cannot cross multiple collections, have an aggregate function or contain functions in where clauses');
        }
        const ast = parsedAST.ast;
        const parsedQuery = {
            limit: 100,
            collection: ast.from[0].table,
        };

        if (ast.columns && !SQLParser._isSelectAll(ast.columns) && ast.columns.length > 0) {
            parsedQuery.projection = {};

            ast.columns.forEach((v) => {
                if (v.expr.type === 'column_ref') {
                    if (v.as) {
                        parsedQuery.projection[v.as] = `$${v.expr.column}`;
                    } else {
                        parsedQuery.projection[v.expr.column] = `$${v.expr.column}`;
                    }
                } else if ((v.expr.type === 'function' || v.expr.type === 'aggr_func') && v.as) {
                    const parsedExpr = SQLParser._makeProjectionExpressionPart(v.expr);
                    parsedQuery.projection[v.as] = parsedExpr;
                } else if (v.expr.type === 'binary_expr' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
                } else if (v.expr.type === 'case' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeCaseCondition(v.expr);
                } else if (v.expr.type === 'cast' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeCastPart(v.expr);
                } else if (v.expr.type === 'select' && v.as && v.as && v.expr.from) {
                    parsedQuery.projection[v.as] = SQLParser._makeArraySubSelectPart(v.expr);
                } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                    parsedQuery.projection[v.as] = SQLParser._makeObjectFromSelect(v.expr);
                } else if (v.expr.type && v.as) {
                    parsedQuery.projection[v.as] = {$literal: v.expr.value};
                } else if (!v.as) {
                    throw new Error(`Require as for calculation:${v.expr.name}`);
                } else {
                    throw new Error(`Not Supported:${v.expr.type}`);
                }
            });
        }

        if (ast.limit) {
            if (ast.limit.seperator && ast.limit.seperator === 'offset' && ast.limit.value[1] && ast.limit.value[1].value) {
                parsedQuery.limit = ast.limit.value[0].value;
                parsedQuery.skip = ast.limit.value[1].value;
            } else if (ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value) {
                parsedQuery.limit = ast.limit.value[0].value;
            }
        }

        if (ast.where) {
            parsedQuery.query = SQLParser._makeQueryPart(ast.where, true, [], false);
        }

        if (ast.orderby && ast.orderby.length > 0) {
            parsedQuery.sort = ast.orderby.reduce((a, v) => {
                a[v.expr.column || v.expr.value] = v.type === 'DESC' ? -1 : 1;
                return a;
            }, {});
        }

        return parsedQuery;
    }

    /** Parses a sql statement into a mongo aggregate pipeline
     *
     * @param {string|object} sqlOrAST - The sql to make into an aggregate
     * @param {object} [options] - the parser options
     * @param {boolean} [options.unwindJoins] - automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select)
     * @return {{pipeline: *[], collections: *[]}}\
     * @throws
     */
    static makeMongoAggregate(sqlOrAST, options = {unwindJoins: false}) {
        // todo fix sub select table return
        const {ast} = SQLParser.parseSQLtoAST(sqlOrAST, options);

        // subselect for arrays need to remove the collections since theyre actually arrays

        const getTables = (subAst, first) => {
            if (!subAst.from) return [];
            if (!$check.array(subAst.from)) return [];
            let tables = [];
            for (const from of subAst.from) {
                if (from.table) {
                    let table = from.table;
                    if (table && table.endsWith('|first')) table = table.substring(0, table.length - 6);
                    else if (table && table.endsWith('|last')) table = table.substring(0, table.length - 5);
                    else if (table && table.endsWith('|unwind')) table = table.substring(0, table.length - 7);
                    tables.push(table);
                } else if (from.expr && from.expr.ast) {
                    tables = tables.concat(getTables(from.expr.ast, false));
                }
            }

            return tables;
        };

        return {
            pipeline: SQLParser._makeAggregatePipeline(ast, options),
            collections: getTables(ast, true),
        };
    }

    /** Parses sql to either a query or aggregate
     *
     * @param {string} sql - the sql command to parse
     * @param {object} [options] - the parser options
     * @return {object}
     */
    static parseSQL(sql, options = {}) {
        if (!sql) {
            throw new Error('No SQL specified');
        }

        const parsedAST = SQLParser.parseSQLtoAST(sql, options);

        let parsedQuery = {};

        if (SQLParser.canQuery(parsedAST)) {
            parsedQuery = SQLParser.makeMongoQuery(parsedAST, options);
            parsedQuery.type="query";
        } else {
            parsedQuery = SQLParser.makeMongoAggregate(parsedAST, options);
            parsedQuery.type="aggregate";
        }

        return parsedQuery;
    }
}

module.exports = SQLParser;
