const { Parser } = require('node-sql-parser');
const $check = require('check-types');
const $merge = require('deepmerge');

const AllowableFunctions = require('./MongoFunctions')

class SQLParser {
    /**Parses a AST QueryPart into a Mongo Query/Match
     *
     * @param {object} queryPart - The AST query part
     * @param {boolean} [ignorePrefix] - Ignore the table prefix
     * @returns {any} - the mongo query/match
     * @private
     */
    static _makeQueryPart(queryPart, ignorePrefix) {
        let result = null;

        const getColumnNameOrVal = (queryPart) => {
            let queryPartToUse = queryPart;
            if (queryPart.left) {
                queryPartToUse = queryPart.left;
            }

            if (queryPartToUse.column) {
                return queryPartToUse.table && !ignorePrefix ? `${queryPartToUse.table}.${queryPartToUse.column}` : queryPartToUse.column;
            } else {
                return queryPartToUse.value;
            }
        };

        if (queryPart.operator === "=") {
            return { [SQLParser._makeQueryPart(queryPart.left, ignorePrefix)]: SQLParser._makeQueryPart(queryPart.right, ignorePrefix) }
        } else if (queryPart.operator === ">") {
            return { [SQLParser._makeQueryPart(queryPart.left, ignorePrefix)]: { $gt: SQLParser._makeQueryPart(queryPart.right, ignorePrefix) } }
        } else if (queryPart.operator === "<") {
            return { [SQLParser._makeQueryPart(queryPart.left, ignorePrefix)]: { $lt: SQLParser._makeQueryPart(queryPart.right, ignorePrefix) } }
        } else if (queryPart.operator === ">=") {
            return { [SQLParser._makeQueryPart(queryPart.left, ignorePrefix)]: { $gte: SQLParser._makeQueryPart(queryPart.right, ignorePrefix) } }
        } else if (queryPart.operator === "<=") {
            return { [SQLParser._makeQueryPart(queryPart.left, ignorePrefix)]: { $lte: SQLParser._makeQueryPart(queryPart.right, ignorePrefix) } }
        } else if (queryPart.operator === "!=") {
            return { [SQLParser._makeQueryPart(queryPart.left, ignorePrefix)]: { $ne: SQLParser._makeQueryPart(queryPart.right, ignorePrefix) } }
        } else if (queryPart.operator === "AND") {
            return $merge(SQLParser._makeQueryPart(queryPart.left, ignorePrefix), SQLParser._makeQueryPart(queryPart.right, ignorePrefix))
        } else if (queryPart.operator === "OR") {
            return { $or: [SQLParser._makeQueryPart(queryPart.left, ignorePrefix), SQLParser._makeQueryPart(queryPart.right, ignorePrefix)] }
        } else if (queryPart.operator === "IN") {
            //todo
            return { $in: [SQLParser._makeQueryPart(queryPart.left, ignorePrefix), SQLParser._makeQueryPart(queryPart.right, ignorePrefix)] }
        } else if (queryPart.operator === "LIKE") {
            let likeVal = queryPart.right.value;
            if (!likeVal) return null;
            let regex = likeVal;
            if (likeVal.startsWith('%') && likeVal.endsWith('%')) {
                regex = new RegExp(`${likeVal.substring(1, likeVal.length - 1)}`);
            } else if (likeVal.startsWith('%')) {
                regex = new RegExp(`${likeVal.substring(1)}$`);
            } else if (likeVal.endsWith('%')) {
                regex = new RegExp(`^${likeVal.substring(0, likeVal.length - 1)}`);
            } else {
                regex = `^${likeVal}$`;
            }

            return { [getColumnNameOrVal(queryPart.left)]: { $regex: regex.source, $options: 'i' } }
        } else {
            let actualVal = getColumnNameOrVal(queryPart);
            if (actualVal && actualVal.startsWith && actualVal.startsWith('$date:')) {
                return { $date: actualVal.substring(6) };
            } else if (actualVal && actualVal.startsWith && actualVal.startsWith('$bool:')) {
                return { $bool: actualVal.substring(6) };
            } else if (actualVal && actualVal.startsWith && actualVal.startsWith('$objectId:')) {
                return { $objectId: actualVal.substring(10) };
            } else if (actualVal && actualVal.startsWith && actualVal.startsWith('$int:')) {
                return { $int: actualVal.substring(5) };
            } else if (actualVal && actualVal.startsWith && actualVal.startsWith('$float:')) {
                return { $float: actualVal.substring(7) };
            } else {
                return actualVal;
            }

        }
    }

    /**Checks whether a mongo query can be performed
     *
     * @param {object} ast - the parsed SQL AST
     * @returns {boolean}
     * @private
     */
    static _canQuery(ast) {
        return !(
            ast.from.length > 1
            || ast.groupby
            || ast.distinct
            || (ast.columns !== "*" && ast.columns.findIndex(c => c.expr.type === "aggr_func") > -1)
            // || (ast.columns.filter(c => c.expr.type === "function" && !AllowableFunctions._list.find(f => f.name === c.expr.name)).length > 0));
            || (ast.columns.filter(c => c.expr.type === "function" || c.expr.type === "binary_expr")));
    }

    static _makeProjectionFunctionPart(expr) {
        let fn = AllowableFunctions._list.find(f => f.name === expr.name.toLowerCase());
        if (!fn) {
            throw new Error(`Function:${expr.name} not available`);
        }
        if (expr.args && expr.args.value) {
            let args = $check.array(expr.args.value) ? expr.args.value : [expr.args.value];
            return fn.parse(args.map(a => {
                if (a.type === "function") {
                    return SQLParser._makeProjectionFunctionPart(a)
                } else if (a.type === "column_ref") {
                    return `$${a.column}`;
                } else {
                    return { $literal: a.value };
                }
            }));
        } else {
            return undefined;
        }


    }

    /**Parses a SQL string to an AST
     *
     * @param {string} sql - the sql statement to parse
     * @param {object} [options] - the AST options
     * @returns {AST[] | AST}
     * @throws
     */
    static _parseSQLtoAST(sql, options) {
        options = options || {};
        const parser = new Parser();
        const ast = parser.astify(sql);
        return ast;
    }

    /**Converts a SQL statement to a mongo query
     *
     * @param {string} sql - the SQL statement to parse
     * @param {object} [options] - the parser options
     * @returns {{limit:number, collection:string, projection:object?, skip:number?, limit:number?, query:object?, sort:object?, count: boolean? }}
     * @throws
     */
    //todo make accept string or ast
    static _makeMongoQuery(ast, options = {}) {
        if (!SQLParser._canQuery(ast)) {
            throw new Error("Query cannot cross multiple collections or have an aggregate")
        }


        let parsedQuery = {
            limit: 100,
            collection: ast.from[0].table
        };

        if (ast.columns && ast.columns !== "*" && ast.columns.length > 0) {
            parsedQuery.projection = {};

            ast.columns.forEach(v => {
                if (v.expr.type === "column_ref") {
                    if (v.as) {
                        parsedQuery.projection[v.as] = `$${v.expr.column}`;
                    } else {
                        parsedQuery.projection[v.expr.column] = 1;
                    }
                } else if (v.expr.type === "function" && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeProjectionFunctionPart(v.expr);
                } else if (v.expr.type === "case" && v.as) {
                    throw new Error('Case not supported')
                } else if (v.expr.type && v.as) {
                    parsedQuery.projection[v.as] = { $literal: v.expr.value }
                } else if (!v.as) {
                    throw new Error(`Require as for calculation:${v.expr.name}`)
                } else {
                    throw new Error(`Not Supported:${v.expr.type}`)
                }
            });

        }

        if (ast.limit) {
            if (ast.limit.seperator && ast.limit.seperator === "offset" && ast.limit.value[1] && ast.limit.value[1].value) {
                parsedQuery.limit = ast.limit.value[0].value;
                parsedQuery.skip = ast.limit.value[1].value;
            } else if (ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value) {
                parsedQuery.limit = ast.limit.value[0].value;
            }
        }

        if (ast.where) {
            parsedQuery.query = SQLParser._makeQueryPart(ast.where, true);
        }

        if (ast.orderby && ast.orderby.length > 0) {
            parsedQuery.sort = ast.orderby.reduce((a, v) => {
                a[v.expr.column || v.expr.value] = v.type === "DESC" ? -1 : 1;
                return a;
            }, {});
        }



        return parsedQuery;
    }

    static _getParsedValueFromBinaryExpression(expressionPart) {
        let result;

        if (expressionPart.type === 'binary_expr') {
            result = SQLParser._binaryExpressionLogic(expressionPart)
        } else if (expressionPart.type === 'column_ref') {
            result = `$${expressionPart.column}`
        } else if (expressionPart.type === 'number') {
            result = expressionPart.value
        }

        return result
    }

    static _binaryExpressionLogic(column) {
        let operator
        if (column.expr) {
            operator = column.expr.operator
        } else {
            operator = column.operator
        }

        const aggregateFunction = AllowableFunctions._list.find(f => f.name === operator.toLowerCase());
        let aggregateResult;
        if (aggregateFunction) {
            if (column.expr && column.expr.left && column.expr.right) {
                const leftPartValue = SQLParser._getParsedValueFromBinaryExpression(column.expr.left);
                const rightPartValue = SQLParser._getParsedValueFromBinaryExpression(column.expr.right);

                aggregateResult = aggregateFunction.parse(leftPartValue, rightPartValue);
            } else if (column.left && column.right) {
                const leftPartValue = SQLParser._getParsedValueFromBinaryExpression(column.left);
                const rightPartValue = SQLParser._getParsedValueFromBinaryExpression(column.right);

                aggregateResult = aggregateFunction.parse(leftPartValue, rightPartValue);
            }
        }

        return aggregateResult;
    }

    static _aggregationFunctionLogic(column) {
        let aggregateResult;

        const aggregateFunction = AllowableFunctions._list.find(f => f.name.toLowerCase() === column.expr.name.toLowerCase());
        if (aggregateFunction) {
            if (column.expr && column.expr.args && column.expr.args.expr && column.expr.args.expr.args) {
                const returnValue = SQLParser._aggregationFunctionLogic(column.expr.args)
                aggregateResult = AllowableFunctions.parse(aggregateFunction, returnValue)
            }

            if (column.expr && column.expr.args && column.expr.args.expr && column.expr.args.expr.column) {
                aggregateResult = aggregateFunction.parse(column.expr.args.expr.column);
            } else if (column.expr && column.expr.args && column.expr.args.expr && column.expr.args.expr.value) {
                aggregateResult = aggregateFunction.parse(column.expr.args.expr.value);
            } else if (column.expr && column.expr.args && column.expr.args.value && column.expr.args.value.length) {
                if (column.expr.args.value.length > 1) {
                    aggregateResult = AllowableFunctions.parse(aggregateFunction,
                        [column.expr.args.value[0].column || column.expr.args.value[0].value, column.expr.args.value[1].column || column.expr.args.value[1].value]);
                } else if (column.expr.args.value[0] && column.expr.args.value[0].column) {
                    aggregateResult = AllowableFunctions.parse(aggregateFunction, column.expr.args.value[0].column);
                }
            }


        }


        return aggregateResult;
    }

    static _joinFunctionLogic(on) {
        let localField;
        let from;
        let foreignField;
        let as;

        localField = on.left.column;

        const splitedRightColumn = on.right.column.split('.')
        from = splitedRightColumn[0]
        foreignField = splitedRightColumn[1]
        as = from

        return {
            localField,
            from,
            foreignField,
            as
        }
    }

    //todo make accept string or ast
    static _makeMongoAggregate(ast, options = {}) {
        let parsedQuery = {
            pipeline: [],
            collections: []
        };

        if (ast.columns && ast.columns !== "*" && ast.columns.length > 0) {
            if (ast.columns.find(column => {
                if (column.expr && column.expr.name && column.expr.name.toLowerCase() === 'unwind') {
                    return true
                }
            })) {
                let column = ast.columns.find(column => {
                    if (column.expr && column.expr.name && column.expr.name.toLowerCase() === 'unwind') {
                        return column
                    }
                })

                let unwind = `$${column.expr.args.value[0].column}`;

                if (unwind) {
                    parsedQuery.pipeline.push({
                        $unwind: unwind
                    })
                }


            }

        }

        if (ast.from.length) {
            let lookup = {};

            // parsedQuery.pipeline.push({ $project: { [ast.from[0].as || ast.from[0].table]: "$$ROOT" } })
            ast.from.forEach(from => {
                if (from.join) {
                    lookup = SQLParser._joinFunctionLogic(from.on)
                }

                if (Object.keys(lookup).length > 0) {
                    parsedQuery.pipeline.push({
                        $lookup: lookup
                    })
                }
            })
        }



        if (ast.where) {
            if (ast.where.operator === "IN") {
                const aggregate = SQLParser._makeMongoAggregate(ast.where.right.value[0])
                parsedQuery.pipeline.push({
                    $lookup: {
                        from: aggregate.collections[0],
                        pipeline: aggregate.pipeline,
                        as: ast.columns.filter(column => {
                            return column.expr.column === ast.where.left.column
                        })[0].as || ast.where.left.column
                    }
                });
            } else {
                parsedQuery.pipeline.push({ $match: SQLParser._makeQueryPart(ast.where) });
            }
        }

        if (ast.orderby && ast.orderby.length > 0) {
            parsedQuery.pipeline.push({
                $sort: ast.orderby.reduce((a, v) => {
                    a[v.expr.column || v.expr.value] = v.type === "DESC" ? -1 : 1;
                    return a;
                }, {})
            });
        }


        if (ast.columns && ast.columns !== "*" && ast.columns.length > 0) {
            let group = {};
            let project = {};


            ast.columns.forEach(column => {
                if (column.as || column.expr.name || column.expr.column) {
                    project[column.as || column.expr.column] = 1;

                    let aggregateResult;

                    if (column.expr.name) {
                        aggregateResult = SQLParser._aggregationFunctionLogic(column)
                    } else if (column.expr.type === 'binary_expr') {
                        aggregateResult = SQLParser._binaryExpressionLogic(column);
                    }

                    if (aggregateResult) {
                        if (ast.groupby && ast.groupby.length) {
                            group._id = `$${ast.groupby[0].column}`;
                            if (column.as) {
                                group[column.as] = aggregateResult;
                            } else {
                                group = $merge(group, aggregateResult);
                            }
                        } else {
                            project[column.as] = aggregateResult;
                        }
                    }

                } else {
                    // TODO fix problem undefind key

                }

            });

            if (Object.keys(group).length > 0) {
                parsedQuery.pipeline.push({
                    $group: group
                })
            }

            if (Object.keys(project).length > 0) {
                parsedQuery.pipeline.push({
                    $project: project
                })
            }
        }

        if (ast.having) {
            parsedQuery.pipeline.push({ $match: SQLParser._makeQueryPart(ast.having) });
        }

        if (ast.limit) {
            if (ast.limit.seperator && ast.limit.seperator === "offset" && ast.limit.value[1] && ast.limit.value[1].value) {
                parsedQuery.$skip = ast.limit.value[1].value;
            } else if (ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value) {
                parsedQuery.$limit = ast.limit.value[0].value;
            }
        }

        parsedQuery.collections = ast.from.map(t => t.table);

        return parsedQuery
    }

    /**Parses sql to either a query or aggregate
     *
     * @param sql
     * @param type
     * @param options
     */
    static parseSQL(sql, options = { unwindJoins: false }) {
        if (!sql) {
            throw new Error("No SQL specified")
        }

        const ast = SQLParser._parseSQLtoAST(sql, options);
        if (!ast.from) {
            throw new Error("No from specified")
        }

        let parsedQuery = {}

        if (SQLParser._canQuery(ast)) {
            parsedQuery = SQLParser._makeMongoQuery(ast, options)
        } else {
            parsedQuery = SQLParser._makeMongoAggregate(ast, options)
        }

        return parsedQuery
    }
}

module.exports = SQLParser;
