const { Parser } = require('node-sql-parser');
const $check = require('check-types');
const merge = require('deepmerge')

const _allowableFunctions = [

    {
        name: "sum",
        parse: (items) => {
            return { $add: items }
        },
    },
    {
        name: "abs",
        parse: (items) => {
            return { $abs: items[0] }
        },
    },
    {
        name: "subtract",
        parse: (items) => {
            return { $subtract: items }
        },
    },
    {
        name: "convert",
        parse: (items) => {
            return { $convert: { input: items[0], to: items[1].$literal } }
        },
    }

];

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
            return merge(SQLParser._makeQueryPart(queryPart.left, ignorePrefix), SQLParser._makeQueryPart(queryPart.right, ignorePrefix))
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

            return { [getColumnNameOrVal(queryPart.left)]: { $regex: regex, $options: 'i' } }
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
            // || (ast.columns !== "*" && ast.columns.findIndex(c => c.expr.type === "aggr_func") > -1)
            || (ast.columns !== "*" && ast.columns.filter(c => c.expr.type === "function" && !_allowableFunctions.find(f => f.name === c.expr.name)).length > 0));
    }

    static _makeProjectionFunctionPart(expr) {
        let fn = _allowableFunctions.find(f => f.name === expr.name);
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
    static parseSQLtoAST(sql, options) {
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
    static makeMongoQuery(sql, options) {
        options = options || {};
        const ast = SQLParser.parseSQLtoAST(sql, options);

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
                } else if (v.expr.type === "aggr_func") {
                    SQLParser.makeAggregate(parsedQuery, v.expr)
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

            if (Object.keys(parsedQuery.projection).length === 0){
                delete parsedQuery.projection
            }
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

    static makeAggregate(parsedQuery, expr) {
        if (expr.name === 'COUNT') {
            parsedQuery.count = true
            parsedQuery.query
        }
    }

    static makeMongoAggregate(ast) {
        let parsedQuery = {
            type: "aggregate",
            pipeline: [],
            buffers: []
        };


        if (ast.from[0].as) {
            parsedQuery.pipeline.push({ $project: { [ast.from[0].as]: "$$ROOT" } })
        }



        if (ast.where) {
            parsedQuery.pipeline.push({ $match: SQLParser._makeQueryPart(ast.where) });
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
            parsedQuery.pipeline.push({
                $project: ast.columns.map(c => {
                    let col = {};
                    if (c.as) {
                        col[c.as] = `$${c.expr.value || c.expr.column}`;
                    } else {
                        col[c.expr.string || c.expr.column] = 1;
                    }
                    return col;
                })
            });
        }

        if (ast.limit) {
            if (ast.limit.seperator && ast.limit.seperator === "offset" && ast.limit.value[1] && ast.limit.value[1].value) {
                parsedQuery.$skip = ast.limit.value[1].value;
            } else if (ast.limit.value && ast.limit.value[0] && ast.limit.value[0].value) {
                parsedQuery.$limit = ast.limit.value[0].value;
            }
        }

        parsedQuery.buffers = ast.from.map(t => t.table);

        return parsedQuery
    }


}

module.exports = SQLParser;
