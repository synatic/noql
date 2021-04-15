const {Parser} = require('node-sql-parser');
const $check = require('check-types');
const $merge = require('deepmerge');

const _allowableFunctions = require('./MongoFunctions');

class SQLParser {


    static _makeRegex(likeVal){
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

    /**Parses a AST QueryPart into a Mongo Query/Match
     *
     * @param {object} queryPart - The AST query part
     * @param {boolean} [ignorePrefix] - Ignore the table prefix
     * @param {array}  [allowedTypes] - Expression types to allow
     * @param {boolean} [includeThis] - include $$this in expresions
     * @returns {any} - the mongo query/match
     * @private
     */
    static _makeQueryPart(queryPart, ignorePrefix,allowedTypes=[],includeThis=false) {
        let result = null;

        if(allowedTypes.length>0&&!allowedTypes.includes(queryPart.type))throw new Error(`Type not allowed for query:${queryPart.type}`)

        const getColumnNameOrVal = (queryPart) => {
            let queryPartToUse = queryPart;
            if (queryPart.left) {
                queryPartToUse = queryPart.left;
            }

            if (queryPartToUse.column) {
                return (includeThis?'$$this.':'') + (queryPartToUse.table && !ignorePrefix ? `${queryPartToUse.table}.${queryPartToUse.column}` : queryPartToUse.column);
            } else {
                return queryPartToUse.value;
            }
        };

        const makeOperator=(op)=>{
            const left=SQLParser._makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis);
            const right=SQLParser._makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis);
            if($check.string(left)&&!left.startsWith('$'))return {[left]: {[op]: right}};
            else return {$expr:{[op]:[left,right]}};
         }

        if(queryPart.type==="binary_expr") {
            if (queryPart.operator === '=') return makeOperator('$eq');
            if (queryPart.operator === '>') return makeOperator('$gt');
            if (queryPart.operator === '<') return makeOperator('$lt');
            if (queryPart.operator === '>=') return makeOperator('$gte');
            if (queryPart.operator === '<=') return makeOperator('$lte');
            if (queryPart.operator === '!=') return makeOperator('$ne');
            if (queryPart.operator === 'AND') return $merge(SQLParser._makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), SQLParser._makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis));
            if(queryPart.operator === 'OR') return {$or: [SQLParser._makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), SQLParser._makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
            if (queryPart.operator === 'IN') return {$in: [SQLParser._makeQueryPart(queryPart.left, ignorePrefix,allowedTypes,includeThis), SQLParser._makeQueryPart(queryPart.right, ignorePrefix,allowedTypes,includeThis)]};
            if (queryPart.operator === 'LIKE') {
                let likeVal = queryPart.right.value;
                const regex=SQLParser._makeRegex(likeVal);
                return {[getColumnNameOrVal(queryPart.left)]: {$regex: regex, $options: 'i'}};
            }
            throw new Error(`Unsupported operator:${queryPart.operator}`);
        }

        if(queryPart.type==="function"||queryPart.type==="select")return SQLParser._makeProjectionExpressionPart(queryPart);

        return getColumnNameOrVal(queryPart);
    }

    /** Makes a projection expression sub part.
     *
     * @param expr
     * @param depth
     * @returns {undefined|*}
     * @private
     */
    static _makeProjectionExpressionPart(expr, depth = 0) {
        let fn = _allowableFunctions.functionList.find(
            (f) => f.name.toLowerCase() === (expr.name || expr.operator).toLowerCase() && (!f.type || f.type === expr.type)
        );
        if (!fn) {
            throw new Error(`Function:${expr.name} not available`);
        }

        if (expr.args && expr.args.value) {
            let args = $check.array(expr.args.value) ? expr.args.value : [expr.args.value];
            return fn.parse(
                args.map((a) => {
                    //todo check all these types
                    if (a.type === 'function') {
                        return SQLParser._makeProjectionExpressionPart(a);
                    } else if (a.type === 'column_ref') {
                        return `$${a.column}`;
                    } else if (a.type === 'binary_expr') {
                        return SQLParser._getParsedValueFromBinaryExpression(a);
                    } else if (a.type === 'select') {
                        return SQLParser._makeArraySubSelectPart(a, depth);
                    } else {
                        return {$literal: a.value};
                    }
                }),
                depth
            );
        } else if (expr.left && expr.right) {
            return SQLParser._getParsedValueFromBinaryExpression(expr);
        } else {
            throw new Error('Unable to parse expression');
        }
    }

    /** Makes an expression tree from the cast statement
     *
     * @param {object} expr - the AST expression that is a cast
     * @returns {*}
     * @private
     */
    static _makeCastPart(expr) {
        if (expr.type !== 'cast') throw new Error(`Invalid type for cast:${expr.type}`);
        let convFN = _allowableFunctions.functionList.find((f) => f.name === 'convert');
        if (!convFN) throw new Error('No conversion function found');
        let to = expr.target.dataType.toLowerCase();

        if (expr.expr.column) return convFN.parse([`$${expr.expr.column}`, to]);
        if (expr.expr.value) return convFN.parse([expr.expr.value, to]);
        return convFN.parse([SQLParser._makeProjectionExpressionPart(expr.expr), to]);
    }

    static _getParsedValueFromBinaryExpression(expressionPart) {
        if (expressionPart.type === 'binary_expr') return SQLParser._binaryExpressionLogic(expressionPart);
        if (expressionPart.type === 'column_ref') return `$${expressionPart.column}`;
        if (expressionPart.type === 'number') return expressionPart.value;
        if (expressionPart.type === 'function') return SQLParser._makeProjectionExpressionPart(expressionPart);

        throw new Error(`Unable to make binary expression part:${expressionPart.type}`);
    }

    /** Makes an array expression from a sub select
     *
     * @param {object} ast - the ast to create a sub select from
     * @param {int} depth - the depth of the query, automatically set
     * @returns {{$map: {input: {object|string}, in: object}}}
     * @private
     */
    static _makeArraySubSelectPart(ast, depth = 0) {
        if (!ast || !ast.from || !ast.from.length || ast.from.length === 0) throw new Error('Invalid array sub select');
        if (!SQLParser.canQuery({ast: ast})) throw new Error('Array sub select does not support aggregation methods');

        let mapIn = '$$this';
        if (ast.columns && ast.columns !== '*' && ast.columns.length > 0) {
            mapIn = {};

            ast.columns.forEach((v) => {
                if (v.expr.type === 'column_ref') {
                    if (v.as) {
                        mapIn[v.as] = `$$this.${v.expr.column}`;
                    } else {
                        mapIn[v.expr.column] = `$$this.${v.expr.column}`;
                    }
                } else if (v.expr.type === 'function' && v.as) {
                    mapIn[v.as] = SQLParser._makeProjectionExpressionPart(v.expr, depth + 1);
                } else if (v.expr.type === 'aggr_func' && v.as) {
                    mapIn[v.as] = SQLParser._makeProjectionExpressionPart(v.expr, depth + 1);
                } else if (v.expr.type === 'binary_expr' && v.as) {
                    mapIn[v.as] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
                } else if (v.expr.type === 'case' && v.as) {
                    throw new Error('Case not supported in array sub select');
                } else if (v.expr.type === 'select' && v.as) {
                    mapIn[v.as] = SQLParser._makeArraySubSelectPart(v.expr, depth + 1);
                } else if (v.expr.type && v.as) {
                    mapIn[v.as] = {$literal: v.expr.value};
                } else if (!v.as) {
                    throw new Error(`Require as for array subselect calculation:${v.expr.name}`);
                } else {
                    throw new Error(`Not Supported:${v.expr.type}`);
                }
            });
        }

        let mapInput=null;


        const makeFilterCond=(queryPart)=>{
            if(queryPart.type==="binary_expr") {
                if (queryPart.operator === '=') return {$eq:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === '>') return {$gt:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === '<')  return {$lt:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === '>=') return {$gte:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === '<=') return {$lte:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === '!=') return {$ne:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === 'AND') return {$and:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if(queryPart.operator === 'OR') return {$or:[makeFilterCond(queryPart.left),makeFilterCond(queryPart.right)]};
                if (queryPart.operator === 'LIKE') {
                    let likeVal = queryPart.right.value;
                    const regex=SQLParser._makeRegex(likeVal);

                    return {$regexMatch:{input:makeFilterCond(queryPart.left),regex: regex, options: 'i'}};
                }
                throw new Error(`Unsupported operator:${queryPart.operator}`);
            }

            if(queryPart.type==='column_ref')return `$$this.${queryPart.column}`;
            if(queryPart.type==='number'||queryPart.type==='string')return queryPart.value;
            throw new Error(`invalid expression type for array sub select:${queryPart.type}`);
        }

        if(ast.where){
            mapInput= {
                $filter: {
                    input: `$${depth > 0 ? '$this.' : ''}${ast.from[0].table}`,
                    cond:{$and:[makeFilterCond(ast.where)]}
                }
            }
        }else if (ast.from[0].table) {
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

    static _binaryExpressionLogic(column) {
        let operator;
        if (column.expr) {
            operator = column.expr.operator;
        } else {
            operator = column.operator;
        }

        const aggregateFunction = _allowableFunctions.functionList.find((f) => f.name === operator.toLowerCase());
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

        const aggregateFunction = _allowableFunctions.functionList.find((f) => f.name.toLowerCase() === column.expr.name.toLowerCase());
        if (!aggregateFunction) throw new Error(`Function not found:${column.expr.name}`);

        if (column.expr && column.expr.args && column.expr.args.expr && column.expr.args.expr.args) {
            const returnValue = SQLParser._aggregationFunctionLogic(column.expr.args);
            aggregateResult = _allowableFunctions.parse(aggregateFunction, returnValue);
        }

        if (column.expr && column.expr.args && column.expr.args.expr && column.expr.args.expr.column) {
            aggregateResult = aggregateFunction.parse(column.expr.args.expr.column);
        } else if (column.expr && column.expr.args && column.expr.args.expr && column.expr.args.expr.value) {
            aggregateResult = aggregateFunction.parse(column.expr.args.expr.value);
        } else if (column.expr && column.expr.args && column.expr.args.value && column.expr.args.value.length) {
            if (column.expr.args.value.length > 1) {
                aggregateResult = _allowableFunctions.parse(aggregateFunction, [
                    column.expr.args.value[0].column || column.expr.args.value[0].value,
                    column.expr.args.value[1].column || column.expr.args.value[1].value,
                ]);
            } else if (column.expr.args.value[0] && column.expr.args.value[0].column) {
                aggregateResult = _allowableFunctions.parse(aggregateFunction, [`$${column.expr.args.value[0].column}`]);
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

        const splitedRightColumn = on.right.column.split('.');
        from = splitedRightColumn[0];
        foreignField = splitedRightColumn[1];
        as = from;

        return {
            localField,
            from,
            foreignField,
            as,
        };
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
        if(!ast.from||!ast.from[0])throw new Error('No FROM specified');
        if(!ast.from[0].table)throw new Error('Initial from must be a collection reference');
        if ($check.array(ast.columns)) {
            let errors = [];
            for (let column of ast.columns) {
                if (column.expr && ['function', 'binary_expr', 'aggr_func'].includes(column.expr.type) && !column.as) {
                    errors.push(`Requires as for ${column.expr.type}${column.expr.name ? ':' + column.expr.name : ''}`);
                }
            }
            if (errors.length > 0) {
                throw new Error(errors.join(','));
            }
        }
    }

    /**Parses a SQL string to an AST
     *
     * @param {string|object} sql - the sql statement to parse
     * @param {object} [options] - the AST options
     * @returns {object}
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
     * @returns {boolean}
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

        const asColumns=ast.columns==='*'?[]:ast.columns.map(c=>c.as).filter(c=>!!c);
        const checkAsUsedInWhere=(expr)=>{
            if(!expr)return false;
            if(expr.type==='binary_expr')return checkAsUsedInWhere(expr.left)||checkAsUsedInWhere(expr.left);
            if(expr.type==='column_ref')return !!asColumns.find(c=>c===expr.column);

            return false;
        };

        return !(
            (
                ast.from.length > 1
                || ast.groupby
                || ast.distinct
                || (ast.columns !== '*' &&
                    ast.columns.findIndex(
                        (c) =>
                            c.expr.type === 'aggr_func' &&
                            !_allowableFunctions.functionList.find(
                                (f) => f.name === c.expr.name.toLowerCase() && (!f.type || f.type === c.expr.type) && f.allowQuery
                            )
                    ) > -1)
                || (ast.columns !== '*' &&
                    ast.columns.findIndex(
                        (c) =>
                            c.expr.type === 'function' &&
                            !_allowableFunctions.functionList.find(
                                (f) => f.name === c.expr.name && (!f.type || f.type === c.expr.type) && f.allowQuery
                            )
                    ) > -1)
               || (ast.columns !== '*' && ast.columns.findIndex((c) => c.expr.type === 'column_ref' && c.expr.column === '*') > -1)
               || checkBinaryExpr(ast.where)
               || ast.from.findIndex((f) => !!f.expr) > -1
               || (asColumns.length>0&&checkAsUsedInWhere(ast.where))
            )
            //|| (ast.columns!=="*"&&ast.columns.filter(c => c.expr.type === "function" || c.expr.type === "binary_expr").length>0));
        );
    }

    /**Converts a SQL statement to a mongo query.
     *
     * @param {string|object} sqlOrAST - the SQL statement or AST to parse
     * @param {object} [options] - the parser options
     * @returns {{limit:number, collection:string, projection:object?, skip:number?, limit:number?, query:object?, sort:object?, count: boolean? }}
     * @throws
     */
    static makeMongoQuery(sqlOrAST, options = {}) {
        const parsedAST = SQLParser.parseSQLtoAST(sqlOrAST, options);
        if (!SQLParser.canQuery(parsedAST)) {
            throw new Error('Query cannot cross multiple collections, have an aggregate function or contain functions in where clauses');
        }
        const ast = parsedAST.ast;
        let parsedQuery = {
            limit: 100,
            collection: ast.from[0].table,
        };

        if (ast.columns && ast.columns !== '*' && ast.columns.length > 0) {
            parsedQuery.projection = {};

            ast.columns.forEach((v) => {
                if (v.expr.type === 'column_ref') {
                    if (v.as) {
                        parsedQuery.projection[v.as] = `$${v.expr.column}`;
                    } else {
                        parsedQuery.projection[v.expr.column] = 1;
                    }
                } else if (v.expr.type === 'function' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeProjectionExpressionPart(v.expr);
                } else if (v.expr.type === 'aggr_func' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeProjectionExpressionPart(v.expr);
                } else if (v.expr.type === 'binary_expr' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._getParsedValueFromBinaryExpression(v.expr);
                } else if (v.expr.type === 'case' && v.as) {
                    throw new Error('Case not supported');
                } else if (v.expr.type === 'cast' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeCastPart(v.expr);
                } else if (v.expr.type === 'select' && v.as) {
                    parsedQuery.projection[v.as] = SQLParser._makeArraySubSelectPart(v.expr);
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
            parsedQuery.query = SQLParser._makeQueryPart(ast.where, true);
        }

        if (ast.orderby && ast.orderby.length > 0) {
            parsedQuery.sort = ast.orderby.reduce((a, v) => {
                a[v.expr.column || v.expr.value] = v.type === 'DESC' ? -1 : 1;
                return a;
            }, {});
        }

        return parsedQuery;
    }

    static _makeAggregatePipeline(ast) {
        let pipeline = [];

        if (ast.columns && ast.columns !== '*' && ast.columns.length > 0) {
            if (
                ast.columns.find((column) => {
                    if (column.expr && column.expr.name && column.expr.name.toLowerCase() === 'unwind') {
                        return true;
                    }
                })
            ) {
                let column = ast.columns.find((column) => {
                    if (column.expr && column.expr.name && column.expr.name.toLowerCase() === 'unwind') {
                        return column;
                    }
                });

                let unwind = `$${column.expr.args.value[0].column}`;

                if (unwind) {
                    pipeline.push({
                        $unwind: unwind,
                    });
                }
            }
        }

        if (ast.from.length) {
            let lookups = [];
            // parsedQuery.pipeline.push({ $project: { [ast.from[0].as || ast.from[0].table]: "$$ROOT" } })

            ast.from.forEach(from => {
                if(from.expr && from.expr.ast){
                    lookups.push({
                        from: from.expr.ast.from[0].table,
                        pipeline: SQLParser._makeAggregatePipeline(from.expr.ast),
                        as: from.as
                    })
                }

                if (from.join) {
                    lookups.push(SQLParser._joinFunctionLogic(from.on));
                }
            })

            if (Object.keys(lookups).length > 0) {
                lookups.forEach(lookup =>{
                    pipeline.push({
                        $lookup: lookup
                    })
                })
            }
        }

        if (ast.where) {
            if (ast.where.operator === 'IN') {
                const aggregate = SQLParser.makeMongoAggregate(ast.where.right.value[0]);
                pipeline.push({
                    $lookup: {
                        from: aggregate.collections[0],
                        pipeline: aggregate.pipeline,
                        as:
                            ast.columns.filter((column) => {
                                return column.expr.column === ast.where.left.column;
                            })[0].as || ast.where.left.column,
                    },
                });
            } else {
                pipeline.push({$match: SQLParser._makeQueryPart(ast.where)});
            }
        }

        if (ast.orderby && ast.orderby.length > 0) {
            pipeline.push({
                $sort: ast.orderby.reduce((a, v) => {
                    a[v.expr.column || v.expr.value] = v.type === 'DESC' ? -1 : 1;
                    return a;
                }, {}),
            });
        }

        if (ast.columns && ast.columns !== '*' && ast.columns.length > 0) {
            let group = {};
            let project = {};
            let projectOptionName = '$project';
            let groupByForCount = false;

            const setIndex = ast.columns.findIndex((column) => column.expr.column === '*');
            if (setIndex >= 0) {
                projectOptionName = '$set';
            }

            ast.columns.forEach((column, index) => {
                if (setIndex !== index) {
                    if (column.as || column.expr.name || column.expr.column) {
                        project[column.as || column.expr.column || (column.expr && column.expr.name)] = 1;

                        let aggregateResult;

                        if (column.expr.name) {
                            aggregateResult = SQLParser._aggregationFunctionLogic(column);
                            if (column.expr.name.toLowerCase() === 'count') {
                                groupByForCount = true;
                            }
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
                            } else if (groupByForCount) {
                                group._id = {};
                                if (column.as) {
                                    group[column.as] = aggregateResult;
                                } else {
                                    group = $merge(group, aggregateResult);
                                }
                                delete project[column.as || column.expr.column];
                            } else {
                                project[column.as || (column.expr && column.expr.name)] = aggregateResult;
                            }
                        }
                    } else {
                        // TODO fix problem undefind key
                    }
                }
            });

            if (Object.keys(group).length > 0) {
                pipeline.push({
                    $group: group,
                });
            }

            if (Object.keys(project).length > 0) {
                pipeline.push({
                    [projectOptionName]: project,
                });
            }
        }

        if (ast.having) {
            pipeline.push({$match: SQLParser._makeQueryPart(ast.having)});
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

    /** Parses a sql statement into a mongo aggregate pipeline
     *
     * @param {string|object} sqlOrAST - The sql to make into an aggregate
     * @param {object} [options] - the parser options
     * @param {boolean} [options.unwindJoins] - automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select)
     * @returns {{pipeline: *[], collections: *[]}}\
     * @throws
     */
    static makeMongoAggregate(sqlOrAST, options = {unwindJoins: false}) {
        const {ast, tableList} = SQLParser.parseSQLtoAST(sqlOrAST, options);
        return {
            pipeline: SQLParser._makeAggregatePipeline(ast),
            collections: tableList.map((t) => t.split('::')[2]),
        };
    }

    /**Parses sql to either a query or aggregate
     *
     * @param {string} sql - the sql command to parse
     * @param {object} [options] - the parser options
     * @param {boolean} [options.unwindJoins] - automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select)
     */
    static parseSQL(sql, options = {unwindJoins: false}) {
        if (!sql) {
            throw new Error('No SQL specified');
        }

        const parsedAST = SQLParser.parseSQLtoAST(sql, options);

        let parsedQuery = {};

        if (SQLParser.canQuery(parsedAST)) {
            parsedQuery = SQLParser.makeMongoQuery(parsedAST, options);
        } else {
            parsedQuery = SQLParser.makeMongoAggregate(parsedAST, options);
        }

        return parsedQuery;
    }
}

module.exports = SQLParser;
