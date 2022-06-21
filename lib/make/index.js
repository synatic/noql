const $check = require('check-types');
const {isSelectAll} = require('../isSelectAll');
const _allowableFunctions = require('../MongoFunctions');
const {parseSQLtoAST} = require('../parseSQLtoAST');
const {canQuery, whereContainsOtherTable} = require('../canQuery');
const {sqlStringToRegex} = require('./sqlStringToRegex');
const $json = require('@synatic/json-magic');
const {createResultObject} = require('./createResultObject');
const {getWhereAstQueries, getWhereStandardQueries} = require('./filter-queries');
/**
 * @typedef {import('../types').ParserInput} ParserInput
 * @typedef {import('../types').ParserResult} ParserResult
 * @typedef {import('../types').ParserOptions} ParserOptions
 * @typedef {import('../types').MongoQuery} MongoQuery
 * @typedef {import('../types').MongoAggregate} MongoAggregate
 * @typedef {import('../types').AstLike} AstLike
 * @typedef {import('../types').PipelineFn} PipelineFn
 * @typedef {import('../types').Column} Column
 * @typedef {import('../types').ColumnParseResult} ColumnParseResult
 */
/**
 * Parses a sql statement into a mongo aggregate pipeline
 *
 * @param {ParserInput} sqlOrAST - The sql to make into an aggregate
 * @param {ParserOptions} [options] - the parser options
 * @returns {MongoAggregate}
 * @throws
 */
function makeMongoAggregate(sqlOrAST, options = {unwindJoins: false}) {
    // todo fix sub select table return
    const {ast} = parseSQLtoAST(sqlOrAST, options);

    // subselect for arrays need to remove the collections since theyre actually arrays

    const getTables = (subAst, first) => {
        let tables = [];

        if (whereContainsOtherTable(subAst.where)) {
            const queries = getWhereAstQueries(subAst.where);
            const subTables = queries.flatMap((q) => q.ast).flatMap(getTables);
            tables = tables.concat(subTables);
        }
        if (!subAst.from) {
            return tables;
        }
        if (!$check.array(subAst.from)) {
            return tables;
        }
        for (const from of subAst.from) {
            if (from.table) {
                let table = from.table;
                if (table && table.endsWith('|first')) {
                    table = table.substring(0, table.length - 6);
                } else if (table && table.endsWith('|last')) {
                    table = table.substring(0, table.length - 5);
                } else if (table && table.endsWith('|unwind')) {
                    table = table.substring(0, table.length - 7);
                }
                tables.push(table);
            } else if (from.expr && from.expr.ast) {
                tables = tables.concat(getTables(from.expr.ast, false));
            }
        }

        return tables;
    };

    return {
        pipeline: makeAggregatePipeline(ast, options),
        collections: getTables(ast, true),
    };
}

/**
 * Converts a SQL statement to a mongo query.
 *
 * @param {ParserInput} sqlOrAST - the SQL statement or AST to parse
 * @param {ParserOptions} [options] - the parser options
 * @returns {MongoQuery}
 * @throws
 */
function makeMongoQuery(sqlOrAST, options = {}) {
    const parsedAST = parseSQLtoAST(sqlOrAST, options);
    if (!canQuery(parsedAST)) {
        throw new Error(
            'Query cannot cross multiple collections, have an aggregate function, contain functions in where clauses or have $$ROOT AS'
        );
    }
    /** @type {AstLike} */
    const ast = parsedAST.ast;
    const result = createResultObject();
    const parsedQuery = {
        limit: 100,
        collection: ast.from[0].table,
    };

    if (ast.columns && !isSelectAll(ast.columns) && ast.columns.length > 0) {
        // parsedQuery.projection = {};
        /** @type {Column[]} */
        const columns = ast.columns;
        columns.forEach((column) => {
            projectColumnParser(column, result);
        });
        parsedQuery.projection = result.parsedProject.$project;
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
        parsedQuery.query = makeQueryPart(ast.where, true, [], false);
    }

    if (ast.orderby && ast.orderby.length > 0) {
        parsedQuery.sort = ast.orderby.reduce((a, v) => {
            a[v.expr.column || v.expr.value] = v.type === 'DESC' ? -1 : 1;
            return a;
        }, {});
    }

    return parsedQuery;
}
/**
 * Creates an mongo aggregation pipeline given an ast
 *
 * @param {AstLike} ast - the ast to make an aggregate pipeline from
 * @param {ParserOptions} [options] - the options to generate the pipeline
 * @returns {any[]}
 */
function makeAggregatePipeline(ast, options = {}) {
    /** @type {PipelineFn[]} */
    let pipeline = [];

    const result = createResultObject();

    let wherePiece;
    if (ast.where) {
        if (whereContainsOtherTable(ast.where)) {
            const astQueryColumns = getWhereAstQueries(ast.where);
            const localTableName = ast.from[0].table;
            astQueryColumns.forEach((astQueryColumn) => {
                const foreignField = astQueryColumn.column;
                astQueryColumn.ast.forEach((actualAst) => {
                    const subPl = makeAggregatePipeline(actualAst, options);
                    pipeline = pipeline.concat(subPl);

                    pipeline.push({
                        $lookup: {
                            from: localTableName,
                            localField: actualAst.columns[0].expr.column,
                            foreignField,
                            as: localTableName,
                        },
                    });
                    pipeline.push({
                        $unwind: {
                            path: `$${localTableName}`,
                        },
                    });
                    pipeline.push({
                        $replaceRoot: {
                            newRoot: `$${localTableName}`,
                        },
                    });
                });
            });
            const standardQueries = getWhereStandardQueries(ast.where);
            standardQueries.forEach((sq) => {
                pipeline.push({
                    $match: makeQueryPart(sq, false, [], false),
                });
            });
        } else {
            wherePiece = {
                $match: makeQueryPart(ast.where, false, [], false),
            };
        }
    }

    if (ast.from[0].as && ast.from[0].table) {
        pipeline.push({$project: {[ast.from[0].as]: '$$ROOT'}});
    }

    const pipeLineJoin = makeJoinForPipeline(ast);
    if (pipeLineJoin.length > 0) {
        pipeline = pipeline.concat(pipeLineJoin);
        if (wherePiece) {
            pipeline.push(wherePiece);
            wherePiece = null;
        }
    }

    if (ast.groupby) {
        if (isSelectAll(ast.columns)) {
            throw new Error(`Select * not allowed with group by`);
        }

        /** @type {Column[]} */
        // @ts-ignore
        const columns = ast.columns;
        columns.forEach((column) => {
            groupByColumnParser(column, result);
        });

        pipeline.push(result.groupBy);
        const groupByProject = {};
        Object.keys(result.groupBy.$group._id).forEach((k) => {
            groupByProject[k] = `$_id.${k}`;
        });
        Object.keys(result.groupBy.$group).forEach((k) => {
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
            pipeline.push({$match: makeQueryPart(ast.having)});
        }
    } else if (ast.columns && !isSelectAll(ast.columns) && ast.columns.length > 0) {
        /** @type {Column[]} */
        // @ts-ignore
        const columns = ast.columns;
        columns.forEach((column) => {
            projectColumnParser(column, result);
        });
        if (result.count.length > 0) {
            result.count.forEach((countStep) => pipeline.push(countStep));
        }
        if (!$check.emptyObject(result.parsedProject.$project)) {
            if (result.exprToMerge && result.exprToMerge.length > 0) {
                pipeline.push({$replaceRoot: {newRoot: {$mergeObjects: result.exprToMerge.concat(result.parsedProject.$project)}}});
            } else {
                if (ast.distinct && ast.distinct.toLowerCase && ast.distinct.toLowerCase() === 'distinct') {
                    pipeline.push({$group: {_id: result.parsedProject.$project}});
                    const newProject = {};
                    for (const k in result.parsedProject.$project) {
                        // eslint-disable-next-line no-prototype-builtins
                        if (!result.parsedProject.$project.hasOwnProperty(k)) {
                            continue;
                        }
                        newProject[k] = `$_id.${k}`;
                    }
                    newProject['_id'] = 0;
                    pipeline.push({$project: newProject});
                } else {
                    pipeline.push(result.parsedProject);
                }
            }
        }
    }

    if (wherePiece) {
        pipeline.unshift(wherePiece);
    }

    // for if initial query is subquery
    if (!ast.from[0].table && ast.from[0].expr && ast.from[0].expr.ast) {
        if (!ast.from[0].as) {
            throw new Error(`AS not specified for initial sub query`);
        }
        pipeline = makeAggregatePipeline(ast.from[0].expr.ast, options)
            .concat([{$project: {[ast.from[0].as]: '$$ROOT'}}])
            .concat(pipeline);
    }

    if (result.replaceRoot) {
        pipeline.push(result.replaceRoot);
    }

    if (result.unwind && result.unwind.length > 0) {
        pipeline = pipeline.concat(result.unwind);
    }

    if (ast.orderby && ast.orderby.length > 0) {
        pipeline.push({
            $sort: ast.orderby.reduce((a, v) => {
                const asMapped = result.asMapping.find((c) => c.column === v.expr.column);
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
    if (ast._next && ast.union && ast.union === 'union all') {
        const otherPipeline = makeAggregatePipeline(ast._next, options);
        pipeline.push({$unionWith: {coll: ast._next.from[0].table, pipeline: otherPipeline}});
    }
    if (ast._next && ast.union && ast.union === 'union') {
        const otherPipeline = makeAggregatePipeline(ast._next, options);
        pipeline.push({$unionWith: {coll: ast._next.from[0].table, pipeline: otherPipeline}});
        const fieldsObj = ast.columns
            .map((c) => c.as || c.expr.column)
            .filter((c) => !!c)
            .reduce((obj, columnName) => {
                obj[columnName] = `$${columnName}`;
                return obj;
            }, {});
        pipeline.push({$group: {_id: fieldsObj}});
        pipeline.push({$replaceRoot: {newRoot: '$_id'}});
    }

    return pipeline;
}

/**
 * Makes a $cond from a case statement
 *
 * @param {object} expr - the expression object to turn into a case
 * @returns {any}
 */
function makeCaseCondition(expr) {
    if (expr.type !== 'case') {
        throw new Error(`Expression is not case`);
    }

    const elseExpr = expr.args.find((a) => a.type === 'else');
    const whens = expr.args.filter((a) => a.type === 'when');

    return {
        $switch: {
            branches: whens.map((w) => {
                return {
                    case: makeFilterCondition(w.cond),
                    then: makeFilterCondition(w.result),
                };
            }),
            default: makeProjectionExpressionPart(elseExpr.result),
        },
    };
}

/**
 * Makes an mongo expression tree from the cast statement
 *
 * @param {object} expr - the AST expression that is a cast
 * @returns {*}
 */
function makeCastPart(expr) {
    if (expr.type !== 'cast') {
        throw new Error(`Invalid type for cast:${expr.type}`);
    }
    const convertFunction = _allowableFunctions.functionMappings.find((f) => f.name === 'convert');
    if (!convertFunction) {
        throw new Error('No conversion function found');
    }
    const to = expr.target.dataType.toLowerCase();

    if (expr.expr.column) {
        return convertFunction.parse([`$${expr.expr.column}`, to]);
    }
    if (expr.expr.value) {
        return convertFunction.parse([expr.expr.value, to]);
    }
    return convertFunction.parse([makeProjectionExpressionPart(expr.expr), to]);
}

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
        return makeProjectionExpressionPart(queryPart);
    }

    if (queryPart.type === 'function') {
        return makeProjectionExpressionPart(queryPart);
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

/**
 * Creates the pipeline components for a join
 *
 * @param {object} ast - the ast that contains the join
 * @returns {*[]}
 */
function makeJoinForPipeline(ast) {
    const pipeline = [];

    const makeJoinPart = (join, previousJoin) => {
        let toTable = join.table || '';
        const toAs = join.as;
        let fromTable = previousJoin.table || '';
        const fromAs = previousJoin.as;

        let joinHint = null;
        if (toTable.toLowerCase().endsWith('|first')) {
            joinHint = 'first';
            toTable = toTable.substring(0, toTable.length - 6);
        } else if (toTable.toLowerCase().endsWith('|last')) {
            joinHint = 'last';
            toTable = toTable.substring(0, toTable.length - 5);
        } else if (toTable.toLowerCase().endsWith('|unwind')) {
            joinHint = 'unwind';
            toTable = toTable.substring(0, toTable.length - 7);
        }

        if (join.table && join.on && join.on.type === 'binary_expr' && join.on.operator === '=') {
            // todo rework this to handle correctly
            const localTable = fromAs || fromTable;
            const foreignTable = toTable;

            const localField =
                (fromTable && join.on.left.table === fromTable) || (fromAs && join.on.left.table === fromAs)
                    ? `${localTable ? localTable + '.' : ''}${join.on.left.column}`
                    : // eslint-disable-next-line sonarjs/no-all-duplicated-branches
                    join.on.right.table === fromTable || join.on.right.table === fromAs
                    ? `${localTable ? localTable + '.' : ''}${join.on.right.column}`
                    : `${localTable ? localTable + '.' : ''}${join.on.right.column}`;

            const foreignField =
                join.on.left.table === toTable || join.on.left.table === toAs
                    ? join.on.left.column
                    : join.on.right.table === toTable || join.on.right.table === toAs
                    ? join.on.right.column
                    : join.on.left.column;

            pipeline.push({
                $lookup: {
                    from: foreignTable,
                    as: toAs || toTable,
                    localField: localField,
                    foreignField: foreignField,
                },
            });
            if (joinHint) {
                if (joinHint === 'first') {
                    pipeline.push({$set: {[toAs || toTable]: {$first: `$${toAs || toTable}`}}});
                } else if (joinHint === 'last') {
                    pipeline.push({$set: {[toAs || toTable]: {$last: `$${toAs || toTable}`}}});
                } else if (joinHint === 'unwind') {
                    pipeline.push({$unwind: `$${toAs || toTable}`});
                }
            }
            if (join.join === 'INNER JOIN') {
                if (joinHint) {
                    pipeline.push({$match: {[toAs || toTable]: {$ne: null}}});
                } else {
                    pipeline.push({$match: {$expr: {$gt: [{$size: `$${toAs || toTable}`}, 0]}}});
                }
            } else if (join.join === 'LEFT JOIN') {
                // dont need anything
            } else {
                throw new Error(`Join not supported:${join.join}`);
            }
        } else {
            const joinQuery = makeFilterCondition(join.on, false, true);
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
                lookupPipeline = makeAggregatePipeline(join.expr.ast);
                if (join.expr.ast.from[0] && join.expr.ast.from[0].table) fromTable = join.expr.ast.from[0].table;
                else throw new Error('Missing table for join sub query');
            }
            lookupPipeline.push({$match: {$expr: joinQuery}});

            pipeline.push({
                $lookup: {
                    from: toTable,
                    as: toAs,
                    let: inputVars,
                    pipeline: lookupPipeline,
                },
            });
            if (joinHint) {
                if (joinHint === 'first') {
                    pipeline.push({$set: {[toAs || toTable]: {$first: `$${toAs || toTable}`}}});
                } else if (joinHint === 'last') {
                    pipeline.push({$set: {[toAs || toTable]: {$last: `$${toAs || toTable}`}}});
                } else if (joinHint === 'unwind') {
                    pipeline.push({$unwind: `$${toAs || toTable}`});
                }
            }
            if (join.join === 'INNER JOIN') {
                if (joinHint) {
                    pipeline.push({$match: {[toAs || toTable]: {$ne: null}}});
                } else {
                    pipeline.push({$match: {$expr: {$gt: [{$size: `$${toAs || toTable}`}, 0]}}});
                }
            } else if (join.join === 'LEFT JOIN') {
                // dont need anything
            } else {
                throw new Error(`Join not supported:${join.join}`);
            }
        }
    };

    for (let i = 1; i < ast.from.length; i++) {
        makeJoinPart(ast.from[i], ast.from[i - 1]);
    }

    return pipeline;
}

/**
 * Creates an object from a select without a from cause
 *
 * @param {AstLike} ast - the ast for the select statement
 * @returns {any}
 */
function makeObjectFromSelect(ast) {
    const toParse = {};
    /** @type {Column[]} */
    const columns = ast.columns;
    columns.forEach((column) => {
        if (column.expr.type === 'column_ref') {
            toParse[`${column.as || column.expr.column}`] = `$${column.expr.table ? column.expr.table + '.' : ''}${column.expr.column}`;
        } else if (column.expr.type === 'function' && column.as) {
            const parsedExpr = makeProjectionExpressionPart(column.expr);
            toParse[`${column.as}`] = parsedExpr;
        } else if (column.expr.type === 'binary_expr' && column.as) {
            toParse[`${column.as}`] = getParsedValueFromBinaryExpression(column.expr);
        } else if (column.expr.type === 'case' && column.as) {
            toParse[`${column.as}`] = makeCaseCondition(column.expr);
        } else if (column.expr.type === 'cast' && column.as) {
            toParse[`${column.as}`] = makeCastPart(column.expr);
        } else if (column.expr.type === 'select' && column.as && column.expr.from) {
            toParse[`${column.as}`] = makeArraySubSelectPart(column.expr);
        } else if (column.expr.type === 'select' && column.as && !column.expr.from) {
            toParse[`${column.as}`] = makeObjectFromSelect(column.expr);
        } else if (column.expr.type && column.as) {
            toParse[`${column.as}`] = {$literal: column.expr.value};
        } else if (!column.as) {
            throw new Error(`Require as for calculation:${column.expr.name}`);
        } else {
            throw new Error(`Not Supported:${column.expr.type}`);
        }
    });

    return {
        $arrayToObject: {$concatArrays: [{$objectToArray: toParse}]},
    };
}

/**
 * Parses a AST QueryPart into a Mongo Query/Match
 *
 * @param {object} queryPart - The AST query part
 * @param {boolean} [ignorePrefix] - Ignore the table prefix
 * @param {Array}  [allowedTypes] - Expression types to allow
 * @param {boolean} [includeThis] - include $$this in expresions
 * @returns {any} - the mongo query/match
 */
function makeQueryPart(queryPart, ignorePrefix, allowedTypes = [], includeThis = false) {
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
                (queryPartToUse.table && !ignorePrefix ? `${queryPartToUse.table}.${queryPartToUse.column}` : queryPartToUse.column)
            );
        } else {
            return queryPartToUse.value;
        }
    };

    const makeOperator = (op) => {
        const left = makeQueryPart(queryPart.left, ignorePrefix, allowedTypes, includeThis);
        const right = makeQueryPart(queryPart.right, ignorePrefix, allowedTypes, includeThis);
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
                    makeQueryPart(queryPart.left, ignorePrefix, allowedTypes, includeThis),
                    makeQueryPart(queryPart.right, ignorePrefix, allowedTypes, includeThis),
                ],
            };
        }
        if (queryPart.operator === 'OR') {
            return {
                $or: [
                    makeQueryPart(queryPart.left, ignorePrefix, allowedTypes, includeThis),
                    makeQueryPart(queryPart.right, ignorePrefix, allowedTypes, includeThis),
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
            return {[getColumnNameOrVal(queryPart.left)]: {$regex: regex, $options: 'i'}};
            // }
        }
        if (queryPart.operator === 'IS NOT') {
            return makeOperator('$ne');
        }
        throw new Error(`Unsupported operator:${queryPart.operator}`);
    }

    if (queryPart.type === 'function' || queryPart.type === 'select') return makeProjectionExpressionPart(queryPart, 0, true);
    if (queryPart.type === 'expr_list') {
        return queryPart.value.map((v) => makeQueryPart(v));
    }

    return getColumnNameOrVal(queryPart);
}

/**
 * Get the value from a binary expression
 *
 * @param {object} expressionPart - the expression to turn into a value
 * @returns {string|undefined|*}
 */
function getParsedValueFromBinaryExpression(expressionPart) {
    if (expressionPart.type === 'binary_expr') {
        return makeBinaryExpressionPart(expressionPart);
    }
    if (expressionPart.type === 'column_ref') {
        return `$${expressionPart.column}`;
    }
    if (['single_quote_string', 'string'].includes(expressionPart.type)) {
        return expressionPart.value;
    }
    if (['number'].includes(expressionPart.type)) {
        return expressionPart.value;
    }
    if (expressionPart.type === 'function') {
        return makeProjectionExpressionPart(expressionPart);
    }

    throw new Error(`Unable to make binary expression part:${expressionPart.type}`);
}

/**
 * Translates a binary expression into a mongo usable part
 *
 * @param {expr} expr - the ast expression
 * @returns {*}
 */
function makeBinaryExpressionPart(expr) {
    let operator;
    if (expr.expr) {
        operator = expr.expr.operator;
    } else {
        operator = expr.operator;
    }

    const exprFunction = _allowableFunctions.functionMappings.find((f) => f.name === operator.toLowerCase());
    let exprResult;
    if (!exprFunction) throw new Error(`Expression not found:${operator}`);

    if (expr.expr && expr.expr.left && expr.expr.right) {
        const leftPartValue = getParsedValueFromBinaryExpression(expr.expr.left);
        const rightPartValue = getParsedValueFromBinaryExpression(expr.expr.right);

        exprResult = exprFunction.parse(leftPartValue, rightPartValue);
    } else if (expr.left && expr.right) {
        const leftPartValue = getParsedValueFromBinaryExpression(expr.left);
        const rightPartValue = getParsedValueFromBinaryExpression(expr.right);

        exprResult = exprFunction.parse(leftPartValue, rightPartValue);
    }

    return exprResult;
}

/**
 *
 * @param expr
 * @param depth
 */
function makeArg(expr, depth) {
    // todo check all these types
    if (expr.type === 'function') {
        return makeProjectionExpressionPart(expr);
    } else if (expr.type === 'column_ref') {
        return `$${expr.table ? expr.table + '.' : ''}${expr.column}`;
    } else if (expr.type === 'binary_expr') {
        return getParsedValueFromBinaryExpression(expr);
    } else if (expr.type === 'select' && expr.from) {
        return makeArraySubSelectPart(expr, depth);
    } else if (expr.type === 'select' && !expr.from) {
        return makeObjectFromSelect(expr);
    } else if (expr.type === 'unary_expr') {
        if (expr.operator === '-') {
            return {$multiply: [-1, makeProjectionExpressionPart(expr.expr)]};
        } else {
            throw new Error(`Unable to parse unary expression:${expr.operator}`);
        }
    } else if (expr.type === 'case') {
        return makeCaseCondition(expr);
    } else if (expr.value !== undefined) {
        return {$literal: expr.value};
    } else {
        throw new Error(`Unable to parse expression type:${expr.type}`);
    }
}
/**
 * Makes a projection expression sub part.
 *
 * @param {import('node-sql-parser').AggrFunc} expr - the expression to make a projection from
 * @param {number} [depth] - the current recursive depth
 * @param {boolean} [forceLiteralParse] - Forces parsing of literal expressions
 * @returns {undefined|*}
 */
function makeProjectionExpressionPart(expr, depth = 0, forceLiteralParse = false) {
    if (!expr.name && !expr.operator) {
        return makeArg(expr, depth);
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
            args.map((a) => makeArg(a, depth)),
            depth,
            forceLiteralParse
        );
    } else if (expr.left && expr.right) {
        return getParsedValueFromBinaryExpression(expr);
    } else if (expr.args && expr.args.expr) {
        return fn.parse(makeArg(expr.args.expr, depth), depth, forceLiteralParse);
    } else {
        return makeArg(expr, depth);
        // throw new Error('Unable to parse expression');
    }
}

/**
 * Makes an array expression from a sub select
 *
 * @param {AstLike} ast - the ast to create a sub select from
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
        /** @type {Column[]} */
        const columns = ast.columns;
        columns.forEach((v) => {
            if (v.expr.type === 'column_ref') {
                mapIn[v.as || v.expr.column] = `$$this.${v.expr.table ? v.expr.table + '.' : ''}${v.expr.column}`;
            } else if (v.expr.type === 'function' && v.as) {
                mapIn[v.as] = makeProjectionExpressionPart(v.expr, depth + 1);
            } else if (v.expr.type === 'aggr_func' && v.as) {
                mapIn[v.as] = makeProjectionExpressionPart(v.expr, depth + 1);
            } else if (v.expr.type === 'binary_expr' && v.as) {
                mapIn[v.as] = getParsedValueFromBinaryExpression(v.expr);
            } else if (v.expr.type === 'case' && v.as) {
                mapIn[v.as] = makeCaseCondition(v.expr);
            } else if (v.expr.type === 'select' && v.as && v.expr.from) {
                mapIn[v.as] = makeArraySubSelectPart(v.expr, depth + 1);
            } else if (v.expr.type === 'select' && v.as && !v.expr.from) {
                mapIn[v.as] = makeObjectFromSelect(v.expr);
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
                cond: {$and: [makeFilterCondition(ast.where, true)]},
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

/**
 * @param {Column} column The column to parse
 * @param {ColumnParseResult} result the result object
 * @returns {void}
 */
function groupByColumnParser(column, result) {
    if (column.expr.type === 'column_ref') {
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: `$${column.expr.column}`}};
            return;
        }
        if (column.as) {
            result.asMapping.push({column: column.expr.column, as: column.as});
            result.groupBy.$group._id[column.as] = `$${column.expr.table ? column.expr.table + '.' : ''}${column.expr.column}`;
            return;
        }
        result.groupBy.$group._id[column.expr.column] = `$${column.expr.table ? column.expr.table + '.' : ''}${column.expr.column}`;
        return;
    }
    if (column.expr.type === 'function' && column.as && column.expr.name && column.expr.name.toLowerCase() === 'unwind') {
        throw new Error('Unwind not allowed with group by');
    }
    if (column.expr.type === 'function' && column.as) {
        const parsedExpr = makeProjectionExpressionPart(column.expr);
        if (column.expr.name.toLowerCase() === 'count') {
            result.groupBy.$group[column.as] = parsedExpr;
            return; // count values can't go in the groupBy
        }
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
            return;
        }
        result.groupBy.$group._id[column.as] = parsedExpr;
        return;
    }
    if (column.expr.type === 'aggr_func' && column.as) {
        const aggregateFunction = _allowableFunctions.functionMappings.find(
            (f) => f.name && f.name.toLowerCase() === column.expr.name.toLowerCase() && (!f.type || f.type === 'aggr_func')
        );
        if (!aggregateFunction) {
            throw new Error(`Function not found:${column.expr.name}`);
        }
        result.groupBy.$group[column.as] = makeProjectionExpressionPart(column.expr);
        return;
    }
    if (column.expr.type === 'binary_expr' && column.as) {
        result.groupBy.$group._id[column.as] = getParsedValueFromBinaryExpression(column.expr);
        return;
    }
    if (column.expr.type === 'case' && column.as) {
        result.groupBy.$group._id[column.as] = makeCaseCondition(column.expr);
        return;
    }
    if (column.expr.type === 'cast' && column.as) {
        result.groupBy.$group._id[column.as] = makeCastPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && column.expr.from) {
        result.groupBy.$group._id[column.as] = makeArraySubSelectPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && !column.expr.from) {
        result.groupBy.$group._id[column.as] = makeObjectFromSelect(column.expr);
        return;
    }
    if (column.expr.type && column.as) {
        result.groupBy.$group._id[column.as] = {$literal: column.expr.value};
        return;
    }
    if (!column.as) {
        throw new Error(`Require as for calculation:${column.expr.name}`);
    }
    throw new Error(`Not Supported:${column.expr.type}`);
}

/**
 * @param {Column} column The column to parse
 * @param {ColumnParseResult} result the result object
 * @returns {void}
 */
function projectColumnParser(column, result) {
    if (column.expr.type === 'column_ref') {
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: `$${column.expr.column}`}};
            return;
        }
        if (column.expr.column === '*' && column.expr.table) {
            result.parsedProject.$project[column.as || column.expr.table] = `$${column.expr.table}`;
            return;
        }
        if (column.expr.column === '*') {
            result.exprToMerge.push('$$ROOT');
            return;
        }
        result.parsedProject.$project[column.as || column.expr.column] = `$${column.expr.table ? column.expr.table + '.' : ''}${
            column.expr.column
        }`;
        return;
    }
    if (column.expr.type === 'function' && column.as && column.expr.name && column.expr.name.toLowerCase() === 'unwind') {
        if (
            column.expr.args &&
            column.expr.args.value &&
            $check.array(column.expr.args.value) &&
            column.expr.args.value[0] &&
            column.expr.args.value[0].column &&
            column.expr.args.value[0].column !== column.as
        ) {
            result.unwind.push({
                $unset: column.expr.args.value[0].column,
            });
        }
        if (column.as && column.as === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: `$${column.expr.args.value[0].value}`}};
            return;
        }
        result.parsedProject.$project[column.as] = makeProjectionExpressionPart(column.expr.args.value[0]);
        result.unwind.push({
            $unwind: `$${column.as}`,
        });
        return;
    }
    if (column.expr.type === 'function' && column.expr.name && column.expr.name.toLowerCase() === 'unset') {
        if (!column.expr.args || !column.expr.args.value) {
            throw new Error('Unset requires the field names to be passed in');
        }
        const fieldsToUnset = column.expr.args.value
            .map((v) => v.column)
            .reduce((obj, col) => {
                return {...obj, [col]: 0};
            }, {});
        result.parsedProject.$project = {...result.parsedProject.$project, ...fieldsToUnset};
        return;
    }
    if (column.expr.type === 'function' && column.as) {
        if (column.expr.name.toLowerCase() === 'count') {
            result.count.push({$count: column.as});
            return;
        }
        const parsedExpr = makeProjectionExpressionPart(column.expr);
        if (column.as && column.as.toUpperCase() === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: parsedExpr}};
            return;
        }
        result.parsedProject.$project[column.as] = parsedExpr;
        return;
    }
    if (column.expr.type === 'aggr_func') {
        result.parsedProject.$project[column.as] = makeProjectionExpressionPart(column.expr);
        return;
    }
    if (column.expr.type === 'binary_expr' && column.as) {
        result.parsedProject.$project[column.as] = getParsedValueFromBinaryExpression(column.expr);
        return;
    }
    if (column.expr.type === 'case' && column.as) {
        result.parsedProject.$project[column.as] = makeCaseCondition(column.expr);
        return;
    }
    if (column.expr.type === 'cast' && column.as) {
        result.parsedProject.$project[column.as] = makeCastPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && column.expr.from) {
        result.parsedProject.$project[column.as] = makeArraySubSelectPart(column.expr);
        return;
    }
    if (column.expr.type === 'select' && column.as && !column.expr.from) {
        result.parsedProject.$project[column.as] = makeObjectFromSelect(column.expr);
        return;
    }
    if (column.expr.type && column.as) {
        if (column.as === '$$ROOT') {
            result.replaceRoot = {$replaceRoot: {newRoot: `$${column.expr.value}`}};
            return;
        }
        result.parsedProject.$project[column.as] = {$literal: column.expr.value};
        return;
    }
    if (!column.as) {
        throw new Error(`Require as for calculation:${column.expr.name}`);
    }
    throw new Error(`Not Supported:${column.expr.type}`);
}

module.exports = {
    makeMongoAggregate,
    makeMongoQuery,
};
