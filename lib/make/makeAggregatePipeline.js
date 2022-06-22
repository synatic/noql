const groupByColumnParserModule = require('./groupByColumnParser');
const makeJoinForPipelineModule = require('./makeJoinForPipeline');
const makeQueryPartModule = require('./makeQueryPart');
const projectColumnParserModule = require('./projectColumnParser');
const $check = require('check-types');
const {isSelectAll} = require('../isSelectAll');
const {whereContainsOtherTable} = require('../canQuery');
const {createResultObject} = require('./createResultObject');
const {
    getWhereAstQueries,
    getWhereStandardQueries,
} = require('./filter-queries');

exports.makeAggregatePipeline = makeAggregatePipeline;

/**
 * Creates an mongo aggregation pipeline given an ast
 *
 * @param {import('../types').AST} ast - the ast to make an aggregate pipeline from
 * @param {import('../types').ParserOptions} [options] - the options to generate the pipeline
 * @returns {import('../types').PipelineFn[]}
 */
function makeAggregatePipeline(ast, options = {}) {
    /** @type {import('../types').PipelineFn[]} */
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
                    $match: makeQueryPartModule.makeQueryPart(
                        sq,
                        false,
                        [],
                        false
                    ),
                });
            });
        } else {
            wherePiece = {
                $match: makeQueryPartModule.makeQueryPart(
                    ast.where,
                    false,
                    [],
                    false
                ),
            };
        }
    }

    if (ast.from[0].as && ast.from[0].table) {
        pipeline.push({$project: {[ast.from[0].as]: '$$ROOT'}});
    }

    const pipeLineJoin = makeJoinForPipelineModule.makeJoinForPipeline(ast);
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

        /** @type {import('../types').Column[]} */
        // @ts-ignore
        const columns = ast.columns;
        columns.forEach((column) => {
            groupByColumnParserModule.groupByColumnParser(column, result);
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
            pipeline.push({
                $match: makeQueryPartModule.makeQueryPart(ast.having),
            });
        }
    } else if (
        ast.columns &&
        !isSelectAll(ast.columns) &&
        ast.columns.length > 0
    ) {
        /** @type {import('../types').Column[]} */
        // @ts-ignore
        const columns = ast.columns;
        columns.forEach((column) => {
            projectColumnParserModule.projectColumnParser(column, result);
        });
        if (result.count.length > 0) {
            result.count.forEach((countStep) => pipeline.push(countStep));
        }
        if (!$check.emptyObject(result.parsedProject.$project)) {
            if (result.exprToMerge && result.exprToMerge.length > 0) {
                pipeline.push({
                    $replaceRoot: {
                        newRoot: {
                            $mergeObjects: result.exprToMerge.concat(
                                result.parsedProject.$project
                            ),
                        },
                    },
                });
            } else {
                if (
                    ast.distinct &&
                    ast.distinct.toLowerCase &&
                    ast.distinct.toLowerCase() === 'distinct'
                ) {
                    pipeline.push({
                        $group: {_id: result.parsedProject.$project},
                    });
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
                const asMapped = result.asMapping.find(
                    (c) => c.column === v.expr.column
                );
                a[asMapped ? asMapped.as : v.expr.column || v.expr.value] =
                    v.type === 'DESC' ? -1 : 1;

                return a;
            }, {}),
        });
    }

    if (ast.limit) {
        if (
            ast.limit.seperator &&
            ast.limit.seperator === 'offset' &&
            ast.limit.value[1] &&
            ast.limit.value[1].value
        ) {
            pipeline.push({$limit: ast.limit.value[0].value});
            pipeline.push({$skip: ast.limit.value[1].value});
        } else if (
            ast.limit.value &&
            ast.limit.value[0] &&
            ast.limit.value[0].value
        ) {
            pipeline.push({$limit: ast.limit.value[0].value});
        }
    }
    if (ast._next && ast.union && ast.union === 'union all') {
        const otherPipeline = makeAggregatePipeline(ast._next, options);
        pipeline.push({
            $unionWith: {
                coll: ast._next.from[0].table,
                pipeline: otherPipeline,
            },
        });
    }
    if (ast._next && ast.union && ast.union === 'union') {
        const otherPipeline = makeAggregatePipeline(ast._next, options);
        pipeline.push({
            $unionWith: {
                coll: ast._next.from[0].table,
                pipeline: otherPipeline,
            },
        });
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
