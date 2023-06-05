const groupByColumnParserModule = require('./groupByColumnParser');
const makeJoinForPipelineModule = require('./makeJoinForPipeline');
const makeQueryPartModule = require('./makeQueryPart');
const projectColumnParserModule = require('./projectColumnParser');
const $check = require('check-types');
const {isSelectAll} = require('../isSelectAll');
const {whereContainsOtherTable} = require('../canQuery');
const {createResultObject} = require('./createResultObject');
const {forceGroupBy} = require('./forceGroupBy');

const {
    getWhereAstQueries,
    getWhereStandardQueries,
} = require('./filter-queries');

const $copy = require('clone-deep');

exports.makeAggregatePipeline = makeAggregatePipeline;

/**
 *
 * Checks whether the query needs to force a group by
 *
 * @param {import('../types').AST} ast - the ast to check if a group by needs to be forced
 * @returns {boolean} - whether a group by needs to be forced
 * @private
 */

// function getAggrFunctions(columns) {
//     const potentialFuncs = [];
//     const aggrFunctions = [];
//     $json.walk(columns, (val, path) => {
//         const pathParts = path.split('/').slice(1);
//         if (val === 'aggr_func') {
//             potentialFuncs.push(
//                 pathParts.slice(0, pathParts.length - 1).join('.')
//             );
//         }
//     });
//
//     for (const potentialFunc of potentialFuncs) {
//         aggrFunctions.push({
//             path: potentialFunc,
//             expr: $json.get(columns, potentialFunc),
//         });
//     }
//
//     return aggrFunctions;
// }

/**
 * Checks whether an _id column is specified
 *
 * @param {Array} columns - the columns to check
 * @returns {boolean} - whether an _id column is specified
 * @private
 */
function _hasIdCol(columns) {
    if (!columns || columns.length === 0) {
        return false;
    }
    for (const col of columns) {
        if (
            col.expr &&
            col.expr.type === 'column_ref' &&
            col.expr.column === '_id'
        ) {
            return true;
        }
    }

    return false;
}

/**
 * Creates an mongo aggregation pipeline given an ast
 *
 * @param {import('../types').AST} ast - the ast to make an aggregate pipeline from
 * @param {import('../types').ParserOptions} [options] - the options to generate the pipeline
 * @returns {import('../types').PipelineFn[]}
 */
function makeAggregatePipeline(ast, options = {}) {
    if (
        !ast.from &&
        !ast.where &&
        !ast.groupby &&
        !ast.columns &&
        !ast.orderby &&
        !ast.limit &&
        !ast.union
    ) {
        if (ast.ast) {
            return makeAggregatePipeline(ast.ast, options);
        }
        throw new Error(`AST is missing properties required for processing`);
    }
    /** @type {import('../types').PipelineFn[]} */
    let pipeline = [];

    const result = createResultObject();

    let wherePiece;
    if (ast.where) {
        if (whereContainsOtherTable(ast.where)) {
            const astQueryColumns = getWhereAstQueries(ast.where);
            const localTableName = ast.from[0].table;
            const operator = ast.where.operator;
            astQueryColumns.forEach((astQueryColumn) => {
                const foreignField = astQueryColumn.column;
                astQueryColumn.ast.forEach((actualAst) => {
                    const ast = actualAst.columns
                        ? actualAst
                        : actualAst.ast
                        ? actualAst.ast
                        : actualAst;

                    const subPl = makeAggregatePipeline(ast, options);
                    pipeline = pipeline.concat(subPl);
                    if (operator === 'NOT IN') {
                        const projections = subPl.filter((p) => !!p.$project);
                        const lastProjection =
                            projections[projections.length - 1];
                        if (!lastProjection) {
                            throw new Error(
                                `Unable to do "NOT IN" query with a "SELECT *" or no column specified`
                            );
                        }
                        const columns = Object.entries(
                            lastProjection.$project
                        ).filter(([key, value]) => {
                            return key !== '_id' && value !== 0;
                        });
                        if (columns.length !== 1) {
                            throw new Error(
                                `Unable to do "NOT IN" query when more than 1 column is specified in the subquery`
                            );
                        }
                        const [inField] = columns[0];
                        const inFieldArray = `all${inField}s`;
                        pipeline.push({
                            $group: {
                                _id: null,
                                [inFieldArray]: {$addToSet: `$${inField}`},
                            },
                        });
                        pipeline.push({
                            $lookup: {
                                from: localTableName,
                                let: {
                                    [inFieldArray]: `$${inFieldArray}`,
                                },
                                pipeline: [
                                    {
                                        $match: {
                                            $expr: {
                                                $not: {
                                                    $in: [
                                                        `$${foreignField}`,
                                                        `$$${inFieldArray}`,
                                                    ],
                                                },
                                            },
                                        },
                                    },
                                ],
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
                        return;
                    }

                    pipeline.push({
                        $lookup: {
                            from: localTableName,
                            localField: ast.columns[0].expr.column,
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
                    false,
                    ast.from && ast.from[0] ? ast.from[0].as : null
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
        // todo check where this gets inserted
        if (wherePiece) {
            pipeline.push(wherePiece);
            wherePiece = null;
        }
    } else {
        if (wherePiece) {
            pipeline.push(wherePiece);
            wherePiece = null;
        }
    }

    const checkForceGroupBy = forceGroupBy(ast);

    if (ast.groupby || checkForceGroupBy) {
        if (isSelectAll(ast.columns)) {
            throw new Error(`Select * not allowed with group by`);
        }

        /** @type {import('../types').Column[]} */
        // @ts-ignore
        const columns = ast.columns;

        columns.forEach((column) => {
            groupByColumnParserModule.groupByColumnParser(column, result);
        });

        // count distinct
        let groupByForFields = {};
        pipeline.push(result.groupBy);
        let secondGroup = null;
        if (result.countDistinct && result.groupBy && result.groupBy.$group) {
            const currentGroup = $copy(result.groupBy.$group);
            if (
                !currentGroup ||
                !currentGroup._id ||
                !$check.object(currentGroup._id)
            ) {
                throw new Error('Group by id missing for count distinct');
            }
            delete currentGroup._id._countDistinctTemp;
            secondGroup = {
                _id: {},
            };

            Object.keys(currentGroup).forEach((k) => {
                if (k === '_id') {
                    Object.keys(currentGroup[k]).forEach((i) => {
                        if (i !== '_countDistinctTemp') {
                            secondGroup._id[i] = `$_id.${i}`;
                        }
                    });
                } else {
                    const aggrName = Object.keys(currentGroup[k])[0];
                    let aggrValue = `$${k}`;
                    if (aggrName === '$sum' && k === result.countDistinct) {
                        aggrValue = 1;
                    }
                    secondGroup[k] = {
                        [aggrName]: aggrValue,
                    };
                }
            });

            pipeline.push({$group: secondGroup});
        }

        groupByForFields = secondGroup ? secondGroup : result.groupBy.$group;

        const groupByProject = result.groupByProject || {};

        if ($check.object(groupByForFields._id)) {
            Object.keys(groupByForFields._id).forEach((k) => {
                groupByProject[k] = `$_id.${k}`;
            });
        }

        Object.keys(groupByForFields).forEach((k) => {
            if (k === '_id') {
                groupByProject[k] = 0;
            } else if (!k.startsWith('_tempAggregateCol_')) {
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
            projectColumnParserModule.projectColumnParser(
                column,
                result,
                ast.from && ast.from[0] ? ast.from[0].as : null
            );
        });
        if (result.count.length > 0) {
            result.count.forEach((countStep) => pipeline.push(countStep));
        }
        if (result.unset) {
            pipeline.push(result.unset);
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
                    (ast.distinct &&
                        ast.distinct.toLowerCase &&
                        ast.distinct.toLowerCase() === 'distinct') ||
                    (ast.distinct &&
                        ast.distinct.type &&
                        ast.distinct.type.toLowerCase &&
                        ast.distinct.type.toLowerCase() === 'distinct')
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

    // if (wherePiece) {
    //     pipeline.unshift(wherePiece);
    // }

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

    if (
        options.unsetId &&
        !isSelectAll(ast.columns) &&
        !_hasIdCol(ast.columns)
    ) {
        pipeline.push({$unset: '_id'});
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
        const unionCollection =
            ast._next.from[0].table ||
            (ast._next.from[0].expr &&
            ast._next.from[0].expr.ast &&
            ast._next.from[0].expr.ast.from &&
            ast._next.from[0].expr.ast.from[0] &&
            ast._next.from[0].expr.ast.from[0].table
                ? ast._next.from[0].expr.ast.from[0].table
                : null) ||
            null;
        if (!unionCollection) {
            throw new Error('No collection for union with');
        }
        pipeline.push({
            $unionWith: {
                coll: unionCollection,
                pipeline: otherPipeline,
            },
        });
    }
    if (ast._next && ast.union && ast.union === 'union') {
        const otherPipeline = makeAggregatePipeline(ast._next, options);
        const unionCollection =
            ast._next.from[0].table ||
            (ast._next.from[0].expr &&
            ast._next.from[0].expr.ast &&
            ast._next.from[0].expr.ast.from &&
            ast._next.from[0].expr.ast.from[0] &&
            ast._next.from[0].expr.ast.from[0].table
                ? ast._next.from[0].expr.ast.from[0].table
                : null) ||
            null;
        if (!unionCollection) {
            throw new Error('No collection for union with');
        }
        pipeline.push({
            $unionWith: {
                coll: unionCollection,
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
