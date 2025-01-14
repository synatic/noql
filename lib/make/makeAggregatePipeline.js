const groupByColumnParserModule = require('./groupByColumnParser');
const makeJoinForPipelineModule = require('./makeJoinForPipeline');
const makeQueryPartModule = require('./makeQueryPart');
const projectColumnParserModule = require('./projectColumnParser');
const $check = require('check-types');
const {isSelectAll} = require('../isSelectAll');
const {whereContainsOtherTable} = require('../canQuery');
const {createResultObject} = require('./createResultObject');
const {forceGroupBy} = require('./forceGroupBy');
const {formatLargeNumber} = require('../formatLargeNumber');

const $copy = require('clone-deep');
const {optimizeJoinAndWhere} = require('./optimize-join-and-where');
const {
    applyPivot,
    applyUnpivot,
    applyMultipleUnpivots,
} = require('./apply-pivot');
const $json = require('@synatic/json-magic');
const projectIsSimple = require('../projectIsSimple');
const lodash = require('lodash');
const projectIsRoot = require('../projectIsRoot'); 

exports.makeAggregatePipeline = makeAggregatePipeline;
exports.stripJoinHints = stripJoinHints;
/**
 *
 *Checks whether the query needs to force a group by
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
 * @param {Array} columns - the columns to check
 * @param {import('../types').NoqlContext} _context - The Noql context to use when generating the output
 * @returns {boolean} - whether an _id column is specified
 * @private
 */
function _hasIdCol(columns, _context) {
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

function _getTableNameFromSubQuery(ast) {
    if (ast.from[0].table) {
        return ast.from[0].table;
    }
    if (ast.from[0].expr && ast.from[0].expr.ast) {
        return _getTableNameFromSubQuery(ast.from[0].expr.ast);
    }
    return null;
}

/**
 * Creates a mongo aggregation pipeline given an ast
 * @param {import('../types').AST} ast - the ast to make an aggregate pipeline from
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @returns {import('../types').PipelineFn[]}
 */
function makeAggregatePipeline(ast, context = {}) {
    if (
        !ast.from &&
        !ast.where &&
        !ast.groupby &&
        !ast.columns &&
        !ast.orderby &&
        !ast.limit &&
        !ast.union &&
        !ast.set_op
    ) {
        if (ast.ast) {
            return makeAggregatePipeline(ast.ast, context);
        }
        throw new Error(`AST is missing properties required for processing`);
    }
    /** @type {import('../types').PipelineFn[]} */
    let pipeline = [];

    const result = createResultObject();
    // used for sort reworking
    let rootProjection = false;

    let wherePiece;
    if (ast.where) {
        // this is for subqueries like in (select id from table)
        if (whereContainsOtherTable(ast.where)) {
            // the parser orders this incorrectly so need to re-order for proper collection sequence
            const mainTableName = _getTableNameFromSubQuery(ast);
            context._reorderedTables = context._reorderedTables || [];
            context._reorderedTables.push(mainTableName);

            // need to break the where down into pieces and then handle the subqueries
            const preprocessedWhere = makeQueryPartModule.makeQueryPart(
                ast.where,
                context,
                false,
                [],
                false,
                '',
                true
            );

            // using the renamer, but just  finding the position to insert it
            // get the sub query info
            const subQueryQueries = [];
            $json.renameKey(preprocessedWhere, (key, path) => {
                if (
                    key === '$$$SubQuery$$$' &&
                    path.endsWith('$$$SubQuery$$$')
                ) {
                    const expr = $json.get(preprocessedWhere, path);
                    const operator = expr.operator;
                    let column = null;
                    let ast = null;
                    if (
                        expr &&
                        expr.left &&
                        expr.left.type === 'column_ref' &&
                        expr.right &&
                        expr.right.type === 'expr_list'
                    ) {
                        column = expr.left.column;
                        ast =
                            (expr.right.value &&
                                expr.right.value[0] &&
                                expr.right.value[0].ast) ||
                            null;
                    } else if (
                        expr &&
                        expr.right &&
                        expr.right.type === 'column_ref' &&
                        expr.left &&
                        expr.left.type === 'expr_list'
                    ) {
                        column = expr.right.column;
                        ast =
                            (expr.left.value &&
                                expr.left.value[0] &&
                                expr.left.value[0].ast) ||
                            null;
                    }
                    subQueryQueries.push({
                        path: path.substring(0, path.indexOf('$$$SubQuery$$$')),
                        ast: ast,
                        column: column,
                        operator: operator,
                    });
                }
            });

            // process all subquery based conditions by creating lookups
            const unsets = [];
            for (const subQueryQuery of subQueryQueries) {
                const mainTableField = subQueryQuery.column;
                if (!mainTableField) {
                    throw new Error(`No column specified for subquery`);
                }
                const subQueryAst = subQueryQuery.ast;
                if (!mainTableField) {
                    throw new Error(`Invalid subquery`);
                }

                // check that subquery has a valid field
                if (subQueryAst.columns.length !== 1) {
                    throw new Error(
                        `Sub query for  field ${mainTableField} must have a single column`
                    );
                }
                if (
                    subQueryAst.columns[0].type !== 'expr' ||
                    !subQueryAst.columns[0].expr
                ) {
                    throw new Error(
                        `Sub query for field ${mainTableField} must have a single column expression`
                    );
                }

                const subQueryFieldName =
                    subQueryAst.columns[0].expr.as ||
                    subQueryAst.columns[0].expr.column;
                if (!subQueryFieldName || subQueryFieldName === '*') {
                    throw new Error(
                        `Sub query for field ${mainTableField} must have a single column`
                    );
                }
                const tempTableField = `tempqueryfield_${mainTableField}`;
                const subPl = makeAggregatePipeline(subQueryAst, context);

                // the parser reorders in tables incorrectly
                const subQueryCollection =
                    _getTableNameFromSubQuery(subQueryAst);
                if (
                    context._reorderedTables.indexOf(subQueryCollection) === -1
                ) {
                    context._reorderedTables.push(subQueryCollection);
                }

                // add lookup stage
                pipeline.push({
                    $lookup: {
                        from: subQueryCollection,
                        let: {
                            [`${mainTableName}_var_${mainTableField}`]: `$${mainTableField}`,
                        },
                        pipeline: subPl
                            .concat([
                                {
                                    $match: {
                                        $expr: {
                                            $eq: [
                                                `$${subQueryFieldName}`,
                                                `$$${mainTableName}_var_${mainTableField}`,
                                            ],
                                        },
                                    },
                                },
                            ])
                            .concat([
                                {$project: {check: {$literal: 1}}},
                                {$limit: 1},
                            ]),
                        as: tempTableField,
                    },
                });

                // fix where statement to include match condition
                if (subQueryQuery.operator === 'NOT IN') {
                    $json.set(preprocessedWhere, subQueryQuery.path + '$expr', {
                        $eq: [{$size: `$${tempTableField}`}, 0],
                    });
                    $json.remove(
                        preprocessedWhere,
                        subQueryQuery.path + '$$$SubQuery$$$'
                    );
                } else if (subQueryQuery.operator === 'IN') {
                    $json.set(preprocessedWhere, subQueryQuery.path + '$expr', {
                        $gt: [{$size: `$${tempTableField}`}, 0],
                    });
                    $json.remove(
                        preprocessedWhere,
                        subQueryQuery.path + '$$$SubQuery$$$'
                    );
                } else {
                    throw new Error(
                        `Sub query operations not supported: ${subQueryQuery.operator}`
                    );
                }

                // unset lookup fields
                unsets.push(tempTableField);
            }
            pipeline.push({$match: preprocessedWhere});

            if (unsets.length > 0) {
                pipeline.push({$unset: unsets});
            }
        } else {
            wherePiece = {
                $match: makeQueryPartModule.makeQueryPart(
                    ast.where,
                    context,
                    false,
                    [],
                    false,
                    ast.from && ast.from[0] ? ast.from[0].as : null,
                    false
                ),
            };
        }
    }

    if (ast.from[0].as && ast.from[0].table) {
        pipeline.push({$project: {[ast.from[0].as]: '$$ROOT'}});
        rootProjection = true;
    }

    const pipeLineJoin = makeJoinForPipelineModule.makeJoinForPipeline(
        ast,
        context
    );
    if (pipeLineJoin.length > 0) {
        optimizeJoinAndWhere(pipeline, pipeLineJoin, wherePiece, context);
        wherePiece = null;
    } else {
        if (wherePiece) {
            pipeline.push(wherePiece);
            wherePiece = null;
        }
    }

    const checkForceGroupBy = forceGroupBy(ast, context);

    if (ast.groupby || checkForceGroupBy) {
        if (isSelectAll(ast.columns)) {
            throw new Error(`Select * not allowed with group by`);
        }

        /** @type {import('../types').Column[]} */
        // @ts-ignore
        const columns = ast.columns;

        columns.forEach((column) => {
            groupByColumnParserModule.groupByColumnParser(
                column,
                result,
                context
            );
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
            } else if (
                !k.startsWith('_tempAggregateCol_') &&
                !$check.assigned(groupByProject[k])
            ) {
                groupByProject[k] = `$${k}`;
            }
        });

        if (!$check.emptyObject(groupByProject)) {
            pipeline.push({$project: groupByProject});
        }

        if (ast.having) {
            pipeline.push({
                $match: makeQueryPartModule.makeQueryPart(ast.having, context),
            });
        }
    } else if (
        ast.columns &&
        !isSelectAll(ast.columns) &&
        ast.columns.length > 0 &&
        !context.projectionAlreadyAdded
    ) {
        /** @type {import('../types').Column[]} */
        // @ts-ignore
        const columns = ast.columns;
        columns.forEach((column) => {
            projectColumnParserModule.projectColumnParser(
                column,
                result,
                context,
                ast.from && ast.from[0] ? stripJoinHints(ast.from[0].as) : null
            );
        });
        if (result.count.length > 0) {
            result.count.forEach((countStep) => pipeline.push(countStep));
        }
        if (result.unset) {
            pipeline.push(result.unset);
        }
        if (result.windowFields && result.windowFields.length) {
            for (const windowField of result.windowFields) {
                pipeline.push({
                    $setWindowFields: windowField,
                });
            }
            result.windowFields = [];
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
        const as = ast.from[0].as;
        const tableAs = stripJoinHints(as);

        result.subQueryRootProjections.push(tableAs);
        if (as.indexOf('|pivot(') >= 0) {
            const prevPipeline = pipeline;
            pipeline = makeAggregatePipeline(ast.from[0].expr.ast, context);
            applyPivot(as, pipeline, context);
            pipeline = pipeline
                .concat([{$project: {[tableAs]: '$$ROOT'}}])
                .concat(prevPipeline);
        } else if (as.indexOf('|unpivot(') >= 0) {
            const prevPipeline = pipeline;
            pipeline = makeAggregatePipeline(ast.from[0].expr.ast, context);
            const projection = prevPipeline
                .slice()
                .reverse()
                .find((p) => !!p.$project);
            const unpivots = as
                .split('|unpivot')
                .filter((u) => u.startsWith('('))
                .map((u) => '|unpivot' + u);
            if (unpivots.length === 1) {
                applyUnpivot(unpivots[0], pipeline, projection, context);
                pipeline = pipeline
                    .concat([{$project: {[tableAs]: '$$ROOT'}}])
                    .concat(prevPipeline);
            } else {
                applyMultipleUnpivots(unpivots, pipeline, projection, context);
                pipeline = pipeline
                    .concat([{$project: {[tableAs]: '$$ROOT'}}])
                    .concat(prevPipeline);
            }
        } else {
            pipeline = makeAggregatePipeline(ast.from[0].expr.ast, context)
                .concat([{$project: {[tableAs]: '$$ROOT'}}])
                .concat(pipeline);
        }
    }

    if (result.replaceRoot) {
        pipeline.push(result.replaceRoot);
    }

    if (result.unwind && result.unwind.length > 0) {
        pipeline = pipeline.concat(result.unwind);
    }

    // generate $sort, can be complex
    if (ast.orderby && ast.orderby.length > 0) {
        const sortKeys = [];

        const sortObj = {};
        for (const currentSort of ast.orderby) {
            const asMapped = result.asMapping.find(
                (c) => c.column === currentSort.expr.column
            );
            let key = '';
            if (asMapped) {
                key = asMapped.as;
            } else {
                if (
                    currentSort.expr.table &&
                    ((result.subQueryRootProjections &&
                        result.subQueryRootProjections.indexOf(
                            currentSort.expr.table
                        ) >= 0) ||
                        rootProjection)
                ) {
                    key = `${currentSort.expr.table}.${
                        currentSort.expr.column || currentSort.expr.value
                    }`;
                } else {
                    key = currentSort.expr.column || currentSort.expr.value;
                }
            }
            sortObj[key] = currentSort.type === 'DESC' ? -1 : 1;
            sortKeys.push(key);
        }

        // check if sortKeys exists, otherwise insert the source before the last project
        if (
            !pipeline.findLastIndex ||
            !$check.function(pipeline.findLastIndex)
        ) {
            console.log(
                `findLastIndex was not a function, typeof pipeline: ${typeof pipeline}, ${pipeline}, ${JSON.stringify(
                    pipeline
                )}`
            );
        }
        const previousProjectIndex = pipeline.findLastIndex(
            (p) => !!p.$project
        );

        // previous project stage found
        if (previousProjectIndex > -1) {
            const previousProject = pipeline[previousProjectIndex];
            const projectObj = $copy(previousProject.$project);
            const unsetKeys = [];
            if (projectIsRoot(previousProject)) {
                const rootKey = Object.keys(projectObj)[0];
                for (const key of sortKeys) {
                    if (!key.startsWith(rootKey + '.')) {
                        unsetKeys.push(key);
                    }
                }
            } else {
                for (const key of sortKeys) {
                    if (projectObj[key] === undefined) {
                        unsetKeys.push(key);
                    }
                }
            }

            // if there are keys in the sort not in the project
            if (unsetKeys.length > 0) {
                // if its a simple project and not following a group
                if (
                    projectIsSimple(previousProject) &&
                    !(
                        pipeline[previousProjectIndex - 1] &&
                        pipeline[previousProjectIndex - 1].$group
                    )
                ) {
                    pipeline.splice(previousProjectIndex, 0, {
                        $sort: sortObj,
                    });
                } else if (projectIsSimple(previousProject)) {
                    // check if we can rework the sort to remove any unneeded prefixes
                    const newSort = $json.renameKey(sortObj, (k) => {
                        if (k.indexOf('.') > -1) {
                            const strippedK = k.substring(k.indexOf('.') + 1);
                            if (projectObj[strippedK] !== undefined) {
                                return strippedK;
                            }
                        }
                        return k;
                    });
                    pipeline.push({$sort: newSort});
                } else if (!projectIsSimple(previousProject)) {
                    // this is a complex dance with prefixing, adding back then removing if required
                    const sortKeyPrefixes = unsetKeys.reduce((a, v) => {
                        if (v.indexOf('.' > -1)) {
                            a.push(v.substring(0, v.indexOf('.')));
                        }
                        return a;
                    }, []);
                    const projectPrefixes = Object.keys(projectObj)
                        .reduce((a, v) => {
                            if (v.indexOf('.' > -1)) {
                                a.push(v.substring(0, v.indexOf('.')));
                            }
                            return a;
                        }, [])
                        .filter((v) => !!v);
                    const removePrefixes = lodash.uniq(
                        lodash
                            .difference(sortKeyPrefixes, projectPrefixes)
                            .filter((v) => !!v)
                    );

                    for (const key of unsetKeys) {
                        projectObj[key] = 1;
                    }
                    pipeline.splice(previousProjectIndex, 1);
                    pipeline.push({$project: projectObj});
                    pipeline.push({$sort: sortObj});
                    pipeline.push({$unset: unsetKeys});
                    if (removePrefixes.length > 0) {
                        pipeline.push({$unset: removePrefixes});
                    }
                } else {
                    pipeline.push({$sort: sortObj});
                }
            } else {
                pipeline.push({$sort: sortObj});
            }
        } else {
            pipeline.push({$sort: sortObj});
        }
    }

    if (context.unsetId && !_hasIdCol(ast.columns, context)) {
        pipeline.push({$unset: '_id'});
    }

    if (ast.limit) {
        if (
            ast.limit.seperator &&
            ast.limit.seperator === 'offset' &&
            ast.limit.value[1] &&
            ast.limit.value[1].value
        ) {
            pipeline.push({
                $limit: formatLargeNumber(ast.limit.value[0].value),
            });
            pipeline.push({$skip: formatLargeNumber(ast.limit.value[1].value)});
        } else if (
            ast.limit.value &&
            ast.limit.value[0] &&
            ast.limit.value[0].value
        ) {
            pipeline.push({$limit: ast.limit.value[0].value});
        }
    }
    if (
        (ast._next && ast.union && ast.union === 'union all') ||
        (ast.set_op && ast.set_op === 'union all')
    ) {
        const otherPipeline = makeAggregatePipeline(ast._next, context);
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
    if (
        (ast._next && ast.union && ast.union === 'union') ||
        (ast.set_op && ast.set_op === 'union')
    ) {
        const otherPipeline = makeAggregatePipeline(ast._next, context);
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
    if (ast._next && ast.set_op && ast.set_op === 'intersect') {
        handleIntersect(ast, context, pipeline);
    }
    if (ast._next && ast.set_op && ast.set_op === 'except') {
        handleExcept(ast, context, pipeline);
    }

    return pipeline;
}

/**
 *
 * @param {import('../types').AST} ast - the ast to make an aggregate pipeline from
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {import('../types').PipelineFn[]} pipeline
 */
function handleIntersect(ast, context, pipeline) {
    const otherPipeline = makeAggregatePipeline(ast._next, context);
    // can be an object with a list of fields, or else '*' : '$*' or a mix of both
    let firstQueryFields = mapColumnsToNameValuePairs(ast.columns);
    let secondQueryFields = mapColumnsToNameValuePairs(ast._next.columns);
    if (firstQueryFields.length !== secondQueryFields.length) {
        throw new Error(
            `each EXCEPT query must have the same number of columns`
        );
    }
    const intersectionCollectionName =
        ast._next.from[0].table ||
        (ast._next.from[0].expr &&
        ast._next.from[0].expr.ast &&
        ast._next.from[0].expr.ast.from &&
        ast._next.from[0].expr.ast.from[0] &&
        ast._next.from[0].expr.ast.from[0].table
            ? ast._next.from[0].expr.ast.from[0].table
            : null) ||
        null;
    if (!intersectionCollectionName) {
        throw new Error('No collection to EXCEPT with');
    }
    const sortStep = extractSortFromPipeline(otherPipeline);
    pipeline.push({
        $unionWith: {
            coll: intersectionCollectionName,
            pipeline: otherPipeline,
        },
    });
    const firstHasSelectAll = hasSelectAll(firstQueryFields);
    const secondHasSelectAll = hasSelectAll(secondQueryFields);
    if (firstHasSelectAll || secondHasSelectAll) {
        if (firstHasSelectAll !== secondHasSelectAll) {
            throw new Error(
                `each INTERSECT query must have the same number of columns and if one has an "*" both must`
            );
        }
        if (!context.schemas) {
            throw new Error(
                'Cannot perform an INTERSECT using "*" without schemas being provided'
            );
        }
        if (ast.from[0].expr || ast._next.from[0].expr) {
            throw new Error(
                'Cannot perform an INTERSECT on subqueries using "*" '
            );
        }
        const firstCollectionName = ast.from[0].as || ast.from[0].table;
        if (!firstCollectionName) {
            throw new Error(
                'Unable to find the first collection name while using INTERSECT'
            );
        }
        const firstSchema = context.schemas[firstCollectionName];
        if (!firstSchema) {
            throw new Error(
                `Schema for INTERSECT not found: ${firstCollectionName}`
            );
        }
        const secondSchema = context.schemas[intersectionCollectionName];
        if (!secondSchema) {
            throw new Error(
                `Schema for INTERSECT not found: ${intersectionCollectionName}`
            );
        }

        firstQueryFields = getNameValuePairsFromSchema(
            firstSchema,
            firstCollectionName
        );
        secondQueryFields = getNameValuePairsFromSchema(
            secondSchema,
            intersectionCollectionName
        );
    }
    const _idField = firstQueryFields.reduce((obj, {name, value}, index) => {
        const {name: otherName, value: otherValue} = secondQueryFields[index];
        if (name === otherName) {
            obj[name] = value;
        } else {
            obj[name] = {
                $ifNull: [
                    value,
                    {
                        $ifNull: [otherValue, null],
                    },
                ],
            };
        }
        return obj;
    }, {});

    pipeline.push({
        $group: {
            _id: _idField,
            count: {$sum: 1},
        },
    });
    pipeline.push({
        $match: {count: {$gt: 1}},
    });
    pipeline.push({$replaceRoot: {newRoot: '$_id'}});
    if (sortStep) {
        pipeline.push(sortStep);
    }
}

/**
 *
 * @param {import('../types').AST} ast - the ast to make an aggregate pipeline from
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {import('../types').PipelineFn[]} pipeline
 */
function handleExcept(ast, context, pipeline) {
    const otherPipeline = makeAggregatePipeline(ast._next, context);
    // can be an object with a list of fields, or else '*' : '$*' or a mix of both
    let firstQueryFields = mapColumnsToNameValuePairs(ast.columns);
    let secondQueryFields = mapColumnsToNameValuePairs(ast._next.columns);
    if (firstQueryFields.length !== secondQueryFields.length) {
        throw new Error(
            `each INTERSECT query must have the same number of columns`
        );
    }
    const intersectionCollectionName =
        ast._next.from[0].table ||
        (ast._next.from[0].expr &&
        ast._next.from[0].expr.ast &&
        ast._next.from[0].expr.ast.from &&
        ast._next.from[0].expr.ast.from[0] &&
        ast._next.from[0].expr.ast.from[0].table
            ? ast._next.from[0].expr.ast.from[0].table
            : null) ||
        null;
    if (!intersectionCollectionName) {
        throw new Error('No collection to EXCEPT with');
    }
    pipeline.push({
        $addFields: {
            ___is_primary: true,
        },
    });
    const sortStep = extractSortFromPipeline(otherPipeline);
    pipeline.push({
        $unionWith: {
            coll: intersectionCollectionName,
            pipeline: otherPipeline,
        },
    });
    const firstHasSelectAll = hasSelectAll(firstQueryFields);
    const secondHasSelectAll = hasSelectAll(secondQueryFields);
    if (firstHasSelectAll || secondHasSelectAll) {
        if (firstHasSelectAll !== secondHasSelectAll) {
            throw new Error(
                `each EXCEPT query must have the same number of columns and if one has an "*" both must`
            );
        }
        if (!context.schemas) {
            throw new Error(
                'Cannot perform an EXCEPT using "*" without schemas being provided'
            );
        }
        if (ast.from[0].expr || ast._next.from[0].expr) {
            throw new Error(
                'Cannot perform an EXCEPT on subqueries using "*" '
            );
        }
        const firstCollectionName = ast.from[0].as || ast.from[0].table;
        if (!firstCollectionName) {
            throw new Error(
                'Unable to find the first collection name while using EXCEPT'
            );
        }
        const firstSchema = context.schemas[firstCollectionName];
        if (!firstSchema) {
            throw new Error(
                `Schema for EXCEPT not found: ${firstCollectionName}`
            );
        }
        const secondSchema = context.schemas[intersectionCollectionName];
        if (!secondSchema) {
            throw new Error(
                `Schema for EXCEPT not found: ${intersectionCollectionName}`
            );
        }

        firstQueryFields = getNameValuePairsFromSchema(
            firstSchema,
            firstCollectionName
        );
        secondQueryFields = getNameValuePairsFromSchema(
            secondSchema,
            intersectionCollectionName
        );
    }
    const _idField = firstQueryFields.reduce((obj, {name, value}, index) => {
        const {name: otherName, value: otherValue} = secondQueryFields[index];
        if (name === otherName) {
            obj[name] = value;
        } else {
            obj[name] = {
                $ifNull: [
                    value,
                    {
                        $ifNull: [otherValue, null],
                    },
                ],
            };
        }
        return obj;
    }, {});
    pipeline.push({
        $group: {
            _id: _idField,
            count: {$sum: 1},
            ___is_primary: {$first: '$___is_primary'},
        },
    });
    pipeline.push({
        $match: {count: {$lte: 1}, ___is_primary: true},
    });
    pipeline.push({$replaceRoot: {newRoot: '$_id'}});
    if (sortStep) {
        pipeline.push(sortStep);
    }
}

/**
 *
 * @param {string} input
 * @returns {string}
 */
function stripJoinHints(input) {
    if (!input) {
        return input;
    }
    return input.split('|')[0];
}

/**
 *
 * @param {import('../types').Columns} columns
 * @returns {{name:string,value:string}[]}
 */
function mapColumnsToNameValuePairs(columns) {
    if (typeof columns === 'string') {
        return [];
    }
    return columns
        .map((c) => c.as || c.expr.column)
        .filter(Boolean)
        .map((columnName) => {
            return {name: columnName, value: `$${columnName}`};
        });
}

/**
 *
 * @param {{name:string,value:string}[]} column
 * @returns {boolean}
 */
function hasSelectAll(column) {
    return column.map((f) => f.name).indexOf('*') >= 0;
}

/**
 *
 * @param {import('json-schema').JSONSchema6} schema
 * @param {string} collectionName
 * @returns {{name:string,value:string}[]}
 */
function getNameValuePairsFromSchema(schema, collectionName) {
    if (!schema.properties) {
        throw new Error(`Schema for "${collectionName}" has no properties`);
    }
    if (typeof schema.properties === 'boolean') {
        throw new Error(
            `Schema for "${collectionName}" had properties of type boolean`
        );
    }
    return Object.keys(schema.properties)
        .map((name) => {
            return {
                name,
                value: `$${name}`,
            };
        })
        .filter((col) => col.name !== '_id');
}

/**
 *
 * @param {import('../types').PipelineFn[]} pipeline
 * @returns {import('../types').PipelineFn|null}
 */
function extractSortFromPipeline(pipeline) {
    const index = pipeline.findIndex((p) => !!p.$sort);
    const sortStep = pipeline[index];
    pipeline.splice(index, 1);
    return sortStep;
}
