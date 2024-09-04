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
const {
    getWhereAstQueries,
    getWhereStandardQueries,
} = require('./filter-queries');

const $copy = require('clone-deep');
const {optimizeJoinAndWhere} = require('./optimize-join-and-where');
const {applyPivot, applyUnpivot} = require('./apply-pivot');
exports.makeAggregatePipeline = makeAggregatePipeline;

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

/**
 * Creates an mongo aggregation pipeline given an ast
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

    let wherePiece;
    if (ast.where) {
        if (whereContainsOtherTable(ast.where)) {
            const astQueryColumns = getWhereAstQueries(ast.where, context);
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

                    const subPl = makeAggregatePipeline(ast, context);
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
            const standardQueries = getWhereStandardQueries(ast.where, context);
            standardQueries.forEach((sq) => {
                pipeline.push({
                    $match: makeQueryPartModule.makeQueryPart(
                        sq,
                        context,
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
                    context,
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
        ast.columns.length > 0
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
        if (result.windowFields) {
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
            applyUnpivot(as, pipeline, context);
            pipeline = pipeline
                .concat([{$project: {[tableAs]: '$$ROOT'}}])
                .concat(prevPipeline);
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

    if (ast.orderby && ast.orderby.length > 0) {
        pipeline.push({
            $sort: ast.orderby.reduce((sortObj, currentSort) => {
                const asMapped = result.asMapping.find(
                    (c) => c.column === currentSort.expr.column
                );
                let key = '';
                if (asMapped) {
                    key = asMapped.as;
                } else {
                    if (
                        currentSort.expr.table &&
                        result.subQueryRootProjections &&
                        result.subQueryRootProjections.indexOf(
                            currentSort.expr.table
                        ) >= 0
                    ) {
                        key = `${currentSort.expr.table}.${
                            currentSort.expr.column || currentSort.expr.value
                        }`;
                    } else {
                        key = currentSort.expr.column || currentSort.expr.value;
                    }
                }
                sortObj[key] = currentSort.type === 'DESC' ? -1 : 1;

                return sortObj;
            }, {}),
        });
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
