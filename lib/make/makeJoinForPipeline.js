const makeFilterConditionModule = require('./makeFilterCondition');
const $check = require('check-types');
const $json = require('@synatic/json-magic');
const makeAggregatePipelineModule = require('./makeAggregatePipeline');
const projectColumnParserModule = require('./projectColumnParser');
const {createResultObject} = require('./createResultObject');

exports.makeJoinForPipeline = makeJoinForPipeline;

/**
 *
 * @param {import('../types').TableDefinition} join
 * @param {string[]} aliases - the aliases used in the joins
 * @param {string} toAs
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @returns {string[]}
 */
function getJoinAliases(join, aliases, toAs, context) {
    const joinAliases = [];
    if ((join.join === 'INNER JOIN' || join.join === 'LEFT JOIN') && join.as) {
        return aliases.filter((a) => a !== toAs);
    }
    return joinAliases;
}

/**
 * Creates the pipeline components for a join
 * @param {import('../types').AST} ast - the ast that contains the join
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @returns {*[]}
 */
function makeJoinForPipeline(ast, context) {
    const pipeline = [];

    const aliases = ast.from
        .map((f) => {
            if (f.as) {
                return f.as;
            } else if (f.table) {
                return f.table;
            } else {
                return null;
            }
        })
        .filter((f) => !!f)
        .map((a) => a.split('|')[0]);

    for (let i = 1; i < ast.from.length; i++) {
        makeJoinPart(
            ast.from[i],
            ast.from[i - 1],
            aliases,
            pipeline,
            context,
            ast
        );
    }

    return pipeline;
}

/**
 * Makes a single join part
 * @param {import('../types').TableDefinition} join
 * @param {import('../types').TableDefinition} previousJoin
 * @param {string[]} aliases - the aliases used in the joins
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {import('../types').TableColumnAst} ast
 * @returns {void}
 */
function makeJoinPart(join, previousJoin, aliases, pipeline, context, ast) {
    let toTable = join.table || '';
    let toAs = join.as || '';

    const joinHints = toTable
        .toLowerCase()
        .split('|')
        .slice(1)
        .concat(toAs.toLowerCase().split('|').slice(1));
    context.joinHints = joinHints;
    toTable = toTable.split('|')[0];
    toAs = toAs.split('|')[0];

    if (
        join.table &&
        join.on &&
        join.on.type === 'binary_expr' &&
        join.on.left.type === 'column_ref' &&
        join.on.right.type === 'column_ref' &&
        join.on.operator === '='
    ) {
        return tableJoin(
            join,
            previousJoin,
            pipeline,
            toTable,
            toAs,
            joinHints,
            context,
            ast
        );
    }
    const prefixLeft = shouldPrefixSide('left');
    const prefixRight = shouldPrefixSide('right');
    const joinAliases = getJoinAliases(join, aliases, toAs, context);
    const joinQuery = makeFilterConditionModule.makeFilterCondition(
        sanitizeOnCondition(join.on, joinAliases),
        context,
        false,
        prefixRight,
        null,
        prefixLeft,
        null,
        joinAliases
    );
    const inputVars = {};
    const replacePaths = [];
    $json.walk(joinQuery, (val, path) => {
        if ($check.string(val) && val.startsWith('$$')) {
            if (val === '$$NOW') {
                return;
            }
            const varName = val.substring(2).replace(/[.-]/g, '_');
            if (join.as) {
                inputVars[varName] = `$${val.substring(2)}`;
            } else {
                const parts = val.substring(2).split('.');
                inputVars[varName] = `$${
                    parts.length > 1 ? parts[1] : parts[0]
                }`;
            }
            replacePaths.push({path: path, newVal: `$$${varName}`});
        } else if (
            $check.string(val) &&
            aliases.find(
                (a) => a !== toAs && a !== toTable && val.startsWith(`$${a}.`)
            )
        ) {
            const varName = lowerCaseFist(
                val.substring(1).replace(/[.-]/g, '_')
            );

            inputVars[varName] = `$${val.substring(1)}`;
            replacePaths.push({path: path, newVal: `$$${varName}`});
        }
    });
    for (const path of replacePaths) {
        $json.set(joinQuery, path.path, path.newVal);
    }

    let lookupPipeline = [];

    if (join.expr && join.expr.ast) {
        const innerAst = join.expr.ast;
        lookupPipeline = makeAggregatePipelineModule.makeAggregatePipeline(
            innerAst,
            context
        );
        if (innerAst.from[0] && innerAst.from[0].table) {
            toTable = innerAst.from[0].table;
        } else if (
            innerAst.from[0] &&
            innerAst.from[0].expr &&
            innerAst.from[0].expr.ast &&
            innerAst.from[0].expr.ast.from &&
            innerAst.from[0].expr.ast.from[0] &&
            innerAst.from[0].expr.ast.from[0].table
        ) {
            toTable = innerAst.from[0].expr.ast.from[0].table;
        } else {
            throw new Error('Missing table for join sub query');
        }
    }

    if (joinHints.includes('optimize')) {
        lookupPipeline.unshift({$match: {$expr: joinQuery}});
    } else {
        lookupPipeline.push({$match: {$expr: joinQuery}});
    }

    pipeline.push({
        $lookup: {
            from: toTable,
            as: toAs || toTable,
            let: inputVars,
            pipeline: lookupPipeline,
        },
    });

    if ((joinHints && joinHints.length > 0) || context.unwindJoins) {
        if (joinHints.includes('first')) {
            pipeline.push({
                $set: {
                    [toAs || toTable]: {$first: `$${toAs || toTable}`},
                },
            });
        } else if (joinHints.includes('last')) {
            pipeline.push({
                $set: {
                    [toAs || toTable]: {$last: `$${toAs || toTable}`},
                },
            });
        } else if (joinHints.includes('unwind') || context.unwindJoins) {
            pipeline.push({
                $unwind: {
                    path: `$${toAs || toTable}`,
                    preserveNullAndEmptyArrays: true,
                },
            });
        }
    }
    if (join.join === 'INNER JOIN') {
        if (
            joinHints &&
            joinHints.length > 0 &&
            (joinHints.includes('first') ||
                joinHints.includes('last') ||
                joinHints.includes('unwind'))
        ) {
            pipeline.push({$match: {[toAs || toTable]: {$ne: null}}});
        } else {
            pipeline.push({
                $match: {
                    $expr: {$gt: [{$size: `$${toAs || toTable}`}, 0]},
                },
            });
        }
    } else if (join.join === 'LEFT JOIN') {
        // dont need anything
    } else {
        throw new Error(`Join not supported:${join.join}`);
    }

    /**
     * @param {'left'|'right'} side The side of the join
     * @returns {boolean} if the side should be prefixed or not
     */
    function shouldPrefixSide(side) {
        const defaultPrefix = side === 'left' ? false : true;
        const table = join.on[side].table;
        if (join.as && table) {
            return table !== toAs;
        }
        return defaultPrefix;
    }
}

/**
 *
 * @param {import('../types').Expression} condition
 * @param {string[]}joinAliases
 * @returns {import('../types').Expression}
 */
function sanitizeOnCondition(condition, joinAliases) {
    // clone condition to prevent issues?
    if (
        condition.left.type === 'function' &&
        condition.left.args &&
        condition.left.args.value &&
        Array.isArray(condition.left.args.value)
    ) {
        condition.left.args.value = condition.left.args.value.map((a) => {
            if (a.table && joinAliases.indexOf(a.table) < 0) {
                return {...a, table: ''};
            }
            return a;
        });
    }
    if (
        condition.right.type === 'function' &&
        condition.right.args &&
        condition.right.args.value &&
        Array.isArray(condition.right.args.value)
    ) {
        condition.right.args.value = condition.right.args.value.map((a) => {
            if (a.table && joinAliases.indexOf(a.table) < 0) {
                return {...a, table: ''};
            }
            return a;
        });
    }
    return condition;
}

/**
 *
 * @param {import('../types').TableDefinition} join
 * @param {import('../types').TableDefinition} previousJoin
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {string} toTable
 * @param {string} toAs
 * @param {string[]} joinHints
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
 * @param {import('../types').TableColumnAst} ast
 * @returns {void}
 */
function tableJoin(
    join,
    previousJoin,
    pipeline,
    toTable,
    toAs,
    joinHints,
    context,
    ast
) {
    let localPart;
    let fromPart;
    if (join.on.left.table === toAs) {
        localPart = join.on.right;
        fromPart = join.on.left;
    } else if (
        join.on.right.table === toAs ||
        join.on.right.table === toTable
    ) {
        localPart = join.on.left;
        fromPart = join.on.right;
        // eslint-disable-next-line sonarjs/no-duplicated-branches
    } else {
        localPart = join.on.right;
        fromPart = join.on.left;
    }
    const localField = localPart.table
        ? `${localPart.schema ? localPart.schema + '.' : ''}${
              localPart.table
          }.${localPart.column}`
        : previousJoin.as
          ? `${previousJoin.as}.${localPart.column}`
          : localPart.column;
    const foreignField = fromPart.column;
    pipeline.push({
        $lookup: {
            from: toTable,
            as: toAs || toTable,
            localField: localField,
            foreignField: foreignField,
        },
    });
    if ((joinHints && joinHints.length > 0) || context.unwindJoins) {
        if (joinHints.includes('first')) {
            pipeline.push({
                $set: {
                    [toAs || toTable]: {$first: `$${toAs || toTable}`},
                },
            });
        } else if (joinHints.includes('last')) {
            pipeline.push({
                $set: {
                    [toAs || toTable]: {$last: `$${toAs || toTable}`},
                },
            });
        } else if (joinHints.includes('unwind') || context.unwindJoins) {
            pipeline.push({
                $unwind: {
                    path: `$${toAs || toTable}`,
                    preserveNullAndEmptyArrays: true,
                },
            });
        }
    }
    if (join.join === 'INNER JOIN') {
        if (
            joinHints &&
            joinHints.length > 0 &&
            (joinHints.includes('first') ||
                joinHints.includes('last') ||
                joinHints.includes('unwind'))
        ) {
            pipeline.push({$match: {[toAs || toTable]: {$ne: null}}});
        } else {
            pipeline.push({
                $match: {
                    $expr: {$gt: [{$size: `$${toAs || toTable}`}, 0]},
                },
            });
        }
    } else if (join.join === 'LEFT JOIN') {
        // dont need anything
    } else if (join.join === 'FULL JOIN') {
        const toCollection = toAs || toTable;
        const localTable = previousJoin.table || localPart.table;
        pipeline.push({
            $unwind: {
                path: `$${toCollection}`,
                preserveNullAndEmptyArrays: true,
            },
        });
        pipeline.push({
            $unionWith: {
                coll: toTable,
                pipeline: [
                    {
                        $lookup: {
                            from: localTable,
                            localField: localPart.column,
                            foreignField: fromPart.column,
                            as: localPart.table,
                        },
                    },
                ],
            },
        });
        pipeline.push({
            $unwind: {
                path: `$${localPart.table}`,
                preserveNullAndEmptyArrays: true,
            },
        });
        const result = createResultObject();
        const columns = ast.columns;
        columns.forEach((column) => {
            projectColumnParserModule.projectColumnParser(
                column,
                result,
                context,
                ast.from && ast.from[0]
                    ? makeAggregatePipelineModule.stripJoinHints(ast.from[0].as)
                    : null
            );
        });
        const project = (result.parsedProject || {}).$project;
        if (!project || Object.keys(project).length === 0) {
            throw new Error(`Unable to get $projection for full outer join`);
        }
        // todo need the projection part and the schema
        pipeline.push({
            $project: {
                ...Object.entries(project).reduce((previous, current) => {
                    const [key, value] = current;
                    previous[key] = {
                        $ifNull: [`$${key}`, value],
                    };
                    return previous;
                }, {}),
            },
        });
        pipeline.push({
            $group: {
                _id: {
                    ...Object.keys(project).reduce((previous, current) => {
                        previous[current] = `$${current}`;
                        return previous;
                    }, {}),
                },
            },
        });
        pipeline.push({
            $project: {
                _id: 0,
                ...Object.keys(project).reduce((previous, current) => {
                    previous[current] = `$_id.${current}`;
                    return previous;
                }, {}),
            },
        });
        context.projectionAlreadyAdded = true;
        if (result.unset) {
            pipeline.push(result.unset);
        }
    } else {
        throw new Error(`Join not supported:${join.join}`);
    }
}

/**
 *
 * @param {string} val
 * @returns {string}
 */
function lowerCaseFist(val) {
    return `${val.charAt(0).toLowerCase()}${val.substring(1)}`;
}
