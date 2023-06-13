const makeFilterConditionModule = require('./makeFilterCondition');
const $check = require('check-types');
const $json = require('@synatic/json-magic');
const makeAggregatePipelineModule = require('./makeAggregatePipeline');

exports.makeJoinForPipeline = makeJoinForPipeline;

/**
 *
 * @param {import('../types').TableDefinition} join
 * @param {string[]} aliases - the aliases used in the joins
 * @param {string} toAs
 */
function getJoinAliases(join, aliases, toAs) {
    const joinAliases = [];
    if ((join.join === 'INNER JOIN' || join.join === 'LEFT JOIN') && join.as) {
        return aliases.filter((a) => a !== toAs);
    }
    return joinAliases;
}

/**
 * Creates the pipeline components for a join
 *
 * @param {import('../types').AST} ast - the ast that contains the join
 * @returns {*[]}
 */
function makeJoinForPipeline(ast) {
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
        makeJoinPart(ast.from[i], ast.from[i - 1], aliases, pipeline);
    }

    return pipeline;
}

/**
 * Makes a single join part
 *
 * @param {import('../types').TableDefinition} join
 * @param {import('../types').TableDefinition} previousJoin
 * @param {string[]} aliases - the aliases used in the joins
 * @param {import('../types').PipelineFn[]} pipeline
 * @returns {void}
 */
function makeJoinPart(join, previousJoin, aliases, pipeline) {
    let toTable = join.table || '';
    let toAs = join.as || '';

    const joinHints = toTable
        .toLowerCase()
        .split('|')
        .slice(1)
        .concat(toAs.toLowerCase().split('|').slice(1));
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
            joinHints
        );
    }
    const prefixLeft = shouldPrefixSide('left');
    const prefixRight = shouldPrefixSide('right');
    const joinAliases = getJoinAliases(join, aliases, toAs);
    const joinQuery = makeFilterConditionModule.makeFilterCondition(
        join.on,
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
            const varName = val.substring(1).replace(/[.-]/g, '_');
            inputVars[varName] = `$${val.substring(1)}`;
            replacePaths.push({path: path, newVal: `$$${varName}`});
        }
    });
    for (const path of replacePaths) {
        $json.set(joinQuery, path.path, path.newVal);
    }

    let lookupPipeline = [];

    if (join.expr && join.expr.ast) {
        lookupPipeline = makeAggregatePipelineModule.makeAggregatePipeline(
            join.expr.ast
        );
        if (
            join.expr.ast.from[0] &&
            (join.expr.ast.from[0].table || join.expr.ast.from[0].as)
        ) {
            toTable = join.expr.ast.from[0].table || join.expr.ast.from[0].as;
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
    if (joinHints && joinHints.length > 0) {
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
        } else if (joinHints.includes('unwind')) {
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
 * @param {import('../types').TableDefinition} join
 * @param {import('../types').TableDefinition} previousJoin
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {string} toTable
 * @param {string} toAs
 * @param {string[]} joinHints
 * @returns {void}
 */
function tableJoin(join, previousJoin, pipeline, toTable, toAs, joinHints) {
    let localPart;
    let fromPart;
    if (join.on.left.table === toAs || join.on.left.table === toTable) {
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
        ? `${localPart.table}.${localPart.column}`
        : `${previousJoin.as || previousJoin.table}.${localPart.column}`;
    const foreignField = fromPart.column;
    pipeline.push({
        $lookup: {
            from: toTable,
            as: toAs || toTable,
            localField: localField,
            foreignField: foreignField,
        },
    });
    if (joinHints && joinHints.length > 0) {
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
        } else if (joinHints.includes('unwind')) {
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
}
