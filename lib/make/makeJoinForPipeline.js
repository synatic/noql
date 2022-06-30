const makeFilterConditionModule = require('./makeFilterCondition');
const $check = require('check-types');
const $json = require('@synatic/json-magic');
const makeAggregatePipelineModule = require('./makeAggregatePipeline');

exports.makeJoinForPipeline = makeJoinForPipeline;

/**
 * Creates the pipeline components for a join
 *
 * @param {import('../types').AST} ast - the ast that contains the join
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

        if (
            join.table &&
            join.on &&
            join.on.type === 'binary_expr' &&
            join.on.operator === '='
        ) {
            // todo rework this to handle correctly
            const localTable = fromAs || fromTable;
            const foreignTable = toTable;

            const localField =
                (fromTable && join.on.left.table === fromTable) ||
                (fromAs && join.on.left.table === fromAs)
                    ? `${localTable ? localTable + '.' : ''}${
                          join.on.left.column
                      }`
                    : // eslint-disable-next-line sonarjs/no-all-duplicated-branches
                    join.on.right.table === fromTable ||
                      join.on.right.table === fromAs
                    ? `${localTable ? localTable + '.' : ''}${
                          join.on.right.column
                      }`
                    : `${localTable ? localTable + '.' : ''}${
                          join.on.right.column
                      }`;

            const foreignField =
                join.on.left.table === toTable || join.on.left.table === toAs
                    ? join.on.left.column
                    : join.on.right.table === toTable ||
                      join.on.right.table === toAs
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
                    pipeline.push({
                        $set: {
                            [toAs || toTable]: {$first: `$${toAs || toTable}`},
                        },
                    });
                } else if (joinHint === 'last') {
                    pipeline.push({
                        $set: {
                            [toAs || toTable]: {$last: `$${toAs || toTable}`},
                        },
                    });
                } else if (joinHint === 'unwind') {
                    pipeline.push({
                        $unwind: {
                            path: `$${toAs || toTable}`,
                            preserveNullAndEmptyArrays: true,
                        },
                    });
                }
            }
            if (join.join === 'INNER JOIN') {
                if (joinHint) {
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
        } else {
            const joinQuery = makeFilterConditionModule.makeFilterCondition(
                join.on,
                false,
                true
            );
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
                lookupPipeline =
                    makeAggregatePipelineModule.makeAggregatePipeline(
                        join.expr.ast
                    );
                if (join.expr.ast.from[0] && join.expr.ast.from[0].table)
                    fromTable = join.expr.ast.from[0].table;
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
                    pipeline.push({
                        $set: {
                            [toAs || toTable]: {$first: `$${toAs || toTable}`},
                        },
                    });
                } else if (joinHint === 'last') {
                    pipeline.push({
                        $set: {
                            [toAs || toTable]: {$last: `$${toAs || toTable}`},
                        },
                    });
                } else if (joinHint === 'unwind') {
                    pipeline.push({
                        $unwind: {
                            path: `$${toAs || toTable}`,
                            preserveNullAndEmptyArrays: true,
                        },
                    });
                }
            }
            if (join.join === 'INNER JOIN') {
                if (joinHint) {
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
    };

    for (let i = 1; i < ast.from.length; i++) {
        makeJoinPart(ast.from[i], ast.from[i - 1]);
    }

    return pipeline;
}
