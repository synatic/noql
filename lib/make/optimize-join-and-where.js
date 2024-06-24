const {snakeCase} = require('lodash');
module.exports = {optimizeJoinAndWhere};

/**
 *
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').PipelineFn[]} pipeLineJoin
 * @param {import('../types').PipelineFn} wherePiece
 * @param {import('../types').NoqlContext} context
 * @returns {void}
 */
function optimizeJoinAndWhere(pipeline, pipeLineJoin, wherePiece, context) {
    let hasBeenOptimised = false;
    const from = context.fullAst.ast.from;
    const lookup = pipeLineJoin.find((p) => Boolean(p.$lookup));
    const match = wherePiece.$match;
    // todo rk going to have to use recursion here
    if (wherePiece && from.length === 2 && lookup && match) {
        const sourceName = from[0].as || from[0].table;
        const destinationName = from[1].as || from[1].table;
        /** @type {'$and'|'$or'} */
        const matchType = match.$and ? '$and' : match.$or ? '$or' : '$and';
        const matches = match[matchType] ? match[matchType] : [match];
        const {destinationMatches, sourceMatches, miscMatches} = getMatches(
            matches,
            sourceName,
            destinationName
        );
        if (sourceMatches.length > 0) {
            if (sourceMatches.length > 1) {
                pipeline.push({$match: {[matchType]: sourceMatches}});
            } else {
                pipeline.push({$match: sourceMatches[0]});
            }
            hasBeenOptimised = true;
        }
        if (destinationMatches.length > 0) {
            const mappedMatches = destinationMatches.map((d) => {
                return mapMatchesToAndFormat(d, destinationName);
            });
            if (!lookup.$lookup.pipeline) {
                const localField = lookup.$lookup.localField;
                const foreignField = lookup.$lookup.foreignField;
                delete lookup.$lookup.localField;
                delete lookup.$lookup.foreignField;
                lookup.$lookup.let = lookup.$lookup.let || {};
                const letVar = snakeCase(localField);
                lookup.$lookup.let[letVar] = `$${localField}`;
                lookup.$lookup.pipeline = [
                    {
                        $match: {
                            $expr: {
                                [matchType]: [
                                    {
                                        $eq: [
                                            `$${foreignField}`,
                                            `$$${letVar}`,
                                        ],
                                    },
                                    ...mappedMatches,
                                ],
                            },
                        },
                    },
                ];
            } else {
                if (
                    lookup.$lookup.pipeline[0].$match &&
                    lookup.$lookup.pipeline[0].$match.$expr &&
                    lookup.$lookup.pipeline[0].$match.$expr[matchType]
                ) {
                    for (const mappedMatch of mappedMatches) {
                        lookup.$lookup.pipeline[0].$match.$expr[matchType].push(
                            mappedMatch
                        );
                    }
                } else {
                    if (mappedMatches.length === 1) {
                        lookup.$lookup.pipeline.push({
                            $match: {$expr: mappedMatches[0]},
                        });
                    } else {
                        lookup.$lookup.pipeline.push({
                            $match: {$expr: {[matchType]: mappedMatches}},
                        });
                    }
                }
            }
            hasBeenOptimised = true;
        }
        if (miscMatches.length > 0) {
            // todo
            throw new Error(`TODO not implemented`);
        }
    }

    pushToPipeline();
    /**
     *
     */
    function pushToPipeline() {
        for (const join of pipeLineJoin) {
            pipeline.push(join);
        }
        if (hasBeenOptimised) {
            return;
        }
        if (wherePiece) {
            pipeline.push(wherePiece);
        }
    }
}
/**
 *
 * @param {Record<string,unknown>[]}matches
 * @param {string} sourceName
 * @param {string} destinationName
 */
function getMatches(matches, sourceName, destinationName) {
    const sourceMatches = matches.filter((m) =>
        Object.keys(m)[0].startsWith(`${sourceName}.`)
    );
    const destinationMatches = matches.filter((m) =>
        Object.keys(m)[0].startsWith(`${destinationName}.`)
    );
    const miscMatches = matches.filter((m) => {
        const key = Object.keys(m)[0];
        return (
            !key.startsWith(`${sourceName}.`) &&
            !key.startsWith(`${destinationName}.`)
        );
    });
    return {
        sourceMatches,
        destinationMatches,
        miscMatches,
    };
}

/**
 *
 * @param d
 * @param {string} destinationName
 */
function mapMatchesToAndFormat(d, destinationName) {
    const key = Object.keys(d)[0];
    const newKey = key.startsWith(`${destinationName}.`)
        ? key.substring(destinationName.length + 1)
        : destinationName;
    const match = d[key];
    const operator = Object.keys(match)[0];
    const value = match[operator];
    return {
        [operator]: [`$${newKey}`, value],
    };
}
