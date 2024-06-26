const {snakeCase} = require('lodash');
const $check = require('check-types');
module.exports = {optimizeJoinAndWhere};

/**
 * @typedef {'$and' | '$or' | null} MatchType
 * @typedef {{wasOptimised:boolean, leftOverMatches: Record<string,unknown>[]}} ProcessResult
 */

/**
 *
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').PipelineFn[]} pipeLineJoin
 * @param {import('../types').PipelineFn} wherePiece
 * @param {import('../types').NoqlContext} context
 * @returns {void}
 */
function optimizeJoinAndWhere(pipeline, pipeLineJoin, wherePiece, context) {
    const lookup = pipeLineJoin.find((p) => Boolean(p.$lookup));

    const {wasOptimised, leftOverMatches} = wherePiece
        ? recursivelyOptimise(wherePiece.$match, lookup, pipeline, context)
        : {wasOptimised: false, leftOverMatches: []};

    pushToPipeline();
    /**
     *
     */
    function pushToPipeline() {
        for (const join of pipeLineJoin) {
            pipeline.push(join);
        }
        if (!wasOptimised && wherePiece) {
            pipeline.push(wherePiece);
            return;
        }
        if (leftOverMatches.length > 0) {
            for (const leftOverMatch of leftOverMatches) {
                pipeline.push({$match: leftOverMatch});
            }
        }
    }
}

/**
 *
 * @param {Record<string,unknown>} match
 * @param {import('../types').PipelineFn} lookup
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 * @returns {ProcessResult} the leftover matches to add to the end
 */
function recursivelyOptimise(match, lookup, pipeline, context) {
    /** @type {ProcessResult} */
    const returnResult = {
        wasOptimised: false,
        leftOverMatches: [],
    };
    if (!match) {
        return returnResult;
    }
    const from = context.fullAst.ast.from;
    const sourceName = from[0].as || from[0].table;
    const destinationName = from[1].as || from[1].table;
    /** @type {MatchType} */
    const matchType = match.$and ? '$and' : match.$or ? '$or' : null;
    const matches = match[matchType] ? match[matchType] : [match];
    mergeProcessResults(
        processMatchesForAnds(matches, lookup, pipeline, context),
        returnResult
    );
    mergeProcessResults(
        processMatchesForOrs(matches, lookup, pipeline, context),
        returnResult
    );

    const {destinationMatches, sourceMatches, miscMatches} = getMatches(
        matches,
        sourceName,
        destinationName
    );
    mergeProcessResults(
        processSourceMatches(sourceMatches, matchType, pipeline),
        returnResult
    );
    mergeProcessResults(
        processDestinationMatches(
            destinationMatches,
            destinationName,
            lookup,
            matchType,
            sourceMatches,
            sourceName
        ),
        returnResult
    );
    if (miscMatches.length > 0) {
        mergeProcessResults(
            {wasOptimised: false, leftOverMatches: miscMatches},
            returnResult
        );
    }
    return returnResult;
}

/**
 *
 * @param {ProcessResult} source
 * @param {ProcessResult} destination
 */
function mergeProcessResults(source, destination) {
    if (source.wasOptimised) {
        destination.wasOptimised = true;
    }
    for (const leftOverMatch of source.leftOverMatches) {
        destination.leftOverMatches.push(leftOverMatch);
    }
}

/**
 *
 * @param {Record<string,unknown>[]} matches
 * @param {import('../types').PipelineFn} lookup
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 * @returns {ProcessResult}
 */
function processMatchesForAnds(matches, lookup, pipeline, context) {
    const indexesOfAnds = [];
    matches.forEach((m, index) => {
        if (Object.keys(m)[0] === '$and') {
            indexesOfAnds.push(index);
        }
    });
    /** @type {ProcessResult} */
    const returnResult = {
        wasOptimised: false,
        leftOverMatches: [],
    };
    if (indexesOfAnds.length) {
        const results = [];
        for (const index of indexesOfAnds) {
            const res = recursivelyOptimise(
                matches[index],
                lookup,
                pipeline,
                context
            );
            results.push({index, ...res});
        }
        results.sort((a, b) => b - a);
        results.forEach((result) => {
            if (result.wasOptimised) {
                returnResult.wasOptimised = true;
                matches.splice(result.index, 1);
            }
            for (const lom of result.leftOverMatches) {
                returnResult.leftOverMatches.push(lom);
            }
        });
    }
    return returnResult;
}

/**
 *
 * @param {Record<string,unknown>[]} matches
 * @param {import('../types').PipelineFn} lookup
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 * @returns {ProcessResult}
 */
function processMatchesForOrs(matches, lookup, pipeline, context) {
    const indexesOfOrs = [];
    matches.forEach((m, index) => {
        if (Object.keys(m)[0] === '$or') {
            indexesOfOrs.push(index);
        }
    });
    /** @type {ProcessResult} */
    const returnResult = {
        wasOptimised: false,
        leftOverMatches: [],
    };
    if (indexesOfOrs.length) {
        const results = [];
        for (const index of indexesOfOrs) {
            const res = recursivelyOptimise(
                matches[index],
                lookup,
                pipeline,
                context
            );
            results.push({index, ...res});
        }
        results.sort((a, b) => b - a);
        results.forEach((result) => {
            if (result.wasOptimised) {
                returnResult.wasOptimised = true;
                matches.splice(result.index, 1);
            }
            for (const lom of result.leftOverMatches) {
                returnResult.leftOverMatches.push(lom);
            }
        });
    }
    return returnResult;
}

/**
 *
 * @param {Record<string,unknown>[]} sourceMatches
 * @param {MatchType} matchType
 * @param {import('../types').PipelineFn[]} pipeline
 * @returns {ProcessResult} wasOptimised
 */
function processSourceMatches(sourceMatches, matchType, pipeline) {
    /** @type {ProcessResult} */
    const returnResult = {
        wasOptimised: false,
        leftOverMatches: [],
    };
    if (sourceMatches.length === 0) {
        return returnResult;
    }
    if (sourceMatches.length > 1) {
        pipeline.push({$match: {[matchType]: sourceMatches}});
        clearArray(sourceMatches);
        returnResult.wasOptimised = true;
    } else if (
        matchType === '$and' ||
        (!$check.assigned(matchType) && sourceMatches.length === 1)
    ) {
        pipeline.push({$match: sourceMatches[0]});
        clearArray(sourceMatches);
        returnResult.wasOptimised = true;
    }
    return returnResult;
}

/**
 *
 * @param {any[]} array
 */
function clearArray(array) {
    array.splice(0, array.length);
}

/**
 *
 * @param {Record<string,unknown>[]} destinationMatches
 * @param {string} destinationName
 * @param {import('../types').PipelineFn} lookup
 * @param {MatchType} matchType
 * @param {Record<string,unknown>[]} leftOverSourceMatches
 * @param {string} sourceName
 * @returns {ProcessResult}
 */
function processDestinationMatches(
    destinationMatches,
    destinationName,
    lookup,
    matchType,
    leftOverSourceMatches,
    sourceName
) {
    /** @type {ProcessResult} */
    const returnResult = {
        wasOptimised: false,
        leftOverMatches: [],
    };
    if (destinationMatches.length === 0) {
        return returnResult;
    }
    // ensures the lookup is in the right format to be optimised
    if (!lookup.$lookup.pipeline) {
        convertLookupToPipeline(lookup);
    } else {
        optimizeExistingPipeline(lookup);
    }
    const mappedMatches = destinationMatches
        .map((d) => {
            return mapMatchesToExpressionFormat(d, destinationName);
        })
        .concat(
            leftOverSourceMatches.map((d) => {
                lookup.$lookup.let = lookup.$lookup.let || {};
                const field = Object.keys(d)[0];
                const letVar = snakeCase(field);
                const explicitKey = `$${letVar}`;
                lookup.$lookup.let[letVar] = `$${field}`;
                return mapMatchesToExpressionFormat(d, sourceName, explicitKey);
            })
        );

    if (
        !$check.assigned(matchType) ||
        (matchType === '$and' && mappedMatches.length === 1)
    ) {
        // only 1 condition
        lookup.$lookup.pipeline.unshift({
            $match: {$expr: mappedMatches[0]},
        });
        returnResult.wasOptimised = true;
        return returnResult;
    }
    if (matchType === '$and') {
        // if matchType is AND, can add it on to existing expression after converting
        lookup.$lookup.pipeline.unshift({
            $match: {$expr: {$and: mappedMatches}},
        });
        returnResult.wasOptimised = true;
        return returnResult;
    }
    if (mappedMatches.length === 1) {
        // find where the or goes
        const found = lookup.$lookup.pipeline.find(
            (step) =>
                !!step.$match && step.$match.$expr && step.$match.$expr.$or
        );
        if (found) {
            found.$match.$expr.$or.unshift(mappedMatches[0]);
            returnResult.wasOptimised = true;
            return returnResult;
        }
    }
    // matchType is $or
    lookup.$lookup.pipeline.unshift({
        $match: {$expr: {$or: mappedMatches}},
    });
    returnResult.wasOptimised = true;
    return returnResult;
}

/**
 *
 * @param {import('../types').PipelineFn} lookup
 */
function optimizeExistingPipeline(lookup) {
    const matches = lookup.$lookup.pipeline.filter((p) => !!p.$match);
    // optimise the existing pipeline
    let indexesToDelete = [];
    for (const match of matches) {
        if (match.$match.$expr.$and) {
            let index = 0;
            let indexesToRemove = [];
            for (const andCondition of match.$match.$expr.$and) {
                const anyArgsInSource = Object.values(andCondition)[0].some(
                    (c) => {
                        return typeof c === 'string'
                            ? c.startsWith('$$')
                            : false;
                    }
                );
                if (anyArgsInSource) {
                    index++;
                    continue;
                }
                lookup.$lookup.pipeline.unshift({
                    $match: {$expr: andCondition},
                });
                indexesToRemove.push(index);
                index++;
            }
            if (indexesToRemove.length > 0) {
                indexesToRemove = indexesToRemove.sort((a, b) => b - a);
                for (const index of indexesToRemove) {
                    match.$match.$expr.$and.splice(index, 1);
                }
                if (match.$match.$expr.$and.length === 0) {
                    indexesToDelete.push(index);
                }
                if (match.$match.$expr.$and.length === 1) {
                    const expr = match.$match.$expr.$and[0];
                    match.$match.$expr = expr;
                }
            }
            index++;
        }
    }
    indexesToDelete = indexesToDelete.sort((a, b) => b - a);
    for (const indexesToDeleteElement of indexesToDelete) {
        lookup.$lookup.pipeline.splice(indexesToDeleteElement, 1);
    }
}
/**
 *
 * @param {import('../types').PipelineFn} lookup
 */
function convertLookupToPipeline(lookup) {
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
                    $eq: [`$${foreignField}`, `$$${letVar}`],
                },
            },
        },
    ];
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
 * @param match
 * @param {string} sourceName
 * @param {string} [explicitKey]
 */
function mapMatchesToExpressionFormat(match, sourceName, explicitKey) {
    const oldKey = Object.keys(match)[0];
    const newKey = explicitKey
        ? explicitKey
        : oldKey.startsWith(`${sourceName}.`)
        ? oldKey.substring(sourceName.length + 1)
        : sourceName;
    const body = match[oldKey];
    const operator = Object.keys(body)[0];
    const value = body[operator];
    return {
        [operator]: [`$${newKey}`, value],
    };
}
