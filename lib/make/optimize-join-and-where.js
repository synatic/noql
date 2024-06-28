const {snakeCase} = require('lodash');
const $check = require('check-types');
module.exports = {optimizeJoinAndWhere};
const {get, isEqual} = require('lodash');

/**
 * @typedef {'$and' | '$or' | null} MatchType
 * @typedef {import('../types').OptimizationProcessResult} ProcessResult
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
    // const wasOptimised = false;
    // const leftOverMatches = [];
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
 * @param {MatchType} parentMatchType
 * @param {ProcessResult} parentResult
 * @returns {ProcessResult} the leftover matches to add to the end
 */
function recursivelyOptimise(
    match,
    lookup,
    pipeline,
    context,
    parentMatchType,
    parentResult
) {
    const returnResult = parentResult || newReturnResult();
    if (!match) {
        return returnResult;
    }
    const from = context.fullAst.ast.from;
    const sourceName = from[0].as || from[0].table;
    const destinationName = from[1].as || from[1].table;
    /** @type {MatchType} */
    const matchType = match.$and ? '$and' : match.$or ? '$or' : null;
    const matches = match[matchType] ? match[matchType] : [match];
    const {ands, ors} = extractNestedMatches(matches);
    const {destinationMatches, sourceMatches, miscMatches} = getMatches(
        matches,
        sourceName,
        destinationName
    );
    if (matchType === '$and' && ands.length > 0) {
        const res = processMatchesForAnds(
            ands,
            lookup,
            pipeline,
            context,
            matchType,
            returnResult
        );
        mergeProcessResults(res, returnResult);
    }
    if (matchType === '$or' && ors.length > 0) {
        const res = processMatchesForOrs(
            ors,
            lookup,
            pipeline,
            context,
            matchType,
            returnResult
        );
        mergeProcessResults(res, returnResult);
    }
    const addSingleOr =
        sourceMatches.length === 1 && matchType === '$or' && ands.length;
    const sourceResults = processSourceMatches(
        sourceMatches,
        matchType,
        pipeline,
        returnResult,
        parentMatchType,
        addSingleOr
    );
    mergeProcessResults(sourceResults, returnResult);
    const destinationResults = processDestinationMatches(
        destinationMatches,
        destinationName,
        lookup,
        matchType,
        sourceMatches,
        sourceName,
        returnResult,
        parentMatchType
    );
    mergeProcessResults(destinationResults, returnResult);
    if (miscMatches.length > 0) {
        mergeProcessResults(
            {
                wasOptimised: false,
                leftOverMatches: miscMatches,
                lookupPipelineStagesAdded: [],
                pipelineStagesAdded: [],
            },
            returnResult
        );
    }
    if (matchType !== '$and' && ands.length > 0) {
        const res = processMatchesForAnds(
            ands,
            lookup,
            pipeline,
            context,
            matchType,
            returnResult
        );
        mergeProcessResults(res, returnResult);
    }
    if (matchType !== '$or' && ors.length > 0) {
        const res = processMatchesForOrs(
            ors,
            lookup,
            pipeline,
            context,
            matchType,
            returnResult
        );
        mergeProcessResults(res, returnResult);
    }
    return returnResult;
}

/**
 *
 * @param {Record<string,unknown>[]} matches
 */
function extractNestedMatches(matches) {
    const andIndexes = [];
    const orIndexes = [];
    matches.forEach((m, index) => {
        if (Object.keys(m)[0] === '$and') {
            andIndexes.push(index);
        }
        if (Object.keys(m)[0] === '$or') {
            orIndexes.push(index);
        }
    });
    const ands = [];
    for (const andIndex of andIndexes) {
        const value = matches[andIndex];
        ands.push(value);
    }
    const ors = [];
    for (const orIndex of orIndexes) {
        const value = matches[orIndex];
        ors.push(value);
    }
    const combined = ands.concat(orIndexes).sort((a, b) => b - a);
    for (const index of combined) {
        matches.splice(index, 1);
    }
    return {
        ands,
        ors,
    };
}
/**
 *
 * @param {Record<string,unknown>[]} matches
 * @param {import('../types').PipelineFn} lookup
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 * @param {MatchType} parentMatchType
 * @param {ProcessResult} parentResultType
 * @returns {ProcessResult}
 */
function processMatchesForAnds(
    matches,
    lookup,
    pipeline,
    context,
    parentMatchType,
    parentResultType
) {
    const returnResult = newReturnResult();
    const results = [];
    for (const match of matches) {
        const res = recursivelyOptimise(
            match,
            lookup,
            pipeline,
            context,
            parentMatchType,
            parentResultType
        );
        results.push(res);
    }
    results.forEach((result) => {
        mergeProcessResults(result, returnResult);
    });
    return returnResult;
}

/**
 *
 * @param {Record<string,unknown>[]} matches
 * @param {import('../types').PipelineFn} lookup
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 * @param {MatchType} parentMatchType
 * @param {ProcessResult} parentResultType
 * @returns {ProcessResult}
 */
function processMatchesForOrs(
    matches,
    lookup,
    pipeline,
    context,
    parentMatchType,
    parentResultType
) {
    const returnResult = newReturnResult();
    const results = [];
    for (const match of matches) {
        const res = recursivelyOptimise(
            match,
            lookup,
            pipeline,
            context,
            parentMatchType,
            parentResultType
        );
        results.push(res);
    }
    results.forEach((result) => {
        mergeProcessResults(result, returnResult);
    });
    return returnResult;
}

/**
 *
 * @param {Record<string,unknown>[]} sourceMatches
 * @param {MatchType} matchType
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').PipelineFn} lookup
 * @param {ProcessResult} currentResult
 * @param {MatchType} parentMatchType
 * @param {boolean} addSingleOr
 * @returns {ProcessResult} wasOptimised
 */
function processSourceMatches(
    sourceMatches,
    matchType,
    pipeline,
    currentResult,
    parentMatchType,
    addSingleOr
) {
    const returnResult = newReturnResult();
    if (sourceMatches.length === 0) {
        return returnResult;
    }
    const target = findTarget();
    if (target) {
        if (!parentMatchType || parentMatchType === matchType) {
            target[matchType].push(...sourceMatches);
        } else {
            target[parentMatchType].push({[matchType]: [...sourceMatches]});
        }
        clearArray(sourceMatches);
        returnResult.wasOptimised = true;
        return returnResult;
    }
    let newStage;
    if (!matchType) {
        if (sourceMatches.length > 1) {
            throw new Error('TODO 1');
        }
        newStage = {$match: sourceMatches[0]};
    } else {
        if (sourceMatches.length > 1) {
            newStage = {$match: {[matchType]: [...sourceMatches]}};
        } else if (matchType === '$and') {
            newStage = {$match: sourceMatches[0]};
        } else {
            if (addSingleOr) {
                newStage = {$match: {[matchType]: [...sourceMatches]}};
            } else {
                // will be handled in the destination
                return returnResult;
            }
        }
    }
    pipeline.push(newStage);
    returnResult.pipelineStagesAdded.push(newStage);
    clearArray(sourceMatches);
    returnResult.wasOptimised = true;
    return returnResult;

    /**
     *
     */
    function findTarget() {
        if (currentResult.pipelineStagesAdded.length === 0) {
            return;
        }
        const matchingSteps = currentResult.pipelineStagesAdded.filter(
            (stage) => !!get(stage, `$match.${parentMatchType || matchType}`)
        );
        if (matchingSteps.length > 0) {
            if (matchingSteps.length === 1) {
                return matchingSteps[0].$match;
            } else {
                throw new Error('TODO 2');
            }
        }
    }
}

/**
 *
 * @param {Record<string,unknown>[]} destinationMatches
 * @param {string} destinationName
 * @param {import('../types').PipelineFn} lookup
 * @param {MatchType} matchType
 * @param {Record<string,unknown>[]} leftOverSourceMatches
 * @param {string} sourceName
 * @param {ProcessResult} currentResult
 * @param {MatchType} parentMatchType
 * @returns {ProcessResult}
 */
function processDestinationMatches(
    destinationMatches,
    destinationName,
    lookup,
    matchType,
    leftOverSourceMatches,
    sourceName,
    currentResult,
    parentMatchType
) {
    const returnResult = newReturnResult();
    if (destinationMatches.length === 0 && leftOverSourceMatches.length === 0) {
        return returnResult;
    }
    // ensures the lookup is in the right format to be optimised
    if (!lookup.$lookup.pipeline) {
        convertLookupToPipeline(lookup);
    } else {
        if (currentResult.lookupPipelineStagesAdded === 0) {
            // only optimise the pipeline if we haven't already done int
            optimizeExistingPipeline(lookup);
        }
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

    const target = findTarget();
    if (target) {
        if (parentMatchType && matchType && parentMatchType !== matchType) {
            target.push({[matchType]: [...mappedMatches]});
        } else {
            target.push(...mappedMatches);
        }
        returnResult.wasOptimised = true;
        return returnResult;
    }
    const newStage = getStage();
    lookup.$lookup.pipeline.unshift(newStage);
    returnResult.wasOptimised = true;
    returnResult.lookupPipelineStagesAdded.push(newStage);
    return returnResult;

    /**
     *
     */
    function getStage() {
        let newStage;
        if (
            !$check.assigned(matchType) ||
            (matchType === '$and' && mappedMatches.length === 1)
        ) {
            // only 1 condition
            newStage = {
                $match: {$expr: mappedMatches[0]},
            };
        } else if (matchType === '$and') {
            // if matchType is AND, can add it on to existing expression after converting
            newStage = {
                $match: {$expr: {$and: mappedMatches}},
            };
        } else {
            // matchType is $or
            newStage = {
                $match: {$expr: {$or: mappedMatches}},
            };
        }
        return newStage;
    }

    /**
     *
     */
    function findTarget() {
        const addedPipelineStageTarget =
            currentResult.lookupPipelineStagesAdded.filter(
                (stage) =>
                    !!get(stage, `$match.$expr.${parentMatchType || matchType}`)
            );
        if (addedPipelineStageTarget.length > 0) {
            if (addedPipelineStageTarget.length === 1) {
                return addedPipelineStageTarget[0].$match.$expr[
                    parentMatchType || matchType
                ];
            }
            throw new Error('TODO 3');
        }
        const pipelineTarget = lookup.$lookup.pipeline.filter(
            (stage) =>
                !!get(stage, `$match.$expr.${parentMatchType || matchType}`)
        );
        if (pipelineTarget.length > 0) {
            if (pipelineTarget.length === 1) {
                return pipelineTarget[0].$match.$expr[
                    parentMatchType || matchType
                ];
            }
            throw new Error('TODO 4');
        }
        return null;
    }
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

/**
 * @returns {ProcessResult}
 */
function newReturnResult() {
    return {
        wasOptimised: false,
        leftOverMatches: [],
        pipelineStagesAdded: [],
        lookupPipelineStagesAdded: [],
    };
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
        const exists = destination.leftOverMatches.some((match) =>
            isEqual(match, leftOverMatch)
        );
        if (!exists) {
            destination.leftOverMatches.push(leftOverMatch);
        }
    }
    for (const stage of source.pipelineStagesAdded) {
        const exists = destination.pipelineStagesAdded.some((match) =>
            isEqual(match, stage)
        );
        if (!exists) {
            destination.pipelineStagesAdded.push(stage);
        }
    }
    for (const stage of source.lookupPipelineStagesAdded) {
        const exists = destination.lookupPipelineStagesAdded.some((match) =>
            isEqual(match, stage)
        );
        if (!exists) {
            destination.lookupPipelineStagesAdded.push(stage);
        }
    }
}
/**
 *
 * @param {any[]} array
 */
function clearArray(array) {
    array.splice(0, array.length);
}