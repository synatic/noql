const {snakeCase} = require('lodash');
const $check = require('check-types');
module.exports = {optimizeJoinAndWhere};
const {get, isEqual, cloneDeep} = require('lodash');

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
    if (!context.optimizeJoins || context.joinHints.includes('nooptimize')) {
        pushToPipeline({
            wasOptimized: false,
            leftOverMatches: [],
        });
        return;
    }
    const originalPipeline = cloneDeep(pipeline);
    const originalPipelineJoin = cloneDeep(pipeLineJoin);
    const originalWherePiece = cloneDeep(wherePiece);

    const lookup = pipeLineJoin.find((p) => Boolean(p.$lookup));
    if (
        wherePiece &&
        (!context.joinHints || !context.joinHints.includes('nooptimize'))
    ) {
        try {
            const result = recursivelyOptimize(
                wherePiece.$match,
                lookup,
                pipeline,
                context
            );
            pushToPipeline(result);
        } catch (err) {
            console.error(err);
            clearObjectAndCopyToIt(pipeline, originalPipeline);
            clearObjectAndCopyToIt(pipeLineJoin, originalPipelineJoin);
            clearObjectAndCopyToIt(wherePiece, originalWherePiece);
            pushToPipeline({
                wasOptimized: false,
                leftOverMatches: [],
            });
        }
    } else {
        pushToPipeline({
            wasOptimized: false,
            leftOverMatches: [],
        });
    }

    /**
     * @param { Pick<ProcessResult,'leftOverMatches'|'wasOptimized'> } result
     */
    function pushToPipeline({leftOverMatches, wasOptimized}) {
        for (const join of pipeLineJoin) {
            pipeline.push(join);
        }
        if (!wasOptimized && wherePiece) {
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
 * @param {MatchType} [parentMatchType]
 * @param {ProcessResult} [parentResult]
 * @returns {ProcessResult} the leftover matches to add to the end
 */
function recursivelyOptimize(
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

    // First, check if we have source matches to determine if we should extract nested structures
    const tempMatches = matches.slice();
    const tempExtracted = extractNestedMatches(tempMatches);
    const tempGetMatches = getMatches(
        tempMatches,
        sourceName,
        destinationName,
        parentMatchType
    );

    // For OR conditions: if we have both source matches and nested ANDs, we must keep
    // them together in the OR structure. Don't extract nested ANDs separately, as that
    // would split the OR semantics. This ensures OR conditions that span both source
    // and destination tables are evaluated together in the lookup pipeline.
    const shouldKeepNestedInOr =
        matchType === '$or' &&
        tempGetMatches.sourceMatches.length > 0 &&
        tempExtracted.ands.length > 0;

    let ands;
    let ors;
    if (shouldKeepNestedInOr) {
        // Don't extract nested ANDs - keep them in the OR structure
        ands = [];
        ors = [];
    } else {
        // Extract nested structures as normal
        const extracted = extractNestedMatches(matches);
        ands = extracted.ands;
        ors = extracted.ors;
    }

    const {
        destinationMatches,
        sourceMatches,
        miscMatches,
        leftOverSourceMatches,
    } = getMatches(matches, sourceName, destinationName, parentMatchType);

    if (matchType === '$and' && ands.length > 0) {
        const res = processMatches(
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
        const res = processMatches(
            ors,
            lookup,
            pipeline,
            context,
            matchType,
            returnResult
        );
        mergeProcessResults(res, returnResult);
    }
    const addSingleForSource =
        sourceMatches.length === 1 &&
        ((matchType === '$or' &&
            !!ands.length &&
            allInSource(ands, sourceName)) ||
            (matchType === '$and' && !!ors.length));

    // For OR conditions with destination matches, we must keep source and destination
    // matches together in the lookup pipeline. Don't split them by processing source
    // matches separately, as that would change the OR semantics.
    const shouldSkipSourceProcessing =
        matchType === '$or' && destinationMatches.length > 0;

    let sourceResults;
    let sourceMatchesToPass = sourceMatches;
    if (shouldSkipSourceProcessing) {
        // Skip processing source matches separately - they'll be combined with
        // destination matches in the lookup pipeline
        sourceResults = newReturnResult();
    } else {
        sourceResults = processSourceMatches(
            sourceMatches,
            matchType,
            pipeline,
            returnResult,
            parentMatchType,
            addSingleForSource
        );
        // After processing, sourceMatches may have been cleared, so use empty array
        sourceMatchesToPass = [];
    }
    mergeProcessResults(sourceResults, returnResult);
    const addSingleForDestination =
        destinationMatches.length === 1 &&
        ((matchType === '$or' && !!ands.length) ||
            (matchType === '$and' && !!ors.length));
    const destinationResults = processDestinationMatches(
        destinationMatches,
        destinationName,
        lookup,
        matchType,
        leftOverSourceMatches.length
            ? sourceMatchesToPass.concat(leftOverSourceMatches)
            : sourceMatchesToPass,
        sourceName,
        returnResult,
        parentMatchType,
        addSingleForDestination
    );
    mergeProcessResults(destinationResults, returnResult);
    if (miscMatches.length > 0) {
        mergeProcessResults(
            {
                wasOptimized: false,
                leftOverMatches: miscMatches,
                lookupPipelineStagesAdded: [],
                pipelineStagesAdded: [],
            },
            returnResult
        );
    }
    if (ands.length > 0) {
        const res = processMatches(
            ands,
            lookup,
            pipeline,
            context,
            matchType,
            returnResult
        );
        mergeProcessResults(res, returnResult);
    }
    if (ors.length > 0) {
        const res = processMatches(
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
 * @param {string} sourceName
 * @returns {boolean}
 */
function allInSource(matches, sourceName) {
    const allKeys = matches.flatMap((match) => {
        return Object.values(match).flatMap((matchValue) => {
            return matchValue.flatMap((v) => Object.keys(v));
        });
    });
    return allKeys.every((key) => {
        return key.startsWith(sourceName);
    });
}

/**
 * Checks if nested AND structures contain destination conditions
 * @param {Record<string,unknown>[]} nestedAnds - Array of nested AND structures
 * @param {string} sourceName - Source table name
 * @param {string} destinationName - Destination table name
 * @returns {boolean}
 */
function nestedAndsContainDestination(nestedAnds, sourceName, destinationName) {
    for (const nestedAnd of nestedAnds) {
        const andConditions = nestedAnd.$and || [];
        for (const condition of andConditions) {
            // Check if condition references destination table
            if (condition.$expr) {
                const exprStr = JSON.stringify(condition.$expr);
                if (
                    exprStr.includes(`${destinationName}.`) ||
                    checkExpressionForString(
                        condition.$expr,
                        `${destinationName}.`,
                        `${sourceName}.`
                    )
                ) {
                    return true;
                }
            } else {
                const keys = Object.keys(condition);
                if (
                    keys.length > 0 &&
                    keys[0].startsWith(`${destinationName}.`)
                ) {
                    return true;
                }
            }
        }
    }
    return false;
}

/**
 * Checks if nested AND structures contain source conditions
 * @param {Record<string,unknown>[]} nestedAnds - Array of nested AND structures
 * @param {string} sourceName - Source table name
 * @param {string} destinationName - Destination table name
 * @returns {boolean}
 */
function nestedAndsContainSource(nestedAnds, sourceName, destinationName) {
    for (const nestedAnd of nestedAnds) {
        const andConditions = nestedAnd.$and || [];
        for (const condition of andConditions) {
            // Check if condition references source table
            if (condition.$expr) {
                if (
                    checkExpressionForString(
                        condition.$expr,
                        `${sourceName}.`,
                        `${destinationName}.`
                    )
                ) {
                    return true;
                }
            } else {
                const keys = Object.keys(condition);
                if (keys.length > 0 && keys[0].startsWith(`${sourceName}.`)) {
                    return true;
                }
            }
        }
    }
    return false;
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
    const combined = andIndexes.concat(orIndexes).sort((a, b) => b - a);
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
function processMatches(
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
        const res = recursivelyOptimize(
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
    clearArray(matches);
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
 * @param {boolean} addSingle
 * @returns {ProcessResult} wasOptimized
 */
function processSourceMatches(
    sourceMatches,
    matchType,
    pipeline,
    currentResult,
    parentMatchType,
    addSingle
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
        returnResult.wasOptimized = true;
        return returnResult;
    }
    let newStage;
    if (addSingle) {
        newStage = {$match: {[matchType]: [...sourceMatches]}};
    } else if (!matchType) {
        if (sourceMatches.length > 1) {
            throw new Error(
                'There were multiple source matches and no match type'
            );
        }
        newStage = {$match: sourceMatches[0]};
    } else {
        if (sourceMatches.length > 1) {
            newStage = {$match: {[matchType]: [...sourceMatches]}};
        } else if (matchType === '$and') {
            newStage = {$match: sourceMatches[0]};
        } else {
            // will be handled in the destination
            return returnResult;
        }
    }
    pipeline.push(newStage);
    returnResult.pipelineStagesAdded.push(newStage);
    clearArray(sourceMatches);
    returnResult.wasOptimized = true;
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
                throw new Error(
                    'there were multiple matching steps while processing source matches'
                );
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
 * @param {boolean} addSingle
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
    parentMatchType,
    addSingle
) {
    const returnResult = newReturnResult();
    if (destinationMatches.length === 0 && leftOverSourceMatches.length === 0) {
        return returnResult;
    }
    // ensures the lookup is in the right format to be optimized
    if (!lookup.$lookup.pipeline) {
        convertLookupToPipeline(lookup);
    } else {
        if (currentResult.lookupPipelineStagesAdded === 0) {
            // only optimize the pipeline if we haven't already done int
            optimizeExistingPipeline(lookup);
        }
    }
    const mappedMatches = destinationMatches
        .map((d) => {
            // Check if this is a nested AND with both source and destination conditions
            if (d.$and) {
                const andConditions = d.$and || [];
                const hasSource = andConditions.some((condition) => {
                    if (condition.$expr) {
                        return checkExpressionForString(
                            condition.$expr,
                            `${sourceName}.`,
                            `${destinationName}.`
                        );
                    }
                    const keys = Object.keys(condition);
                    return keys.length > 0 && keys[0].startsWith(`${sourceName}.`);
                });
                const hasDestination = andConditions.some((condition) => {
                    if (condition.$expr) {
                        return checkExpressionForString(
                            condition.$expr,
                            `${destinationName}.`,
                            `${sourceName}.`
                        );
                    }
                    const keys = Object.keys(condition);
                    return (
                        keys.length > 0 &&
                        keys[0].startsWith(`${destinationName}.`)
                    );
                });
                
                // If nested AND has both source and destination, map each condition appropriately
                if (hasSource && hasDestination) {
                    lookup.$lookup.let = lookup.$lookup.let || {};
                    return {
                        $and: andConditions.map((condition) => {
                            const isSource = condition.$expr
                                ? checkExpressionForString(
                                      condition.$expr,
                                      `${sourceName}.`,
                                      `${destinationName}.`
                                  )
                                : Object.keys(condition)[0]?.startsWith(
                                      `${sourceName}.`
                                  );
                            
                            if (isSource) {
                                // Map source condition with sourceName and set up let variables
                                const field = Object.keys(condition)[0];
                                if (field === '$expr') {
                                    const res = mapMatchesToExpressionFormat(
                                        condition,
                                        sourceName
                                    );
                                    findLetValuesAndModifyExpression(
                                        res,
                                        sourceName,
                                        lookup.$lookup.let
                                    );
                                    return res;
                                }
                                const letVar = snakeCase(field);
                                const explicitKey = `$${letVar}`;
                                lookup.$lookup.let[letVar] = `$${field}`;
                                return mapMatchesToExpressionFormat(
                                    condition,
                                    sourceName,
                                    explicitKey
                                );
                            } else {
                                // Map destination condition with destinationName
                                return mapMatchesToExpressionFormat(
                                    condition,
                                    destinationName
                                );
                            }
                        }),
                    };
                }
            }
            // Default: map with destinationName
            return mapMatchesToExpressionFormat(d, destinationName);
        })
        .concat(
            leftOverSourceMatches.map((d) => {
                lookup.$lookup.let = lookup.$lookup.let || {};
                const field = Object.keys(d)[0];
                if (field === '$expr') {
                    const res = mapMatchesToExpressionFormat(d, sourceName);
                    findLetValuesAndModifyExpression(
                        res,
                        sourceName,
                        lookup.$lookup.let
                    );

                    return res;
                }
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
        returnResult.wasOptimized = true;
        return returnResult;
    }
    const newStage = getStage();
    lookup.$lookup.pipeline.unshift(newStage);
    returnResult.wasOptimized = true;
    returnResult.lookupPipelineStagesAdded.push(newStage);
    return returnResult;

    /**
     *
     */
    function getStage() {
        let newStage;
        if (addSingle) {
            newStage = {
                $match: {$expr: {[matchType]: [...mappedMatches]}},
            };
        } else if (
            !$check.assigned(matchType) ||
            (matchType === '$and' && mappedMatches.length === 1)
        ) {
            // only 1 condition
            const expr = mappedMatches[0];
            newStage = {
                $match: {$expr: expr},
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
            throw new Error(
                'There were multiple added pipeline stage targets, unable to determine which one to use'
            );
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
            throw new Error(
                'There were multiple pipeline targets, unable to determine which one to use'
            );
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
    // optimize the existing pipeline
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
 * @param {MatchType} parentMatchType
 */
function getMatches(matches, sourceName, destinationName, parentMatchType) {
    const sourceMatches = [];
    const destinationMatches = [];

    // Filter out source matches
    for (let i = matches.length - 1; i >= 0; i--) {
        const m = matches[i];
        if (m.$expr) {
            if (
                checkExpressionForString(
                    m.$expr,
                    `${sourceName}.`,
                    `${destinationName}.`
                )
            ) {
                sourceMatches.unshift(m);
                matches.splice(i, 1);
            }
        } else if (Object.keys(m)[0].startsWith(`${sourceName}.`)) {
            sourceMatches.unshift(m);
            matches.splice(i, 1);
        } else if (m.$and) {
            // Check nested AND structures for source conditions
            // BUT: if it also has destination conditions, don't categorize as source
            // (it needs to be processed in the lookup pipeline)
            const andConditions = m.$and || [];
            const hasSource = andConditions.some((condition) => {
                if (condition.$expr) {
                    return checkExpressionForString(
                        condition.$expr,
                        `${sourceName}.`,
                        `${destinationName}.`
                    );
                }
                const keys = Object.keys(condition);
                return keys.length > 0 && keys[0].startsWith(`${sourceName}.`);
            });
            const hasDestination = andConditions.some((condition) => {
                if (condition.$expr) {
                    return checkExpressionForString(
                        condition.$expr,
                        `${destinationName}.`,
                        `${sourceName}.`
                    );
                }
                const keys = Object.keys(condition);
                return (
                    keys.length > 0 &&
                    keys[0].startsWith(`${destinationName}.`)
                );
            });
            // Only categorize as source if it has source conditions but NOT destination conditions
            if (hasSource && !hasDestination) {
                sourceMatches.unshift(m);
                matches.splice(i, 1);
            }
        }
    }

    // Filter out destination matches
    for (let i = matches.length - 1; i >= 0; i--) {
        const m = matches[i];
        if (m.$expr) {
            if (
                checkExpressionForString(
                    m.$expr,
                    `${destinationName}.`,
                    `${sourceName}.`
                )
            ) {
                destinationMatches.unshift(m);
                matches.splice(i, 1);
            }
        } else if (Object.keys(m)[0].startsWith(`${destinationName}.`)) {
            destinationMatches.unshift(m);
            matches.splice(i, 1);
        } else if (m.$and) {
            // Check nested AND structures for destination conditions
            const andConditions = m.$and || [];
            const hasDestination = andConditions.some((condition) => {
                if (condition.$expr) {
                    return checkExpressionForString(
                        condition.$expr,
                        `${destinationName}.`,
                        `${sourceName}.`
                    );
                }
                const keys = Object.keys(condition);
                return (
                    keys.length > 0 && keys[0].startsWith(`${destinationName}.`)
                );
            });
            if (hasDestination) {
                destinationMatches.unshift(m);
                matches.splice(i, 1);
            }
        }
    }

    // The remaining matches are misc matches
    const miscMatches = matches.slice();

    if (parentMatchType && destinationMatches.length) {
        return {
            sourceMatches: [],
            destinationMatches: destinationMatches,
            miscMatches,
            leftOverSourceMatches: sourceMatches,
        };
    }
    return {
        sourceMatches,
        destinationMatches,
        miscMatches,
        leftOverSourceMatches: [],
    };
}

/**
 *
 * @param {Record<string,unknown>} expression
 * @param {string} mustContain
 * @param {string} mustNotContain
 */
function checkExpressionForString(expression, mustContain, mustNotContain) {
    let expressionArray;
    if (expression.$and) {
        expressionArray = expression.$and;
    } else if (expression.$or) {
        expressionArray = expression.$or;
    } else {
        return false;
    }
    const containsRequired = expressionContains(expressionArray, mustContain);
    const containsNotAllowed = expressionContains(
        expressionArray,
        mustNotContain
    );
    if (containsNotAllowed) {
        return false;
    }
    return containsRequired;
}

/**
 *
 * @param {Record<string,unknown>[]} expression
 * @param {string} searchString
 * @returns {boolean}
 */
function expressionContains(expression, searchString) {
    return expression.some((e) => {
        if (typeof e === 'string') {
            return (
                e.startsWith(searchString) || e.startsWith(`$${searchString}`)
            );
        }
        if ($check.object(e)) {
            const values = Object.values(e).flat();
            return expressionContains(values, searchString);
        }
    });
}

/**
 *
 * @param {Record<string,unknown>} expression
 * @param {string} sourceName
 * @param {Record<string,unknown>} letObj
 */
function findLetValuesAndModifyExpression(expression, sourceName, letObj) {
    const key = Object.keys(expression)[0];
    const values = Object.values(expression)[0];
    if ($check.array(values)) {
        values.forEach((value) => {
            if ($check.object(value)) {
                findLetValuesAndModifyExpression(value, sourceName, letObj);
            }
        });
        return;
    }
    if ($check.string(values) && values.startsWith(`$${sourceName}`)) {
        const letVar = snakeCase(values);
        letObj[letVar] = values;
        expression[key] = `$${letVar}`;
    }
}
/**
 *
 * @param match
 * @param {string} sourceName
 * @param {string} [explicitKey]
 */
function mapMatchesToExpressionFormat(match, sourceName, explicitKey) {
    const oldKey = Object.keys(match)[0];
    if (oldKey === '$expr') {
        // already an expression
        return match[oldKey];
    }
    if (oldKey === '$and') {
        // Handle nested AND structures - recursively map each condition
        const andConditions = match[oldKey] || [];
        return {
            $and: andConditions.map((condition) =>
                mapMatchesToExpressionFormat(condition, sourceName, explicitKey)
            ),
        };
    }
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
        wasOptimized: false,
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
    if (source.wasOptimized) {
        destination.wasOptimized = true;
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

/**
 *
 * @param {Record<string,unknown>} objectToClearAndCopyTo
 * @param {Record<string,unknown>} objectToCopyFrom
 * @returns {void}
 */
function clearObjectAndCopyToIt(objectToClearAndCopyTo, objectToCopyFrom) {
    // eslint-disable-next-line guard-for-in
    for (const key in objectToClearAndCopyTo) {
        delete objectToClearAndCopyTo[key];
    }
    // eslint-disable-next-line guard-for-in
    for (const key in objectToCopyFrom) {
        objectToClearAndCopyTo[key] = objectToCopyFrom[key];
    }
}
