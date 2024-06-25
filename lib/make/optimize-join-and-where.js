const {snakeCase} = require('lodash');
module.exports = {optimizeJoinAndWhere};

/**
 * @typedef {'$and' | '$or' | null} MatchType
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
            pipeline.push({$match: leftOverMatches});
        }
    }
}

/**
 *
 * @param {Record<string,unknown>} match
 * @param {import('../types').PipelineFn} lookup
 * @param {import('../types').PipelineFn[]} pipeline
 * @param {import('../types').NoqlContext} context
 * @returns {{wasOptimised:boolean, leftOverMatches: Record<string,unknown>[]}} the leftover matches to add to the end
 */
function recursivelyOptimise(match, lookup, pipeline, context) {
    let wasOptimised = false;
    let leftOverMatches = [];
    if (!match) {
        return {wasOptimised, leftOverMatches};
    }
    const from = context.fullAst.ast.from;
    const sourceName = from[0].as || from[0].table;
    const destinationName = from[1].as || from[1].table;
    /** @type {MatchType} */
    const matchType = match.$and ? '$and' : match.$or ? '$or' : null;
    const matches = match[matchType] ? match[matchType] : [match];
    const {destinationMatches, sourceMatches, miscMatches} = getMatches(
        matches,
        sourceName,
        destinationName
    );
    wasOptimised = processSourceMatches(sourceMatches, matchType, pipeline);
    const destinationOptimised = processDestinationMatches(
        destinationMatches,
        destinationName,
        lookup,
        matchType,
        sourceMatches,
        sourceName
    );
    if (destinationOptimised) {
        wasOptimised = true;
    }
    if (miscMatches.length > 0) {
        leftOverMatches = leftOverMatches.concat(miscMatches);
    }
    return {wasOptimised, leftOverMatches};
}

/**
 *
 * @param {Record<string,unknown>[]} sourceMatches
 * @param {MatchType} matchType
 * @param {import('../types').PipelineFn[]} pipeline
 * @returns {boolean} wasOptimised
 */
function processSourceMatches(sourceMatches, matchType, pipeline) {
    if (sourceMatches.length === 0) {
        return false;
    }
    if (sourceMatches.length > 1) {
        pipeline.push({$match: {[matchType]: sourceMatches}});
        clearArray(sourceMatches);
        return true;
    } else if (matchType === '$and') {
        pipeline.push({$match: sourceMatches[0]});
        clearArray(sourceMatches);
        return true;
    }
    return false;
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
 * @returns {boolean}
 */
function processDestinationMatches(
    destinationMatches,
    destinationName,
    lookup,
    matchType,
    leftOverSourceMatches,
    sourceName
) {
    if (destinationMatches.length === 0) {
        return false;
    }
    // ensures the lookup is in the right format to be optimised
    if (!lookup.$lookup.pipeline) {
        convertLookupToPipeline(lookup);
    } else {
        const matches = lookup.$lookup.pipeline.filter((p) => !!p.$match);
        // optimise the existing pipeline
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
                    if (match.$match.$expr.$and.length === 1) {
                        const expr = match.$match.$expr.$and[0];
                        match.$match.$expr = expr;
                    }
                }
            }
        }
    }
    const mappedMatches = destinationMatches
        .map((d) => {
            return mapMatchesToAndFormat(d, destinationName);
        })
        .concat(
            leftOverSourceMatches.map((d) => {
                lookup.$lookup.let = lookup.$lookup.let || {};
                const field = Object.keys(d)[0];
                const letVar = snakeCase(field);
                const explicitKey = `$${letVar}`;
                lookup.$lookup.let[letVar] = `$${field}`;
                return mapMatchesToAndFormat(d, sourceName, explicitKey);
            })
        );

    if (!matchType || mappedMatches.length === 1) {
        // only 1 condition
        lookup.$lookup.pipeline.unshift({
            $match: {$expr: mappedMatches[0]},
        });
        return true;
    }
    if (matchType === '$and') {
        // if matchType is AND, can add it on to existing expression after converting
        lookup.$lookup.pipeline.unshift({
            $match: {$expr: {$and: mappedMatches}},
        });
        return true;
    }
    // matchType is $or
    lookup.$lookup.pipeline.unshift({
        $match: {$expr: {$or: mappedMatches}},
    });
    return true;
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
 * @param d
 * @param {string} name
 * @param {string} [explicitKey]
 */
function mapMatchesToAndFormat(d, name, explicitKey) {
    const key = Object.keys(d)[0];
    const newKey = explicitKey
        ? explicitKey
        : key.startsWith(`${name}.`)
        ? key.substring(name.length + 1)
        : name;
    const match = d[key];
    const operator = Object.keys(match)[0];
    const value = match[operator];
    return {
        [operator]: [`$${newKey}`, value],
    };
}
