const $check = require('check-types');

/**
 *
 * @param {object} matchCondition - the query/match to check
 * @param options
 */
function findIndexesOnMatch(matchCondition, options = {}) {
    const getIndexFromSegment = (segment, fromAnd = false) => {
        let indexes = [];

        if (!$check.object(segment)) {
            return [];
        }

        const index = {};

        for (const k in segment) {
            // eslint-disable-next-line no-prototype-builtins
            if (!segment.hasOwnProperty(k)) {
                continue;
            }
            if (k.startsWith('$')) {
                if (k === '$and') {
                    for (const andElem of segment[k]) {
                        indexes = indexes.concat(
                            getIndexFromSegment(andElem, true)
                        );
                    }
                } else if (k === '$eq') {
                    index[k] = 1;
                }
            } else {
                index[k] = 1;
            }
        }
        indexes.push(index);
        return indexes;
    };

    return getIndexFromSegment(matchCondition);
}

function findIndexesOnPipeline(pipeline, collection = null, options = {}) {}

/**
 * Finds potential indexes given a query or aggregate pipeline
 *
 * @param {import('./types').ParsedQueryOrAggregate} findIndexesOn  - the find or aggregate pipeline
 * @param {object} [options] - the AST options
 * @returns {object[]}
 * @throws
 */
function findPotentialIndexes(findIndexesOn, options = {}) {
    if (!findIndexesOn) {
        return [];
    }

    if (findIndexesOn.type === 'query') {
        return [
            {
                collection: findIndexesOn.collection,
                indexes: findIndexesOnMatch(findIndexesOn.query, options),
            },
        ];
    } else if (findIndexesOn.type === 'aggregate') {
        return findIndexesOnPipeline(findIndexesOn, null, options);
    } else {
        throw new Error('Invalid Parsed Query or Aggregate for index search');
    }
}

module.exports = {findPotentialIndexes, findIndexesOnMatch};
