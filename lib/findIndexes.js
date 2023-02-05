const $check = require('check-types');

/**
 * Finds potential indexes on a match condition
 *
 * @param {object} matchCondition - the query/match to check
 * @param {object}  [options] - options object
 * @returns {object[]}
 */
function findIndexesOnMatch(matchCondition, options = {}) {
    const getIndexFromSegment = (segment, fromAnd) => {
        let indexes = [];

        if (!$check.object(segment)) {
            return [];
        }

        const index = fromAnd || {};

        for (const k in segment) {
            // eslint-disable-next-line no-prototype-builtins
            if (!segment.hasOwnProperty(k)) {
                continue;
            }
            if (k.startsWith('$')) {
                if (k === '$and') {
                    for (const andElem of segment[k]) {
                        getIndexFromSegment(andElem, index);
                    }
                } else if (
                    ['$eq', '$ne', '$gt', '$gte', '$in', '$nin'].includes(k)
                ) {
                    index[k] = 1;
                } else if (['$lt', '$lte'].includes(k)) {
                    index[k] = -1;
                } else if ($check.array(segment[k])) {
                    for (const elem of segment[k]) {
                        indexes = indexes.concat(
                            getIndexFromSegment(elem, null)
                        );
                    }
                } else {
                    indexes = indexes.concat(
                        getIndexFromSegment(segment[k], null)
                    );
                }
            } else {
                index[k] = 1;
            }
        }
        indexes.push(index);
        return indexes;
    };

    return getIndexFromSegment(matchCondition).filter(
        (i) => !$check.emptyObject(i)
    );
}

/**
 * Find indexes on an aggregate pipeline
 *
 * @param pipeline
 * @param {string} collection - the collection name this pipeline is part of
 * @param {object} [options] - the options
 */
function findIndexesOnPipeline(pipeline, collection, options = {}) {
    const indexes = {
        [collection]: [],
    };

    const addIndex = (collection, index) => {
        if (!indexes[collection]) {
            indexes[collection] = [];
        }
        if ($check.object(index)) {
            indexes[collection].push(index);
        } else if ($check.array(index)) {
            indexes[collection] = indexes[collection].concat(index);
        }
    };

    for (const item of pipeline) {
        const stageName = Object.keys(item)[0];
        const stage = item[stageName];
        if (stageName === '$lookup' && !stage.pipeline) {
            addIndex(stage.from, {[stage.foreignField]: 1});
        } else if (stageName === '$match') {
            addIndex(collection, findIndexesOnMatch(stage));
        }
    }

    return Object.keys(indexes).reduce((a, v) => {
        a.push({
            collection: v,
            indexes: indexes[v],
        });
        return a;
    }, []);
}

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
        return findIndexesOnPipeline(
            findIndexesOn.pipeline,
            findIndexesOn.collections[0],
            options
        );
    } else {
        throw new Error('Invalid Parsed Query or Aggregate for index search');
    }
}

module.exports = {
    findPotentialIndexes,
    findIndexesOnMatch,
    findIndexesOnPipeline,
};
