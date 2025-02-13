const $copy = require('clone-deep');
const $json = require('@synatic/json-magic');
const $hash = require('object-hash');
const projectIsRoot = require('./projectIsRoot');
const projectIsSimple = require('./projectIsSimple');
const lodash = require('lodash');
const arraySequenceIndexOf = require('./arraySequenceIndexOf');
const $check = require('check-types');
/**
 * Extracts and returns the names of the stages in a MongoDB aggregation pipeline.
 * @param {Array<object>} mongoAggregate - An array representing the MongoDB aggregation pipeline,
 * where each element is an object containing a stage name and its definition.
 * @returns {Array<string>} An array of stage names extracted from the pipeline.
 */
function _getStageNames(mongoAggregate) {
    return mongoAggregate.map((stage) => Object.keys(stage)[0]);
}

/**
 * Modifies a MongoDB aggregation pipeline by identifying and replacing stages that match a given pattern.
 * @param {Array} mongoAggregate - The original MongoDB aggregation pipeline, represented as an array of stages.
 * @param {Array} pattern - An array representing the sequence of stage names to be matched within the pipeline.
 * @param {Function} fixFunction - A callback function that receives the matched stages as input and returns the replacement stages.
 * @param {object} [options] - Optional settings for the function.
 * @param {boolean} [options.copy] - Whether to create a copy of the pipeline before applying changes. Defaults to false.
 * @returns {Array} - A new MongoDB aggregation pipeline with the matched stages replaced, or the original pipeline if no matches were found.
 */
function _patternFixer(mongoAggregate, pattern, fixFunction, options = {}) {
    let newAggregate = options.copy ? $copy(mongoAggregate) : mongoAggregate;
    let sequenceIndex = arraySequenceIndexOf(
        pattern,
        _getStageNames(newAggregate),
        0,
        (a, b) => a === b
    );

    // just in case something goes awry
    let iterations = 0;

    while (sequenceIndex > -1 && iterations < 50) {
        iterations++;
        const fixedValues = fixFunction(
            newAggregate.slice(sequenceIndex, sequenceIndex + pattern.length)
        );
        if (fixedValues && fixedValues.length > 0) {
            newAggregate = (
                sequenceIndex > 0 ? newAggregate.slice(0, sequenceIndex) : []
            )
                .concat(fixedValues)
                .concat(newAggregate.slice(sequenceIndex + pattern.length));

            sequenceIndex = arraySequenceIndexOf(
                pattern,
                _getStageNames(newAggregate),
                0,
                (a, b) => a === b
            );
        } else {
            sequenceIndex = arraySequenceIndexOf(
                pattern,
                _getStageNames(newAggregate),
                sequenceIndex + 1,
                (a, b) => a === b
            );
        }
    }
    return newAggregate;
}

/**
 * Modifies the properties and values within an object by updating keys and references
 * based on a given reference key.
 * @param {object} value - The object whose keys and values will be modified.
 * @param {string} referenceKey - The reference string used to identify and update
 * keys and values within the object.
 * @returns {object} - A new object with updated keys and values according to the
 * specified reference key.
 */
function _changeReference(value, referenceKey) {
    return _changeReferenceValue(
        _changeReferenceKey(value, referenceKey),
        referenceKey
    );
}

function _changeReferenceKey(value, referenceKey) {
    let newVal = $copy(value);
    newVal = $json.renameKey(newVal, (key) => {
        if (key.startsWith && key.startsWith(referenceKey + '.')) {
            return key.substring(referenceKey.length + 1);
        } else {
            return key;
        }
    });
    return newVal;
}

function _changeReferenceValue(value, referenceKey) {
    let newVal = $copy(value);
    newVal = $json.changeValue(newVal, (value) => {
        if (
            value &&
            value.startsWith &&
            value.startsWith('$' + referenceKey + '.')
        ) {
            return '$' + value.substring(referenceKey.length + 2);
        } else {
            return value;
        }
    });
    return newVal;
}

// pattern match and fix functions
const _patternsToFix = [
    {
        name: 'removeUnneededProjectRootProject',
        description:
            'Removes root project when followed by a project since setting it to root then using the project is unneeded',
        pattern: ['$project', '$project'],
        fixerFn: (stages) => {
            const testProject1Key = Object.keys(stages[0]['$project'])[0];
            if (projectIsRoot(stages[0]) && !projectIsRoot(stages[1])) {
                const project = stages[1]['$project'];
                const newProject = _changeReferenceValue(
                    project,
                    testProject1Key
                );

                return [{$project: newProject}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeUnneededProjectRootSetBack',
        description:
            'Removes redundant project when start and end project are set roots.',
        pattern: ['$project', '$project', '$project'],
        fixerFn: (stages) => {
            const testProject1Key = Object.keys(stages[0]['$project'])[0];
            const testProject2 = stages[2]['$project'];

            if (
                projectIsRoot(stages[0]) &&
                !projectIsRoot(stages[1]) &&
                projectIsRoot(stages[2])
            ) {
                const project = stages[1]['$project'];
                const newProject = _changeReferenceValue(
                    project,
                    testProject1Key
                );

                return [{$project: newProject}, {$project: testProject2}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeUnneededProjectRootWithMatch',
        description: 'Removes redundant project preceeding a match',
        pattern: ['$project', '$match', '$project', '$project', '$project'],
        fixerFn: (stages) => {
            const testProject1Key = Object.keys(stages[0]['$project'])[0];
            const lastProject = stages[4]['$project'];

            if (projectIsRoot(stages[0]) && projectIsRoot(stages[4])) {
                const newMatch = _changeReference(
                    stages[1]['$match'],
                    testProject1Key
                );
                const newProject1 = _changeReferenceValue(
                    stages[2]['$project'],
                    testProject1Key
                );
                return [
                    {$match: newMatch},
                    {$project: newProject1},
                    stages[3],
                    {$project: lastProject},
                ];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeUnneededProjectWithGroup',
        description: 'Removes redundant projects with groups',
        pattern: ['$project', '$match', '$project', '$project', '$group'],
        fixerFn: (stages) => {
            const testProject1Key = Object.keys(stages[0]['$project'])[0];
            const testProject2Key = Object.keys(stages[3]['$project'])[0];

            if (projectIsRoot(stages[0]) && projectIsRoot(stages[3])) {
                const newMatch = _changeReference(
                    stages[1]['$match'],
                    testProject1Key
                );
                const newProject = _changeReferenceValue(
                    stages[2]['$project'],
                    testProject1Key
                );
                const newGroup = _changeReferenceValue(
                    stages[4]['$group'],
                    testProject2Key
                );

                return [
                    {$match: newMatch},
                    {$project: newProject},
                    {$group: newGroup},
                ];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeUnneededProjectRootWithGroup',
        description: 'Removes redundant root project before a group',
        pattern: ['$project', '$group'],
        fixerFn: (stages) => {
            const testProject1Key = Object.keys(stages[0]['$project'])[0];

            if (projectIsRoot(stages[0])) {
                const newGroup = _changeReferenceValue(
                    stages[1]['$group'],
                    testProject1Key
                );

                return [{$group: newGroup}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeUnneededProjectRootWithSort',
        description: 'Removes redundant project root stage before sort',
        pattern: ['$project', '$sort', '$project'],
        fixerFn: (stages) => {
            if (projectIsRoot(stages[0])) {
                const testProject1Key = Object.keys(stages[0]['$project'])[0];
                const newSort = _changeReferenceKey(
                    stages[1]['$sort'],
                    testProject1Key
                );
                const newProject = _changeReferenceValue(
                    stages[2]['$project'],
                    testProject1Key
                );

                return [{$sort: newSort}, {$project: newProject}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeDuplicateProjects',
        description: 'Removes redundant projects when theyre both simple',
        pattern: ['$project', '$project'],
        fixerFn: (stages) => {
            if (projectIsSimple(stages[1])) {
                const project1 = stages[0]['$project'];
                const project2 = stages[1]['$project'];
                const project2Keys = Object.keys(project2);
                const newProject = {};

                for (const project2Key of project2Keys) {
                    const project2Val = project2[project2Key];
                    const project2KeyVal =
                        project2Val.startsWith && project2Val.startsWith('$')
                            ? project2Val.substring(1)
                            : null;

                    // if it's still not a proper key, exit
                    if (!project2KeyVal || !project1[project2KeyVal]) {
                        return null;
                    }

                    newProject[project2Key] = project1[project2KeyVal];
                }

                return [{$project: newProject}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeDuplicateProjectsWithSort',
        description: 'Removes redundant projects when theyre both simple',
        pattern: ['$project', '$sort', '$project'],
        fixerFn: (stages) => {
            if (lodash.isEqual(stages[0]['$project'], stages[2]['$project'])) {
                return [stages[0], stages[1]];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeDuplicateProjectsWithMatch',
        description: 'Removes redundant projects when theyre both simple',
        pattern: ['$project', '$match', '$project'],
        fixerFn: (stages) => {
            if (lodash.isEqual(stages[0]['$project'], stages[2]['$project'])) {
                return [stages[0], stages[1]];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeUnneededProjectBeforeGroup',
        description: 'Removes redundant project before group if its simple',
        pattern: ['$project', '$group'],
        fixerFn: (stages) => {
            if (projectIsSimple(stages[0])) {
                const project = stages[0]['$project'];
                const group = $copy(stages[1]['$group']);

                const newGroup = $json.changeValue(group, (value) => {
                    if (value?.startsWith && value.startsWith('$')) {
                        const valKey = value.substring(1);
                        if (!project[valKey]) {
                            return null;
                        }
                        return project[valKey];
                    } else {
                        return value;
                    }
                });

                return [{$group: newGroup}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'removeRedundantProjectRootBeforeMatch',
        description: 'Removes redundant root before group if its simple',
        pattern: ['$project', '$match', '$project'],
        fixerFn: (stages) => {
            if (projectIsRoot(stages[0]) && projectIsSimple(stages[2])) {
                const testProject1Key = Object.keys(stages[0]['$project'])[0];
                const newMatch = _changeReferenceKey(
                    stages[1]['$match'],
                    testProject1Key
                );
                const newProject = _changeReferenceValue(
                    stages[2]['$project'],
                    testProject1Key
                );

                return [{$match: newMatch}, {$project: newProject}];
            } else {
                return null;
            }
        },
    },
    {
        name: 'switchSortMatch',
        description: 'Always switch adjacent sort and match stages',
        pattern: ['$sort', '$match'],
        fixerFn: (stages) => {
            return [stages[1], stages[0]];
        },
    },
];

/**
 * Determines whether the provided object value satisfies a "simple match" condition based on the given prefix.
 * @param {any} objVal - The object or value to evaluate. It can be an object, array, string, or other types.
 * @param {string} prefix - The prefix string used for validating keys or string values in the object.
 * @returns {boolean} Returns true if the object or value matches the "simple match" criteria with the prefix, otherwise false.
 */
function _matchPieceIsSimple(objVal, prefix) {
    if ($check.object(objVal)) {
        let isSimple = false;
        for (const objKey of Object.keys(objVal)) {
            if (!objKey.startsWith('$')) {
                if (!objKey.startsWith(prefix)) {
                    return false;
                } else {
                    isSimple =
                        isSimple || _matchPieceIsSimple(objVal[objKey], prefix);
                }
            } else {
                isSimple =
                    isSimple || _matchPieceIsSimple(objVal[objKey], prefix);
            }
        }
        return isSimple;
    } else if ($check.array(objVal)) {
        for (const obj of objVal) {
            if (!_matchPieceIsSimple(obj, prefix)) {
                return false;
            }
        }
        return true;
    } else if ($check.string(objVal)) {
        return objVal.startsWith('$') ? objVal.startsWith('$' + prefix) : true;
    } else {
        return true;
    }
}

/**
 * Determines if a given match stage in a pipeline is simple.
 * @param {object} stage - The pipeline stage to evaluate.
 * @param {string} prefix - The prefix to use when processing the match object.
 * @returns {boolean} Returns true if the match stage is simple, false otherwise.
 */
function _matchIsSimple(stage, prefix) {
    if (!stage) {
        return false;
    }
    if (!stage['$match']) {
        return false;
    }
    const match = stage['$match'];
    return _matchPieceIsSimple(match, prefix + '.');
}

/**
 * Adjusts the order and reference of the `$match` and `$project` stages in a MongoDB aggregation pipeline.
 * Used when the where is further down the pipeline stack.
 * Ensures that the pipeline maintains proper structure when specific `$match` and `$project` stages are present.
 * @param {Array} mongoAggregate - The MongoDB aggregation pipeline to be modified.
 * @returns {Array} The modified MongoDB aggregation pipeline with corrected stage order and references.
 */
function _fixEndWhere(mongoAggregate) {
    const stages = _getStageNames(mongoAggregate);

    const firstStage = mongoAggregate[0];
    if (projectIsRoot(firstStage) && stages.includes('$match')) {
        const projectRootField = Object.keys(firstStage['$project'])[0];
        let lastMatch = null;
        let lastMatchIndex = -1;
        for (let i = 1; i < mongoAggregate.length; i++) {
            const stage = mongoAggregate[i];
            if (stage['$project']) {
                break;
            }
            if (stage['$match'] && _matchIsSimple(stage, projectRootField)) {
                lastMatch = stage;
                lastMatchIndex = i;
                break;
            }
        }

        if (lastMatch) {
            mongoAggregate.splice(lastMatchIndex, 1);
            mongoAggregate.unshift(
                _changeReference(lastMatch, projectRootField)
            );
        }
    }

    return mongoAggregate;
}

/**
 * Optimizes a given MongoDB aggregation pipeline by repeatedly applying transformation rules
 * to remove redundant or unneeded operations for improved performance.
 * @param {Array} mongoAggregate - The original MongoDB aggregation pipeline to be optimized.
 * @param {object} [options] - Optional configuration settings.
 * @param {number} [options.iterations] - Maximum number of optimization iterations to perform.
 * @returns {Array} - The optimized MongoDB aggregation pipeline.
 */
function optimizeMongoAggregate(mongoAggregate, options = {}) {
    let newAggregate = $copy(mongoAggregate);
    let lastHash = '';
    let iteration = options.iterations || 10;

    while (iteration > 0 && lastHash !== $hash(newAggregate)) {
        lastHash = $hash(newAggregate);

        for (const pipelineStage of newAggregate) {
            if (
                pipelineStage.$lookup &&
                pipelineStage.$lookup.pipeline &&
                pipelineStage.$lookup.pipeline.length > 0
            ) {
                pipelineStage.$lookup.pipeline = optimizeMongoAggregate(
                    pipelineStage.$lookup.pipeline,
                    options
                );
            }
        }

        for (const pattern of _patternsToFix) {
            newAggregate = _patternFixer(
                newAggregate,
                pattern.pattern,
                pattern.fixerFn,
                {copy: false}
            );
        }

        newAggregate = _fixEndWhere(newAggregate);
        iteration--;
    }

    return newAggregate;
}

module.exports = {
    optimizeMongoAggregate: optimizeMongoAggregate,
};
