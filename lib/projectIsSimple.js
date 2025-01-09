const projectIsRoot = require('./projectIsRoot');
const $check = require('check-types');

/**
 * Determines whether the given project stage of an aggregation pipeline is considered simple.
 * A simple project stage typically maps direct fields or values without complex computations.
 * @param {object} stage - The stage object from an aggregation pipeline to be analyzed.
 * @returns {boolean} Returns `true` if the project stage is simple, otherwise `false`.
 */
function projectIsSimple(stage) {
    if (!stage || !stage.$project) {
        return false;
    }
    if (projectIsRoot(stage)) {
        return false;
    }

    const project = stage['$project'];
    let isSimple = true;
    for (const key in project) {
        if (Object.prototype.hasOwnProperty.call(project, key)) {
            const projectVal = project[key];
            if ($check.integer(projectVal)) {
                continue;
            }

            if (!$check.string(projectVal) || !projectVal.startsWith('$')) {
                isSimple = false;
                break;
            }
        }
    }

    return isSimple;
}

module.exports = projectIsSimple;
