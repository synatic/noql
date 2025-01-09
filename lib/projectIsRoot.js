/**
 * Checks if the given stage contains a `$project` operator that projects only the root document (`$$ROOT`).
 * @param {object} stage - The stage object to be checked.
 * @returns {boolean} Returns `true` if the `$project` operator projects only the root document, otherwise `false`.
 */
function projectIsRoot(stage) {
    if (stage && stage['$project']) {
        const project = stage['$project'];
        const projectKeys = Object.keys(project);
        if (projectKeys.length === 1) {
            const projectKey = projectKeys[0];
            if (project[projectKey] === '$$ROOT') {
                return true;
            }
        }
    }
    return false;
}

module.exports = projectIsRoot;
