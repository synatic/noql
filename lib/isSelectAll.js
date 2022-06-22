/**
 * Checks whether a columns is a select *
 *
 * @param {import('./types').Columns} columns - the columns to check
 * @returns {boolean}
 */
function isSelectAll(columns) {
    if (!columns) {
        return false;
    }
    return columns === '*';
}

module.exports = {isSelectAll};
