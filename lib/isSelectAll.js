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
    return typeof columns === 'string'
        ? columns === '*'
        : columns.length === 1 &&
              columns[0].expr.column === '*' &&
              !columns[0].expr.table;
}

module.exports = {isSelectAll};
