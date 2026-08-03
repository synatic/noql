/**
 * Builds a MongoDB field reference path, optionally scoped to an array element.
 * Also normalizes legacy workaround column names written as `$this.field` or
 * `$$this.field` in SQL so production queries that used that hack keep working.
 *
 * @param {string|null|undefined} table
 * @param {string} column
 * @param {boolean} [includeThis]
 * @returns {string}
 */
function buildFieldReference(table, column, includeThis = false) {
    const path = `${table ? `${table}.` : ''}${column}`;
    return normalizeFieldReference(path, includeThis);
}

/**
 * @param {string} path
 * @param {boolean} [includeThis]
 * @returns {string}
 */
function normalizeFieldReference(path, includeThis = false) {
    if (!path || typeof path !== 'string') {
        return path;
    }

    // Already a correct array-element ref (including legacy `$$this.field` SQL)
    if (path.startsWith('$$this.')) {
        return path;
    }

    // Legacy workaround: `$this.field` in SQL → `$$this.field`
    if (path.startsWith('$this.')) {
        return `$${path}`;
    }

    // Guard against accidental extra $ prefixes from mixed hacks
    if (/^\$+this\./.test(path)) {
        return path.replace(/^\$+this\./, '$$this.');
    }

    if (includeThis) {
        return `$$this.${path}`;
    }

    return path.startsWith('$') ? path : `$${path}`;
}

module.exports = {
    buildFieldReference,
    normalizeFieldReference,
};
