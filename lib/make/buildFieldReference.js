/**
 * Builds a MongoDB field reference path, optionally scoped to an array element.
 * Also normalizes legacy workaround column names written as `$this.field` or
 * `$$this.field` in SQL so production queries that used that hack keep working.
 *
 * Inside array subselects (`includeThis`):
 * - bare `field` → `$$this.field` (current array element)
 * - `$field` / `$$ROOT.field` → parent/root document (already a Mongo path)
 * - `$this.field` / `$$this.field` → `$$this.field` (legacy element workaround)
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
    if (path === '$$this' || path.startsWith('$$this.')) {
        return path;
    }

    // Legacy workaround: `$this.field` in SQL → `$$this.field`
    if (path === '$this' || path.startsWith('$this.')) {
        return `$${path}`;
    }

    // Guard against accidental extra $ prefixes from mixed hacks
    if (/^\$+this(\.|$)/.test(path)) {
        return path.replace(/^\$+this(?=\.|$)/, () => '$$this');
    }

    // `$parent.field`, `$$ROOT.field`, `$$NOW`, etc. are already Mongo paths.
    // In array subselects these refer to the parent/root document, not $$this.
    if (path.startsWith('$')) {
        return path;
    }

    if (includeThis) {
        return `$$this.${path}`;
    }

    return `$${path}`;
}

module.exports = {
    buildFieldReference,
    normalizeFieldReference,
};
