module.exports = {getLimitAndOffset};

/**
 * Reads the LIMIT and OFFSET values from a node-sql-parser limit node.
 * node-sql-parser gives `[limit, offset]` for `LIMIT n OFFSET m`, but only
 * `[offset]` (with seperator 'offset') for `OFFSET m` on its own.
 * @param {object} limitNode the ast.limit node
 * @returns {{limit?: number|string, offset?: number|string}} the values, omitted when not set or 0
 */
function getLimitAndOffset(limitNode) {
    const values = (limitNode && limitNode.value) || [];
    const isOffset = limitNode && limitNode.seperator === 'offset';
    let limit;
    let offset;
    if (isOffset && values.length === 1) {
        offset = values[0] && values[0].value;
    } else {
        limit = values[0] && values[0].value;
        offset = isOffset ? values[1] && values[1].value : undefined;
    }
    const result = {};
    if (limit) {
        result.limit = limit;
    }
    if (offset) {
        result.offset = offset;
    }
    return result;
}
