/**
 * @returns {import('../types').ColumnParseResult}
 */
function createResultObject() {
    return {
        replaceRoot: null,
        asMapping: [],
        groupBy: {
            $group: {
                _id: {},
            },
        },
        unwind: [],
        parsedProject: {$project: {}},
        exprToMerge: [],
        count: [],
        unset: [],
    };
}

module.exports = {createResultObject};
