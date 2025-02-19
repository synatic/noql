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
        groupByProject: null,
        exprToMerge: [],
        count: [],
        unset: null,
        countDistinct: null,
        windowFields: [],
        subQueryRootProjections: [],
        set: null,
        unsetAfterReplaceOrSet: null,
    };
}

module.exports = {createResultObject};
