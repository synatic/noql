const $check = require('check-types');
const {whereContainsOtherTable} = require('./canQuery');
const {getWhereAstQueries} = require('./make/filter-queries');

module.exports = {getTables};

/**
 * Gets a list of tables used in the query/aggregation
 * @type {import('./types').GetTables}
 */
function getTables(subAst, context) {
    let tables = [];

    if (
        !subAst.where &&
        !subAst.from &&
        subAst.ast &&
        subAst.tableList &&
        subAst.columnList
    ) {
        return getTables(subAst.ast, context);
    }

    if (whereContainsOtherTable(subAst.where)) {
        const queries = getWhereAstQueries(subAst.where, context);
        const subTables = queries
            .flatMap((q) => q.ast)
            .flatMap((a) => getTables(a, context));
        tables = tables.concat(subTables);
    }
    if (!subAst.from) {
        return tables;
    }
    if (!$check.array(subAst.from)) {
        return tables;
    }
    for (const from of subAst.from) {
        if (from.table) {
            tables.push(from.table.split('|')[0]);
        } else if (from.expr && from.expr.ast) {
            tables = tables.concat(getTables(from.expr.ast, context));
        }
    }

    if (subAst._next) {
        tables = tables.concat(getTables(subAst._next, context));
    }

    return tables;
}
