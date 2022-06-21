const $check = require('check-types');
const {parseSQLtoAST} = require('../parseSQLtoAST');
const {canQuery, whereContainsOtherTable} = require('../canQuery');
const {createResultObject} = require('./createResultObject');
const {getWhereAstQueries} = require('./filter-queries');
const makeQueryPartModule = require('./makeQueryPart');
const projectColumnParserModule = require('./projectColumnParser');
const makeAggregatePipelineModule = require('./makeAggregatePipeline');
/**
 * Parses a sql statement into a mongo aggregate pipeline
 *
 * @param {import('../types').ParserInput} sqlOrAST - The sql to make into an aggregate
 * @param {import('../types').ParserOptions} [options] - the parser options
 * @returns {import('../types').ParsedMongoAggregate}
 * @throws
 */
function makeMongoAggregate(sqlOrAST, options = {unwindJoins: false}) {
    // todo fix sub select table return
    const {ast} = parseSQLtoAST(sqlOrAST, options);

    // subselect for arrays need to remove the collections since theyre actually arrays

    return {
        pipeline: makeAggregatePipelineModule.makeAggregatePipeline(
            ast,
            options
        ),
        collections: getTables(ast),
        type: 'aggregate',
    };
}
/**
 * Gets a list of tables used in the aggregation
 *
 * @param {import('../types').AST} subAst
 * @returns {string[]}
 */
function getTables(subAst) {
    let tables = [];

    if (whereContainsOtherTable(subAst.where)) {
        const queries = getWhereAstQueries(subAst.where);
        const subTables = queries.flatMap((q) => q.ast).flatMap(getTables);
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
            let table = from.table;
            if (table && table.endsWith('|first')) {
                table = table.substring(0, table.length - 6);
            } else if (table && table.endsWith('|last')) {
                table = table.substring(0, table.length - 5);
            } else if (table && table.endsWith('|unwind')) {
                table = table.substring(0, table.length - 7);
            }
            tables.push(table);
        } else if (from.expr && from.expr.ast) {
            tables = tables.concat(getTables(from.expr.ast));
        }
    }

    return tables;
}
/**
 * Converts a SQL statement to a mongo query.
 *
 * @param {import('../types').ParserInput} sqlOrAST - the SQL statement or AST to parse
 * @param {import('../types').ParserOptions} [options] - the parser options
 * @returns {import('../types').ParsedMongoQuery}
 * @throws
 */
function makeMongoQuery(sqlOrAST, options = {}) {
    const parsedAST = parseSQLtoAST(sqlOrAST, options);
    if (!canQuery(parsedAST)) {
        throw new Error(
            'Query cannot cross multiple collections, have an aggregate function, contain functions in where clauses or have $$ROOT AS'
        );
    }
    const ast = parsedAST.ast;
    const result = createResultObject();
    /** @type{import('../types').ParsedMongoQuery} */
    const parsedQuery = {
        limit: 100,
        collection: ast.from[0].table,
        type: 'query',
    };

    if (ast.columns && Array.isArray(ast.columns) && ast.columns.length > 0) {
        ast.columns.forEach((column) => {
            projectColumnParserModule.projectColumnParser(column, result);
        });
        parsedQuery.projection = result.parsedProject.$project;
    }

    if (ast.limit) {
        if (
            ast.limit.seperator &&
            ast.limit.seperator === 'offset' &&
            ast.limit.value[1] &&
            ast.limit.value[1].value
        ) {
            parsedQuery.limit = ast.limit.value[0].value;
            parsedQuery.skip = ast.limit.value[1].value;
        } else if (
            ast.limit.value &&
            ast.limit.value[0] &&
            ast.limit.value[0].value
        ) {
            parsedQuery.limit = ast.limit.value[0].value;
        }
    }

    if (ast.where) {
        parsedQuery.query = makeQueryPartModule.makeQueryPart(
            ast.where,
            true,
            [],
            false
        );
    }

    if (ast.orderby && ast.orderby.length > 0) {
        parsedQuery.sort = ast.orderby.reduce((a, v) => {
            a[v.expr.column || v.expr.value] = v.type === 'DESC' ? -1 : 1;
            return a;
        }, {});
    }

    return parsedQuery;
}

module.exports = {
    makeMongoAggregate,
    makeMongoQuery,
};
