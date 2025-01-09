const {parseSQLtoAST} = require('../parseSQLtoAST');
const {canQuery} = require('../canQuery');
const {createResultObject} = require('./createResultObject');
const makeQueryPartModule = require('./makeQueryPart');
const projectColumnParserModule = require('./projectColumnParser');
const makeAggregatePipelineModule = require('./makeAggregatePipeline');
const arraySequenceIndexOf = require('../arraySequenceIndexOf');
const lodash = require('lodash');
/**
 * Parses a sql statement into a mongo aggregate pipeline
 *
 * @param {import('../types').ParserInput} sqlOrAST - The sql to make into an aggregate
 * @param {import('../types').ParserOptions} [options] - the parser options
 * @param {import('../types').NoqlContext} [context] - The Noql context to use when generating the output
 * @returns {import('../types').ParsedMongoAggregate}
 * @throws
 */
function makeMongoAggregate(sqlOrAST, options = {unwindJoins: false}, context) {
    const {
        parsedAst: {ast},
        context: newContext,
    } = parseSQLtoAST(sqlOrAST, options);
    if (!context) {
        context = newContext;
    }
    // todo fix sub select table return

    const pipeline = makeAggregatePipelineModule.makeAggregatePipeline(
        ast,
        context
    );
    const tables = context.tables.filter((x, i, a) => a.indexOf(x) === i);
    if (
        context &&
        context._reorderedTables &&
        context._reorderedTables.length > 0
    ) {
        const tablesIndexOf = arraySequenceIndexOf(
            lodash.reverse(context._reorderedTables),
            tables
        );
        if (tablesIndexOf > -1) {
            tables.splice(
                tablesIndexOf,
                context._reorderedTables.length,
                ...lodash.reverse(context._reorderedTables)
            );
        }
    }

    return {
        pipeline: pipeline,
        // subselect for arrays need to remove the collections since theyre actually arrays
        collections: tables,
        type: 'aggregate',
    };
}
/**
 * Converts a SQL statement to a mongo query.
 *
 * @param {import('../types').ParserInput} sqlOrAST - the SQL statement or AST to parse
 * @param {import('../types').ParserOptions} [options] - the parser options
 * @param {import('../types').NoqlContext} [context] - The Noql context to use when generating the output
 * @returns {import('../types').ParsedMongoQuery}
 * @throws
 */
function makeMongoQuery(sqlOrAST, options = {}, context) {
    const {parsedAst, context: newContext} = parseSQLtoAST(sqlOrAST, options);
    if (!context) {
        context = newContext;
    }
    if (!canQuery(parsedAst)) {
        throw new Error(
            'Query cannot cross multiple collections, have an aggregate function, contain functions in where clauses or have $$ROOT AS'
        );
    }
    const ast = parsedAst.ast;
    const result = createResultObject();
    /** @type{import('../types').ParsedMongoQuery} */
    const parsedQuery = {
        limit: 100,
        collection: ast.from[0].table,
        type: 'query',
    };

    if (ast.columns && Array.isArray(ast.columns) && ast.columns.length > 0) {
        ast.columns.forEach((column) => {
            projectColumnParserModule.projectColumnParser(
                column,
                result,
                context
            );
        });
        if (Object.keys(result.parsedProject.$project).length) {
            parsedQuery.projection = result.parsedProject.$project;
        }
        if (result.unset && result.unset.$unset) {
            for (const colToUnset of result.unset.$unset) {
                parsedQuery.projection[colToUnset] = 0;
            }
        }
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
            context,
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
