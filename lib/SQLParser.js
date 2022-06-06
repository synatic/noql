const {parseSQLtoAST} = require('./parseSQLtoAST');
const {canQuery} = require('./canQuery');
const {makeMongoAggregate, makeMongoQuery} = require('./make');
/**
 * @typedef {import('node-sql-parser').TableColumnAst} TableColumnAst
 * @typedef {import('node-sql-parser').Option} NodeSqlParserOptions
 * @typedef {import('./types').MongoQuery} MongoQuery
 */

/**
 * Class representing a SQL parser
 *
 * @class SQLParser
 */
class SQLParser {
    /**
     * Parses sql to either a query or aggregate
     *
     * @param {string} sql - the sql command to parse
     * @param {object} [options] - the parser options
     * @returns {object}
     */
    static parseSQL(sql, options = {}) {
        if (!sql) {
            throw new Error('No SQL specified');
        }

        const parsedAST = parseSQLtoAST(sql, options);

        let parsedQuery = {};

        if (canQuery(parsedAST)) {
            parsedQuery = makeMongoQuery(parsedAST, options);
            parsedQuery.type = 'query';
        } else {
            parsedQuery = makeMongoAggregate(parsedAST, options);
            parsedQuery.type = 'aggregate';
        }

        return parsedQuery;
    }
    /**
     * Parses a sql statement into a mongo aggregate pipeline
     *
     * @param {string|object} sqlOrAST - The sql to make into an aggregate
     * @param {object} [options] - the parser options
     * @param {boolean} [options.unwindJoins] - automatically unwind joins, by default is set to false and unwind should be done by using unwind in the select)
     * @returns {{pipeline: *[], collections: *[]}}\
     * @throws
     */
    static makeMongoAggregate(sqlOrAST, options) {
        return makeMongoAggregate(sqlOrAST, options);
    }
    /**
     * Parses a SQL string to an AST
     *
     * @param {string|TableColumnAst} sql - the sql statement to parse
     * @param {NodeSqlParserOptions} [options] - the AST options
     * @returns {TableColumnAst}
     * @throws
     */
    static parseSQLtoAST(sql, options) {
        return parseSQLtoAST(sql, options);
    }
    /**
     * Converts a SQL statement to a mongo query.
     *
     * @param {string|TableColumnAst} sqlOrAST - the SQL statement or AST to parse
     * @param {NodeSqlParserOptions} [options] - the parser options
     * @returns {MongoQuery}
     * @throws
     */
    static makeMongoQuery(sqlOrAST, options) {
        return makeMongoQuery(sqlOrAST, options);
    }
}

module.exports = SQLParser;
