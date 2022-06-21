const {parseSQLtoAST} = require('./parseSQLtoAST');
const {canQuery} = require('./canQuery');
const {makeMongoAggregate, makeMongoQuery} = require('./make');
/**
 * @typedef {import('./types').ParserInput} ParserInput
 * @typedef {import('./types').ParserOptions} ParserOptions
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
     * @returns {import('./types').ParsedQueryOrAggregate}
     */
    static parseSQL(sql, options = {}) {
        if (!sql) {
            throw new Error('No SQL specified');
        }

        const parsedAST = parseSQLtoAST(sql, options);

        if (canQuery(parsedAST)) {
            return makeMongoQuery(parsedAST, options);
        }
        return makeMongoAggregate(parsedAST, options);
    }
    /**
     * Parses a sql statement into a mongo aggregate pipeline
     *
     * @param {ParserInput} sqlOrAST - The sql to make into an aggregate
     * @param {ParserOptions} [options] - the parser options
     * @returns {import('./types').ParsedMongoAggregate}
     * @throws
     */
    static makeMongoAggregate(sqlOrAST, options) {
        return makeMongoAggregate(sqlOrAST, options);
    }
    /**
     * Parses a SQL string to an AST
     *
     * @param {ParserInput} sql - the sql statement to parse
     * @param {ParserOptions} [options] - the AST options
     * @returns {import('./types').TableColumnAst}
     * @throws
     */
    static parseSQLtoAST(sql, options) {
        return parseSQLtoAST(sql, options);
    }
    /**
     * Converts a SQL statement to a mongo query.
     *
     * @param {ParserInput} sqlOrAST - the SQL statement or AST to parse
     * @param {ParserOptions} [options] - the parser options
     * @returns {import('./types').ParsedMongoQuery}
     * @throws
     */
    static makeMongoQuery(sqlOrAST, options) {
        return makeMongoQuery(sqlOrAST, options);
    }
}

module.exports = SQLParser;
