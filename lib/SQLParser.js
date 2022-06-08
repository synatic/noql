const {parseSQLtoAST} = require('./parseSQLtoAST');
const {canQuery} = require('./canQuery');
const {makeMongoAggregate, makeMongoQuery} = require('./make');
/**
 * @typedef {import('./types').ParserInput} ParserInput
 * @typedef {import('./types').ParserResult} ParserResult
 * @typedef {import('./types').ParserOptions} ParserOptions
 * @typedef {import('./types').MongoQuery} MongoQuery
 * @typedef {import('./types').MongoAggregate} MongoAggregate
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
     * @param {ParserInput} sqlOrAST - The sql to make into an aggregate
     * @param {ParserOptions} [options] - the parser options
     * @returns {MongoAggregate}
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
     * @returns {ParserResult}
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
     * @returns {MongoQuery}
     * @throws
     */
    static makeMongoQuery(sqlOrAST, options) {
        return makeMongoQuery(sqlOrAST, options);
    }
}

module.exports = SQLParser;
