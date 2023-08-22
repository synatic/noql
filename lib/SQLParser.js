const {parseSQLtoAST} = require('./parseSQLtoAST');
const {canQuery} = require('./canQuery');
const {makeMongoAggregate, makeMongoQuery} = require('./make');
const {getResultSchemaForStatement} = require('./metadata');

/**
 * @typedef {import('./types').ParserOptions} ParserOptions
 * @typedef {import('./types').ParsedQueryOrAggregate} ParsedQueryOrAggregate
 * @typedef {import('./types').ParserInput} ParserInput
 * @typedef {import('./types').ParsedMongoAggregate} ParsedMongoAggregate
 * @typedef {import('./types').TableColumnAst} TableColumnAst
 * @typedef {import('./types').ParsedMongoQuery} ParsedMongoQuery
 * @typedef {import('./types').GetSchemaFunction} GetSchemaFunction
 * @typedef {import('./types').ResultSchema} ResultSchema
 * @typedef {import('./types').ParseResult} ParseResult
 */

/**
 * Class representing a SQL parser
 *
 * @class SQLParser
 */
class SQLParser {
    /**
     * The version of the parser
     *
     * @type {string}
     * @readonly
     * @static
     * @memberof SQLParser
     * @example
     * const {SQLParser} = require('@synatic/noql);
     * console.log(SQLParser.VERSION);
     * // 2.0.2
     */
    static get VERSION() {
        return require('../package.json').version;
    }

    /**
     * Parses sql to either a query or aggregate
     *
     * @param {string} sql - the sql command to parse
     * @param {object} [options] - the parser options
     * @returns {ParsedQueryOrAggregate}
     * @throws
     * @memberof SQLParser
     * @example
     * const {SQLParser} = require('@synatic/noql');
     * const sql = 'SELECT * FROM users WHERE age > 18';
     * const parsed = SQLParser.parseSQL(sql);
     * console.log(parsed);
     * // {
     * //   collection: 'users',
     * //   query: { age: { $gt: 18 } },
     * //   projection: { _id: 0 },
     * // }
     */
    static parseSQL(sql, options = {}) {
        if (!sql) {
            throw new Error('No SQL specified');
        }

        const {parsedAst, context} = parseSQLtoAST(sql, options);

        if (canQuery(parsedAst)) {
            return makeMongoQuery(parsedAst, options, context);
        }
        return makeMongoAggregate(parsedAst, options, context);
    }
    /**
     * Parses a sql statement into a mongo aggregate pipeline
     *
     * @param {ParserInput} sqlOrAST - The sql to make into an aggregate
     * @param {ParserOptions} [options] - the parser options
     * @returns {ParsedMongoAggregate}
     * @throws
     */
    static makeMongoAggregate(sqlOrAST, options) {
        return makeMongoAggregate(sqlOrAST, options);
    }
    /**
     * Parses a SQL string to an AST
     *
     * @param { ParserInput} sql - the sql statement to parse
     * @param {ParserOptions} [options] - the AST options
     * @returns {TableColumnAst}
     * @throws
     */
    static parseSQLtoAST(sql, options) {
        const {parsedAst} = parseSQLtoAST(sql, options);
        return parsedAst;
    }
    /**
     * Converts a SQL statement to a mongo query.
     *
     * @param {ParserInput} sqlOrAST - the SQL statement or AST to parse
     * @param {ParserOptions} [options] - the parser options
     * @returns {ParsedMongoQuery}
     * @throws
     */
    static makeMongoQuery(sqlOrAST, options) {
        return makeMongoQuery(sqlOrAST, options);
    }
    /**
     * Checks whether a mongo query can be performed or an aggregate is required
     *
     * @param {ParserInput} sqlOrAST - the SQL statement or AST to parse
     * @param {ParserOptions} [options] - the parser options
     * @returns {boolean}
     * @throws
     */
    static canQuery(sqlOrAST, options) {
        return canQuery(sqlOrAST, options);
    }

    // todo RK move getSchemaFunction into options and update tests
    /**
     * @param {string} statement
     * @param {GetSchemaFunction} getSchemaFunction
     * @param {ParserOptions} [options] - the AST options
     * @returns {Promise<ResultSchema[]>}
     */
    static async getResultSchema(statement, getSchemaFunction, options) {
        return getResultSchemaForStatement(
            statement,
            getSchemaFunction,
            options
        );
    }
}

module.exports = SQLParser;
