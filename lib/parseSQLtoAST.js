const {Parser} = require('node-sql-parser');
const $check = require('check-types');
const {validateAST} = require('./validateAst');
/**
 * @typedef {import('node-sql-parser').TableColumnAst} TableColumnAst
 * @typedef {import('node-sql-parser').Option} NodeSqlParserOptions
 * @typedef {import('./types').MongoQuery} MongoQuery
 */

/**
 * Parses a SQL string to an AST
 *
 * @param {string|TableColumnAst} sql - the sql statement to parse
 * @param {NodeSqlParserOptions} [options] - the AST options
 * @returns {TableColumnAst}
 * @throws
 */
function parseSQLtoAST(sql, options = {}) {
    if ($check.object(sql)) {
        if (sql.ast) {
            return sql;
        }
        throw new Error(`SQL object does not contain the required key "ast"`);
    }
    const parser = new Parser();
    /** @type {TableColumnAst} */
    let parsedAST;
    try {
        parsedAST = parser.parse(sql, options);
    } catch (exp) {
        throw new Error(
            `${exp.location && exp.location.start ? exp.location.start.line + ':' + exp.location.start.column + ' - ' : ''}${exp.message}`
        );
    }
    validateAST(parsedAST);

    return parsedAST;
}

module.exports = {parseSQLtoAST};
