const {Parser} = require('node-sql-parser');
const $check = require('check-types');
const {validateAST} = require('./validateAst');
const {fixAST} = require('./fixAST');

/**
 * Parses a SQL string to an AST
 *
 * @param {import('./types').ParserInput} sql - the sql statement to parse
 * @param {import('./types').ParserOptions} [options] - the AST options
 * @returns {import('./types').ParseResult}
 * @throws
 */
function parseSQLtoAST(sql, options = {}) {
    /** @type {import('./types').NoqlContext} */
    const context = {
        schemas: options.schemas,
    };
    if ($check.object(sql)) {
        if (sql.ast) {
            return {
                parsedAst: sql,
                context,
            };
        }
        throw new Error(`SQL object does not contain the required key "ast"`);
    }
    context.rawStatement = sql;
    const parser = new Parser();
    /** @type {import('./types').TableColumnAst} */
    let parsedAst;
    // Remove terminating semicolons, as the parser thinks it is part of the query
    sql = sql.trim();
    sql = sql.replace(/;+$/, '');
    context.cleanedStatement = sql;
    try {
        // @ts-ignore
        parsedAst = parser.parse(sql, {
            database: options.database,
            type: options.type,
        });
    } catch (exp) {
        let message = '';
        if (exp.location && exp.location.start) {
            message = `[Start: Line ${exp.location.start.line}, Col:${exp.location.start.column}]`;
            if (exp.location.end) {
                message =
                    message +
                    `[End: Line ${exp.location.end.line}, Col:${exp.location.end.column}]`;
            }
        }
        if (message.length) {
            message = message + ' - ';
        }
        message = message + exp.message;
        throw new Error(message);
    }
    parsedAst = fixAST(parsedAst);
    validateAST(parsedAst);

    return {parsedAst, context};
}

module.exports = {parseSQLtoAST};
