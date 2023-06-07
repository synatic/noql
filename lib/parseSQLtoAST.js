const {Parser} = require('node-sql-parser');
const $check = require('check-types');
const {validateAST} = require('./validateAst');
const {fixAST} = require('./fixAST');

/**
 * Parses a SQL string to an AST
 *
 * @param {import('./types').ParserInput} sql - the sql statement to parse
 * @param {import('./types').ParserOptions} [options] - the AST options
 * @returns {import('./types').TableColumnAst}
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
    /** @type {import('./types').TableColumnAst} */
    let parsedAST;
    // Remove terminating semicolons, as the parser thinks it is part of the query
    sql = sql.trim();
    sql = sql.replace(/;+$/, '');
    try {
        // @ts-ignore
        parsedAST = parser.parse(sql, {
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
    parsedAST = fixAST(parsedAST);
    validateAST(parsedAST);

    return parsedAST;
}

module.exports = {parseSQLtoAST};
