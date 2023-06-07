const {Parser: $sqlParser} = require('node-sql-parser');
const $json = require('@synatic/json-magic');
const $check = require('check-types');

const parser = new $sqlParser();

function fixAst(ast, options = {}) {
    if (!ast) {
        return ast;
    }

    if (options.stripUnusedTableAlias) {
        ast = stripUnusedTableAlias(ast);
    }

    if (options.forceAs) {
        ast = forceAs(ast);
    }

    return ast;
}

function stripUnusedTableAlias(ast) {
    const tables = {};

    const workingAst = ast.ast;

    for (const column of workingAst.columns) {
        if (column.table) {
            tables[ast.table] = true;
        } else if (column.type === 'expr' && column.expr && column.expr.table) {
            tables[column.expr.table] = true;
        }
    }

    for (const from of workingAst.from) {
        if (from.as && !from.expr) {
            if (!tables[from.as]) {
                from.as = null;
            }
        } else if (from.expr && from.expr.ast) {
            from.expr = stripUnusedTableAlias(from.expr);
        }
    }

    return ast;
}

function forceAs(ast) {
    const workingAst = ast.ast;

    for (const column of workingAst.columns) {
        column.as =
            !column.as &&
            column.expr &&
            column.expr.type === 'column_ref' &&
            column.expr.column
                ? column.expr.column
                : column.as;
    }

    for (const from of workingAst.from) {
        if (from.expr && from.expr.ast) {
            from.expr = forceAs(from.expr);
        }
    }

    return ast;
}

function replaceDollar(ast) {
    $json.changeValue(ast, (val, path) => {
        const paths = path.split('/');
        const fieldName = paths[paths.length - 1];
        const parentFieldName = paths[paths.length - 2];

        if (
            $check.string(val) &&
            ['table', 'as'].includes(fieldName) &&
            val.startsWith('$') &&
            val !== '$$ROOT'
        ) {
            return '__' + val.substring(1);
        } else if (
            $check.string(val) &&
            ['column', 'as'].includes(fieldName) &&
            val.indexOf('..') > 1
        ) {
            return val.replace('..', '__');
        } else if (
            val === 'DECIMAL' &&
            fieldName === 'dataType' &&
            parentFieldName === 'target'
        ) {
            return 'DOUBLE';
        } else {
            return val;
        }
    });

    return ast;
}

/**
 *
 * @param {string} statement
 * @param {object} [options] - the parser options
 * @param {string} [options.database] - the type of database to parse with
 * @param {boolean} [options.stripUnusedTableAlias] - strip unused table alias
 * @param {boolean} [options.forceAs] - force as statement for columns
 * @returns {{parsedAst:import('node-sql-parser').TableColumnAst,replacements:Set<{oldAlias:string,newAlias:string}>}}
 */
function escapeAndParseStatementToAst(statement, options = {}) {
    return replaceDollar(
        fixAst(
            parser.parse(statement, {
                database: options.database,
            }),
            options
        )
    );
}
module.exports = {escapeAndParseStatementToAst};
