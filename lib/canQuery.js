const _allowableFunctions = require('./MongoFunctions');
const {isSelectAll} = require('./isSelectAll');
const {parseSQLtoAST} = require('./parseSQLtoAST');

/**
 * Checks whether a mongo query can be performed or an aggregate is required
 *
 * @param {import('./types').ParserInput} sqlOrAST - the SQL statement or AST to parse
 * @param {import('./types').ParserOptions} [options] - the parser options
 * @returns {boolean}
 * @throws
 */
function canQuery(sqlOrAST, options = {isArray: false}) {
    const parsedAST = parseSQLtoAST(sqlOrAST, options);

    const ast = parsedAST.ast;
    const asColumns = isSelectAll(ast.columns) ? [] : ast.columns.map((c) => c.as).filter((c) => !!c);
    const checkAsUsedInWhere = (expr) => {
        if (!expr) {
            return false;
        }
        if (expr.type === 'binary_expr') {
            return checkAsUsedInWhere(expr.left) || checkAsUsedInWhere(expr.right);
        }
        if (expr.type === 'column_ref') {
            return !!asColumns.find((c) => c === expr.column);
        }

        return false;
    };

    return !(
        (
            ast.from.length > 1 ||
            !ast.from[0].table ||
            ast.groupby ||
            ast.distinct ||
            (ast.columns !== '*' &&
                ast.columns.findIndex(
                    (c) =>
                        c.expr.type === 'aggr_func' &&
                        !_allowableFunctions.functionMappings.find(
                            (f) => f.name === c.expr.name.toLowerCase() && (!f.type || f.type === c.expr.type) && f.allowQuery
                        )
                ) > -1) ||
            (ast.columns !== '*' && !options.isArray && ast.columns.findIndex((c) => c.as === '$$ROOT') > -1) ||
            (ast.columns !== '*' &&
                ast.columns.findIndex(
                    (c) =>
                        c.expr.type === 'function' &&
                        !_allowableFunctions.functionMappings.find(
                            (f) => f.name === c.expr.name.toLowerCase() && (!f.type || f.type === c.expr.type) && f.allowQuery
                        )
                ) > -1) ||
            (ast.columns !== '*' && ast.columns.findIndex((c) => c.expr.type === 'column_ref' && c.expr.column === '*') > -1) ||
            ast.from.findIndex((f) => !!f.expr) > -1 ||
            (asColumns.length > 0 && checkAsUsedInWhere(ast.where)) ||
            whereContainsOtherTable(ast.where)
        )
        // || (ast.columns!=="*"&&ast.columns.filter(c => c.expr.type === "function" || c.expr.type === "binary_expr").length>0));
    );
}

/**
 *
 * @param {*} expr
 * @returns {boolean}
 */
function whereContainsOtherTable(expr) {
    if (!expr) {
        return false;
    }
    if (expr.type === 'binary_expr') {
        return whereContainsOtherTable(expr.left) || whereContainsOtherTable(expr.right);
    }
    if (expr.type === 'expr_list') {
        return !expr.value.every((val) => val.type === 'single_quote_string');
    }
    return false;
}

module.exports = {canQuery, whereContainsOtherTable};
