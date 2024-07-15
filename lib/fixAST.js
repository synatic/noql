const $json = require('@synatic/json-magic');
const _aggrFunctionRenames = ['firstn', 'lastn'];

/**
 * Reworks an AST to fix any issues that are not covered by node-sql-parser
 * @param {import('./types').TableColumnAst} parsedAST - the parsedAST to validated
 * @returns {import('./types').TableColumnAst} - the fixed ast
 */
function fixAST(parsedAST) {
    if (!parsedAST) {
        return parsedAST;
    }
    if (Array.isArray(parsedAST.ast)) {
        const nonEmpty = parsedAST.ast.filter((a) => Object.keys(a).length);
        if (nonEmpty.length !== 1) {
            throw new Error(`Multiple root AST's are not yet supported`);
        }
        parsedAST.ast = nonEmpty[0];
    }

    const functionsToFix = [];
    const doubleQuotedColumnsToFix = [];
    $json.walk(parsedAST, (val, path) => {
        const pathParts = path.split('/').slice(1);
        const fixPath = pathParts.slice(0, pathParts.length - 1);
        if (val === 'function') {
            functionsToFix.push(fixPath);
        }
        if (val === 'double_quote_string' && path.indexOf('columns') >= 0) {
            doubleQuotedColumnsToFix.push(
                pathParts.slice(0, pathParts.length - 3)
            );
        }
    });
    for (const functionToFix of functionsToFix) {
        const value = $json.get(parsedAST, functionToFix);
        if (!value.name) {
            continue;
        }
        value.name =
            value.name.name && Array.isArray(value.name.name)
                ? value.name.name.length === 1
                    ? value.name.name[0].value
                    : (() => {
                          throw new Error(
                              `Function name had multiple values: ${value.name.join(', ')}`
                          );
                      })()
                : value.name;
        if (_aggrFunctionRenames.includes(value.name)) {
            value.type = 'aggr_func';
        }
    }

    for (const quoteFix of doubleQuotedColumnsToFix) {
        const value = $json.get(parsedAST, quoteFix);
        if (value.type === 'column_ref') {
            value.column = value.column.expr.value;
        }
    }
    return parsedAST;
}

module.exports = {fixAST};
