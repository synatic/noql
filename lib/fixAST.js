const $json = require('@synatic/json-magic');

const _aggrFunctionRenames = ['firstn', 'lastn'];

/**
 * Reworks an AST to fix any issues that are not covered by node-sql-parser
 *
 * @param {import('./types').TableColumnAst} parsedAST - the parsedAST to validated
 * @returns {import('./types').TableColumnAst} - the fixed ast
 */
function fixAST(parsedAST) {
    if (!parsedAST) {
        return parsedAST;
    }

    const fixPaths = [];
    $json.walk(parsedAST, (val, path) => {
        const pathParts = path.split('/').slice(1);
        const fixPath = pathParts.slice(0, pathParts.length - 1);
        const fieldName = pathParts[pathParts.length - 1];
        if (_aggrFunctionRenames.includes(val) && fieldName === 'name') {
            fixPaths.push(fixPath);
        }
    });

    for (const fixPath of fixPaths) {
        const fnObj = $json.get(parsedAST, fixPath);
        if (fnObj && fnObj.type === 'function') {
            fnObj.type = 'aggr_func';
        }
    }
    return parsedAST;
}

module.exports = {fixAST};
