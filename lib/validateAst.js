const $check = require('check-types');
const _allowableFunctions = require('./MongoFunctions');

/**
 * Validates an AST to ensure secondary parsing criteria are met
 *
 * @param {import('./types').TableColumnAst} parsedAST - the parsedAST to validated
 * @returns {void}
 */
function validateAST(parsedAST) {
    if (!parsedAST) {
        throw new Error('Invalid AST');
    }
    if (!parsedAST.tableList || parsedAST.tableList.length === 0) {
        throw new Error('SQL statement requires at least 1 collection');
    }
    const ast = parsedAST.ast;
    if (!ast.from || !ast.from[0]) {
        throw new Error('No FROM specified');
    }
    if ($check.array(ast.columns)) {
        const errors = [];
        for (const column of ast.columns) {
            if (
                column.expr &&
                ['function', 'binary_expr', 'aggr_func'].includes(
                    column.expr.type
                ) &&
                !column.as
            ) {
                const definition = _allowableFunctions.functionByName(
                    column.expr.name
                );
                if (!definition || definition.requiresAs !== false) {
                    errors.push(
                        `Requires as for ${column.expr.type}${
                            column.expr.name ? ':' + column.expr.name : ''
                        }`
                    );
                }
            }
            if (
                column.expr &&
                ['aggr_func'].includes(column.expr.type) &&
                !['SUM', 'AVG', 'MIN', 'MAX', 'COUNT'].includes(
                    column.expr.name
                ) &&
                !ast.groupby
            ) {
                errors.push(
                    `Requires group by for ${column.expr.type}${
                        column.expr.name ? ':' + column.expr.name : ''
                    }`
                );
            }
            if (
                column.expr &&
                (column.expr.type === 'function' ||
                    column.expr.type === 'aggr_func') &&
                column.expr.name.toLowerCase() === 'unwind'
            ) {
                continue;
            }
            if (
                column.expr &&
                column.expr.type === 'function' &&
                !_allowableFunctions.functionMappings.find(
                    (f) =>
                        f.name === column.expr.name.toLowerCase() &&
                        (!f.type || f.type === column.expr.type)
                )
            ) {
                errors.push(`Function not found: ${column.expr.name}`);
            }
            if (
                column.expr &&
                column.expr.type === 'aggr_func' &&
                !_allowableFunctions.functionMappings.find(
                    (f) =>
                        f.name === column.expr.name.toLowerCase() &&
                        (!f.type || f.type === column.expr.type)
                )
            ) {
                errors.push(
                    `Aggregate function not found: ${column.expr.name}`
                );
            }
        }
        if (errors.length > 0) {
            throw new Error(errors.join(','));
        }
    }
}

module.exports = {validateAST};
