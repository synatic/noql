/**
 * Checks if a where statement has any functions
 *
 * @param {object} where - the where statement to check
 * @returns {*|boolean}
 */
function whereHasFunction(where) {
    const checkBinaryExpr = (expr) => {
        let hasFunctions = false;
        if (!expr) return false;
        if (expr.type === 'binary_expr') {
            hasFunctions = hasFunctions || checkBinaryExpr(expr.left);
            hasFunctions = hasFunctions || checkBinaryExpr(expr.right);
        } else {
            hasFunctions = hasFunctions || (['function'].includes(expr.type) && expr.name !== 'FIELD_EXISTS');
        }
        return hasFunctions;
    };
    return checkBinaryExpr(where);
}

module.exports = {whereHasFunction};
