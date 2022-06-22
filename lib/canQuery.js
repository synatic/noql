const _allowableFunctions = require('./MongoFunctions');
const {isSelectAll: checkIfIsSelectAll} = require('./isSelectAll');
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
    const isSelectAll = checkIfIsSelectAll(ast.columns);
    /** @type{import('./types').Column[]} */
    const columns = typeof ast.columns === 'string' ? null : ast.columns;
    const asColumns = isSelectAll
        ? []
        : columns.map((c) => c.as).filter((c) => !!c);
    const checkAsUsedInWhere = (expr) => {
        if (!expr) {
            return false;
        }
        if (expr.type === 'binary_expr') {
            return (
                checkAsUsedInWhere(expr.left) || checkAsUsedInWhere(expr.right)
            );
        }
        if (expr.type === 'column_ref') {
            return !!asColumns.find((c) => c === expr.column);
        }

        return false;
    };
    const moreThanOneFrom = ast.from.length > 1;
    const hasNoTable = !ast.from[0].table;
    const hasGroupBy = !!ast.groupby;
    const hasDistinct = !!ast.distinct;
    const containsAllowedAggregateFunctions =
        !isSelectAll &&
        columns.findIndex(checkIfContainsAllowedAggregateFunctions) > -1;
    const hasAsRoot =
        !isSelectAll &&
        !options.isArray &&
        columns.findIndex((c) => c.as === '$$ROOT') > -1;
    const containsAllowedFunctions =
        !isSelectAll && columns.findIndex(checkIfContainsAllowedFunctions) > -1;
    const containsColumnRefAsterix =
        !isSelectAll &&
        columns.findIndex(
            (c) => c.expr.type === 'column_ref' && c.expr.column === '*'
        ) > -1;
    const whereContainsOtherTable = checkWhereContainsOtherTable(ast.where);
    const asColumnsUsedInWhere =
        asColumns.length > 0 && checkAsUsedInWhere(ast.where);
    const fromHasExpr = ast.from.findIndex((f) => !!f.expr) > -1;
    const hasUnion = !!ast.union;
    const isAggregate =
        moreThanOneFrom ||
        hasNoTable ||
        hasGroupBy ||
        hasDistinct ||
        containsAllowedAggregateFunctions ||
        hasAsRoot ||
        containsAllowedFunctions ||
        containsColumnRefAsterix ||
        fromHasExpr ||
        asColumnsUsedInWhere ||
        whereContainsOtherTable ||
        hasUnion;
    return !isAggregate;
}
/**
 *
 * @param {import('./types').Column} column
 * @returns {boolean}
 */
function checkIfContainsAllowedFunctions(column) {
    return (
        column.expr.type === 'function' &&
        !_allowableFunctions.functionMappings.find(findFnFromColumnType(column))
    );
}

/**
 *
 * @param {import('./types').Column} column
 * @returns {boolean}
 */
function checkIfContainsAllowedAggregateFunctions(column) {
    if (column.expr.type !== 'aggr_func') {
        return false;
    }
    const someValue = _allowableFunctions.functionMappings.find(
        findFnFromColumnType(column)
    );
    return !someValue;
}

/**
 *
 * @param {import('./types').Column} column
 * @returns {()=>*}
 */
function findFnFromColumnType(column) {
    return (fn) =>
        fn.name === column.expr.name.toLowerCase() &&
        (!fn.type || fn.type === column.expr.type) &&
        fn.allowQuery;
}

/**
 *
 * @param {import('./types').Expression} expr
 * @returns {boolean}
 */
function checkWhereContainsOtherTable(expr) {
    if (!expr) {
        return false;
    }
    if (expr.type === 'binary_expr') {
        return (
            checkWhereContainsOtherTable(expr.left) ||
            checkWhereContainsOtherTable(expr.right)
        );
    }
    if (expr.type === 'expr_list') {
        return !expr.value.every((val) =>
            ['number', 'string', 'single_quote_string'].includes(val.type)
        );
    }
    return false;
}

module.exports = {
    canQuery,
    whereContainsOtherTable: checkWhereContainsOtherTable,
};
