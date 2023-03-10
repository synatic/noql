const _allowableFunctions = require('./MongoFunctions');
const {isSelectAll: checkIfIsSelectAll} = require('./isSelectAll');
const {parseSQLtoAST} = require('./parseSQLtoAST');
/**
 * Checks whether the expression is null or its type is null
 *
 * @param {any} val - the expression value to check
 * @returns {boolean} - whether it is null or not
 * @private
 */
function _checkNullOrEmptyType(val) {
    return !val || (val && !val.type);
}

/**
 * Returns a function to check whether the column supports a function
 *
 * @param {import('./types').Column} column - the column to check
 * @returns {()=>*} - the function to check the column type
 */
function findFnFromColumnType(column) {
    return (fn) =>
        fn.name === column.expr.name.toLowerCase() &&
        (!fn.type || fn.type === column.expr.type) &&
        fn.allowQuery;
}

/**
 * Checks whether the column contains an allowed function
 *
 * @param {import('./types').Column} column - the column to check
 * @returns {boolean} - whether the column contains an allowed query function
 */
function checkIfContainsAllowedFunctions(column) {
    return (
        column.expr.type === 'function' &&
        !_allowableFunctions.functionMappings.find(findFnFromColumnType(column))
    );
}

/**
 * Checks whether the column contains an allowed aggregate function
 *
 * @param {import('./types').Column} column - the column to check
 * @returns {boolean} - whether the column contains an allowed aggregate function
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
 * Checks whether a mongo query can be performed or an aggregate is required
 *
 * @param {import('./types').ParserInput} sqlOrAST - the SQL statement or AST to parse
 * @param {import('./types').ParserOptions} [options] - the parser options
 * @returns {boolean} - if the sql or ast can be executed as a query
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
    const hasDistinct =
        ast.distinct === 'DISTINCT' || !_checkNullOrEmptyType(ast.distinct);
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
    const hasTableAlias = !!(ast.from && ast.from[0] && ast.from[0].as);
    // const hasForcedGroupBy = forceGroupBy(ast);

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
        hasUnion ||
        hasTableAlias;

    return !isAggregate;
}

/**
 *
 * @param expr
 */
function isAllowableType(expr) {
    if (expr.type === 'function' && expr.args) {
        if (expr.args.type === 'expr_list') {
            return expr.args.value.every((val) => isAllowableType(val));
        } else {
            return false;
        }
    } else if (expr.type === 'column_ref') {
        return true;
    } else {
        return ['number', 'string', 'single_quote_string'].includes(expr.type);
    }
}

/**
 * Checks whether the expression statement contains other tables to execute a sub select
 *
 * @param {import('./types').Expression} expr - the expressions to check
 * @returns {boolean} - whether the expression contains other tables
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
        return !expr.value.every((val) => isAllowableType(val));
    }

    return false;
}

module.exports = {
    canQuery,
    whereContainsOtherTable: checkWhereContainsOtherTable,
};
