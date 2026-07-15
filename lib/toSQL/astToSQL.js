const RESERVED_SAFE = /^[A-Za-z_][A-Za-z0-9_]*$/;

/**
 * Backtick-quotes an identifier. Always quoting (rather than only when
 * needed) sidesteps having to track the grammar's reserved-word list here.
 * @param {string} name
 * @returns {string}
 */
function quoteIdent(name) {
    return `\`${String(name).replace(/`/g, '')}\``;
}

/**
 * Serializes a column-ref AST node (`{kind:'column', table, name}`) to a
 * dotted SQL identifier. A `name` containing a literal dot (a 3+ level
 * nested path collapsed into one segment by the reverse converter) is
 * backtick-quoted as a single compound identifier so the grammar doesn't
 * re-split it into a 3-part schema.table.column reference.
 * @param {{table:string|null, name:string}} node
 * @returns {string}
 */
function columnRefToSQL(node) {
    if (!node.table) {
        return quoteIdent(node.name);
    }
    return `${quoteIdent(node.table)}.${quoteIdent(node.name)}`;
}

function literalToSQL(value) {
    if (value === null || value === undefined) return 'NULL';
    if (typeof value === 'boolean') return value ? 'TRUE' : 'FALSE';
    if (typeof value === 'number' || typeof value === 'bigint') return String(value);
    if (value instanceof Date) return `DATE_FROM_STRING('${value.toISOString()}')`;
    if (value && typeof value === 'object' && value._bsontype === 'ObjectId') {
        return `TO_OBJECTID('${value.toString()}')`;
    }
    if (Array.isArray(value) || typeof value === 'object') {
        // an object/array literal only ever gets here via PARSE_JSON('...')
        // (or an exact array/document equality match) — either way it
        // round-trips through re-parsing the same JSON text.
        return `PARSE_JSON('${JSON.stringify(value).replace(/'/g, "''")}')`;
    }
    return `'${String(value).replace(/'/g, "''")}'`;
}

/**
 * Serializes a noql SQL AST expression node to SQL text.
 * @param {object} node
 * @returns {string}
 */
function exprToSQL(node) {
    switch (node.kind) {
        case 'column':
            return columnRefToSQL(node);
        case 'star':
            return node.table ? `${quoteIdent(node.table)}.*` : '*';
        case 'literal':
            return literalToSQL(node.value);
        case 'binary':
            return `(${exprToSQL(node.left)} ${node.op} ${exprToSQL(node.right)})`;
        case 'logical':
            return `(${node.args.map(exprToSQL).join(` ${node.op} `)})`;
        case 'not':
            return `(NOT ${exprToSQL(node.expr)})`;
        case 'isNull':
            return `(${exprToSQL(node.expr)} IS ${node.negate ? 'NOT ' : ''}NULL)`;
        case 'like':
            return `(${exprToSQL(node.expr)} ${node.negate ? 'NOT LIKE' : 'LIKE'} ${literalToSQL(node.pattern)})`;
        case 'in': {
            const list = Array.isArray(node.list)
                ? `(${node.list.map(exprToSQL).join(', ')})`
                : exprToSQL(node.list);
            return `(${exprToSQL(node.expr)} ${node.negate ? 'NOT IN' : 'IN'} ${list})`;
        }
        case 'func':
            return `${node.name}(${node.args.map(exprToSQL).join(', ')})`;
        case 'aggr':
            return `${node.name}(${node.distinct ? 'DISTINCT ' : ''}${node.args.map(exprToSQL).join(', ')})`;
        case 'cast':
            return `CAST(${exprToSQL(node.expr)} AS ${node.dataType})`;
        case 'case': {
            const branches = node.branches
                .map((b) => `WHEN ${exprToSQL(b.when)} THEN ${exprToSQL(b.then)}`)
                .join(' ');
            return `(CASE ${branches} ELSE ${exprToSQL(node.else)} END)`;
        }
        default:
            throw new Error(`Unsupported AST expression node kind: ${node.kind}`);
    }
}

/**
 * Serializes a noql SQL AST (as produced by lib/toSQL/pipelineToAST.js) back
 * into a SQL string.
 * @param {object} ast
 * @returns {string}
 */
function astToSQL(ast) {
    const parts = ['SELECT'];

    const colSql = ast.columns === 'star' ? ['*'] : ast.columns.map((c) => {
        const e = exprToSQL(c.expr);
        return c.as ? `${e} AS ${quoteIdent(c.as)}` : e;
    });
    if (ast.unset) {
        colSql.push(`UNSET(${ast.unset.map(quoteIdent).join(', ')})`);
    }
    parts.push(colSql.join(', '));

    parts.push('FROM', quoteIdent(ast.from.table));
    if (ast.from.as) {
        parts.push(quoteIdent(ast.from.as));
    }

    for (const join of ast.joins) {
        const aliasToken = join.hint ? `${join.as}|${join.hint}` : join.as;
        parts.push(join.joinType === 'INNER' ? 'INNER JOIN' : 'LEFT JOIN');
        parts.push(quoteIdent(join.table), quoteIdent(aliasToken));
        parts.push('ON', exprToSQL(join.on));
    }

    if (ast.where) {
        parts.push('WHERE', exprToSQL(ast.where));
    }

    if (ast.groupBy && ast.groupBy.length > 0) {
        parts.push('GROUP BY', ast.groupBy.map(exprToSQL).join(', '));
    }
    if (ast.having) {
        parts.push('HAVING', exprToSQL(ast.having));
    }
    if (ast.orderBy && ast.orderBy.length > 0) {
        parts.push(
            'ORDER BY',
            ast.orderBy.map((o) => `${exprToSQL(o.expr)} ${o.dir}`).join(', ')
        );
    }
    if (ast.limit !== null && ast.limit !== undefined) {
        parts.push('LIMIT', String(ast.limit));
    }
    if (ast.offset !== null && ast.offset !== undefined) {
        parts.push('OFFSET', String(ast.offset));
    }

    return parts.join(' ');
}

module.exports = {astToSQL, exprToSQL, columnRefToSQL, literalToSQL};
