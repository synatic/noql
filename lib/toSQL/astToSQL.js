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
 * @param {number} depth
 * @returns {string}
 */
function indent(depth) {
    return '  '.repeat(depth);
}

/**
 * Serializes a noql SQL AST expression node to SQL text.
 * @param {object} node
 * @param {{pretty?: boolean, depth?: number}} [options]
 * @returns {string}
 */
function exprToSQL(node, options = {}) {
    const pretty = !!options.pretty;
    const depth = options.depth || 0;

    switch (node.kind) {
        case 'column':
            return columnRefToSQL(node);
        case 'star':
            return node.table ? `${quoteIdent(node.table)}.*` : '*';
        case 'literal':
            return literalToSQL(node.value);
        case 'binary':
            return `(${exprToSQL(node.left, options)} ${node.op} ${exprToSQL(node.right, options)})`;
        case 'logical': {
            if (pretty && node.args.length > 1) {
                const inner = indent(depth + 1);
                const close = indent(depth);
                const parts = node.args.map((arg) =>
                    exprToSQL(arg, {...options, depth: depth + 1})
                );
                return `(\n${inner}${parts.join(`\n${inner}${node.op} `)}\n${close})`;
            }
            return `(${node.args.map((arg) => exprToSQL(arg, options)).join(` ${node.op} `)})`;
        }
        case 'not':
            return `(NOT ${exprToSQL(node.expr, options)})`;
        case 'isNull':
            return `(${exprToSQL(node.expr, options)} IS ${node.negate ? 'NOT ' : ''}NULL)`;
        case 'like':
            return `(${exprToSQL(node.expr, options)} ${node.negate ? 'NOT LIKE' : 'LIKE'} ${literalToSQL(node.pattern)})`;
        case 'in': {
            const list = Array.isArray(node.list)
                ? `(${node.list.map((item) => exprToSQL(item, options)).join(', ')})`
                : exprToSQL(node.list, options);
            return `(${exprToSQL(node.expr, options)} ${node.negate ? 'NOT IN' : 'IN'} ${list})`;
        }
        case 'func':
            return `${node.name}(${node.args.map((arg) => exprToSQL(arg, options)).join(', ')})`;
        case 'aggr':
            return `${node.name}(${node.distinct ? 'DISTINCT ' : ''}${node.args.map((arg) => exprToSQL(arg, options)).join(', ')})`;
        case 'cast':
            return `CAST(${exprToSQL(node.expr, options)} AS ${node.dataType})`;
        case 'case': {
            if (pretty) {
                const inner = indent(depth + 1);
                const close = indent(depth);
                const branches = node.branches
                    .map(
                        (b) =>
                            `${inner}WHEN ${exprToSQL(b.when, {...options, depth: depth + 1})} THEN ${exprToSQL(b.then, {...options, depth: depth + 1})}`
                    )
                    .join('\n');
                return `(CASE\n${branches}\n${inner}ELSE ${exprToSQL(node.else, {...options, depth: depth + 1})}\n${close}END)`;
            }
            const branches = node.branches
                .map((b) => `WHEN ${exprToSQL(b.when, options)} THEN ${exprToSQL(b.then, options)}`)
                .join(' ');
            return `(CASE ${branches} ELSE ${exprToSQL(node.else, options)} END)`;
        }
        default:
            throw new Error(`Unsupported AST expression node kind: ${node.kind}`);
    }
}

/**
 * Serializes a noql SQL AST (as produced by lib/toSQL/pipelineToAST.js) back
 * into a SQL string.
 * @param {object} ast
 * @param {{pretty?: boolean, depth?: number}} [options]
 * @returns {string}
 */
function astToSQL(ast, options = {}) {
    const pretty = !!options.pretty;
    const depth = options.depth || 0;
    const pad = indent(depth);
    const pad1 = indent(depth + 1);
    const exprOpts = {pretty, depth: depth + 1};

    const colSql =
        ast.columns === 'star'
            ? ['*']
            : ast.columns.map((c) => {
                  const e = exprToSQL(c.expr, exprOpts);
                  return c.as ? `${e} AS ${quoteIdent(c.as)}` : e;
              });
    if (ast.unset) {
        colSql.push(`UNSET(${ast.unset.map(quoteIdent).join(', ')})`);
    }

    if (!pretty) {
        const parts = ['SELECT', colSql.join(', '), 'FROM', quoteIdent(ast.from.table)];
        if (ast.from.as) {
            parts.push(quoteIdent(ast.from.as));
        }

        for (const join of ast.joins) {
            const hintTokens = [join.hint, join.optimizeHint ? 'optimize' : null].filter(Boolean);
            const aliasToken = hintTokens.length ? `${join.as}|${hintTokens.join('|')}` : join.as;
            const tableSql =
                join.table && typeof join.table === 'object' && join.table.subquery
                    ? `(${astToSQL(join.table.subquery, options)})`
                    : quoteIdent(join.table);
            parts.push(join.joinType === 'INNER' ? 'INNER JOIN' : 'LEFT JOIN');
            parts.push(tableSql, quoteIdent(aliasToken));
            parts.push('ON', exprToSQL(join.on, options));
        }

        if (ast.where) {
            parts.push('WHERE', exprToSQL(ast.where, options));
        }
        if (ast.groupBy && ast.groupBy.length > 0) {
            parts.push('GROUP BY', ast.groupBy.map((g) => exprToSQL(g, options)).join(', '));
        }
        if (ast.having) {
            parts.push('HAVING', exprToSQL(ast.having, options));
        }
        if (ast.orderBy && ast.orderBy.length > 0) {
            parts.push(
                'ORDER BY',
                ast.orderBy.map((o) => `${exprToSQL(o.expr, options)} ${o.dir}`).join(', ')
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

    const lines = [];
    lines.push(`${pad}SELECT`);
    colSql.forEach((col, i) => {
        const suffix = i < colSql.length - 1 ? ',' : '';
        lines.push(`${pad1}${col}${suffix}`);
    });

    let fromLine = `${pad}FROM ${quoteIdent(ast.from.table)}`;
    if (ast.from.as) {
        fromLine += ` ${quoteIdent(ast.from.as)}`;
    }
    lines.push(fromLine);

    for (const join of ast.joins) {
        const hintTokens = [join.hint, join.optimizeHint ? 'optimize' : null].filter(Boolean);
        const aliasToken = hintTokens.length ? `${join.as}|${hintTokens.join('|')}` : join.as;
        const joinKw = join.joinType === 'INNER' ? 'INNER JOIN' : 'LEFT JOIN';

        if (join.table && typeof join.table === 'object' && join.table.subquery) {
            lines.push(`${pad}${joinKw} (`);
            lines.push(astToSQL(join.table.subquery, {pretty: true, depth: depth + 1}));
            lines.push(`${pad}) ${quoteIdent(aliasToken)}`);
        } else {
            lines.push(
                `${pad}${joinKw} ${quoteIdent(join.table)} ${quoteIdent(aliasToken)}`
            );
        }
        lines.push(`${pad1}ON ${exprToSQL(join.on, exprOpts)}`);
    }

    if (ast.where) {
        lines.push(`${pad}WHERE ${exprToSQL(ast.where, exprOpts)}`);
    }
    if (ast.groupBy && ast.groupBy.length > 0) {
        lines.push(
            `${pad}GROUP BY ${ast.groupBy.map((g) => exprToSQL(g, exprOpts)).join(', ')}`
        );
    }
    if (ast.having) {
        lines.push(`${pad}HAVING ${exprToSQL(ast.having, exprOpts)}`);
    }
    if (ast.orderBy && ast.orderBy.length > 0) {
        lines.push(
            `${pad}ORDER BY ${ast.orderBy.map((o) => `${exprToSQL(o.expr, exprOpts)} ${o.dir}`).join(', ')}`
        );
    }
    if (ast.limit !== null && ast.limit !== undefined) {
        lines.push(`${pad}LIMIT ${ast.limit}`);
    }
    if (ast.offset !== null && ast.offset !== undefined) {
        lines.push(`${pad}OFFSET ${ast.offset}`);
    }

    return lines.join('\n');
}

module.exports = {astToSQL, exprToSQL, columnRefToSQL, literalToSQL};
