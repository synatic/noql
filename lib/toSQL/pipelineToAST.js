const {matchToAST} = require('./matchToAST');
const {expressionToAST, columnRefFromPath} = require('./expressionToAST');

function unsupported(reason) {
    throw new Error(`Unsupported for reverse (pipeline -> SQL) conversion: ${reason}`);
}

function isPlainObject(v) {
    return v && typeof v === 'object' && !Array.isArray(v);
}

/**
 * Recursively rewrites `cast` AST nodes to the equivalent `CONVERT(...)`
 * function-call form. noql's forward converter rejects a bare `CAST` inside
 * a JOIN ON condition (though both compile to the identical $convert
 * shape), so join reconstruction needs this instead of the CAST syntax
 * `expressionToAST` otherwise prefers.
 * @param {object} node
 * @returns {object}
 */
function castToConvert(node) {
    if (!node || typeof node !== 'object') return node;
    if (Array.isArray(node)) return node.map(castToConvert);
    if (node.kind === 'cast') {
        return {
            kind: 'func',
            name: 'CONVERT',
            args: [castToConvert(node.expr), {kind: 'literal', value: node.dataType, wrapped: true}],
        };
    }
    const out = {};
    for (const [k, v] of Object.entries(node)) {
        out[k] = v && typeof v === 'object' ? castToConvert(v) : v;
    }
    return out;
}

/**
 * @param {object} stage
 * @returns {string|null} the alias if this is a `{$project:{[alias]:'$$ROOT'}}` root-wrap stage
 */
function rootWrapAlias(stage) {
    if (!stage.$project) return null;
    const keys = Object.keys(stage.$project);
    if (keys.length === 1 && stage.$project[keys[0]] === '$$ROOT') {
        return keys[0];
    }
    return null;
}

/**
 * Reverses a $lookup (+ optional hint stages + optional null-filter match)
 * group starting at `stages[i]` back into a join AST node.
 * @param {object[]} stages
 * @param {number} i
 * @returns {{join: object, consumed: number}}
 */
function parseJoinGroup(stages, i) {
    const lookupStage = stages[i];
    const lookup = lookupStage.$lookup;
    const as = lookup.as;
    let consumed = 1;
    let hint = null; // 'first' | 'last' | 'unwind' | null
    let onCondition;
    let joinType = 'LEFT';

    if (lookup.pipeline) {
        const subStages = lookup.pipeline;
        const matches = subStages.filter((s) => s.$match);
        if (matches.length !== 1) {
            unsupported(
                `join "${as}" has a $lookup sub-pipeline with ${matches.length} $match stages (expected exactly 1) — likely a joined sub-select, which isn't supported yet`
            );
        }
        const nonMatchNonLimit = subStages.filter((s) => !s.$match && !s.$limit);
        if (nonMatchNonLimit.length > 0) {
            unsupported(`join "${as}" has an unrecognized $lookup sub-pipeline stage`);
        }
        const varMap = {};
        for (const [varName, path] of Object.entries(lookup.let || {})) {
            if (typeof path === 'string' && path.startsWith('$')) {
                varMap[varName] = path.slice(1);
            }
        }
        onCondition = castToConvert(matchToAST(matches[0].$match, varMap));
        if (subStages.some((s) => s.$limit === 1)) {
            hint = 'first';
        }
    } else if (lookup.localField && lookup.foreignField) {
        onCondition = {
            kind: 'binary',
            op: '=',
            left: columnRefFromPath(lookup.localField),
            right: columnRefFromPath(`${as}.${lookup.foreignField}`),
        };
    } else {
        unsupported(`join "${as}" has an unrecognized $lookup shape`);
    }

    // optional hint stage: $set{$first|$last} or $unwind
    const next = stages[i + consumed];
    if (next && next.$set && isPlainObject(next.$set[as])) {
        const setKeys = Object.keys(next.$set[as]);
        if (setKeys.length === 1 && setKeys[0] === '$first' && next.$set[as].$first === `$${as}`) {
            hint = 'first';
            consumed++;
        } else if (setKeys.length === 1 && setKeys[0] === '$last' && next.$set[as].$last === `$${as}`) {
            hint = 'last';
            consumed++;
        }
    } else if (next && next.$unwind) {
        const unwindPath = typeof next.$unwind === 'string' ? next.$unwind : next.$unwind.path;
        if (unwindPath === `$${as}`) {
            hint = 'unwind';
            consumed++;
        }
    }

    // optional null-filter match => marks this as an INNER join
    const afterHint = stages[i + consumed];
    if (afterHint && afterHint.$match) {
        const mk = Object.keys(afterHint.$match);
        if (mk.length === 1 && mk[0] === as && isPlainObject(afterHint.$match[as])) {
            const inner = afterHint.$match[as];
            if (Object.keys(inner).length === 1 && inner.$ne === null) {
                joinType = 'INNER';
                consumed++;
            }
        } else if (
            mk.length === 1 &&
            mk[0] === '$expr' &&
            JSON.stringify(afterHint.$match.$expr) ===
                JSON.stringify({$gt: [{$size: `$${as}`}, 0]})
        ) {
            joinType = 'INNER';
            consumed++;
        }
    }

    return {
        join: {
            joinType,
            table: lookup.from,
            as,
            on: onCondition,
            hint,
        },
        consumed,
    };
}

/**
 * @param {object} projectValue the value of a `{$project: <value>}` stage
 * @returns {'star'|object[]}
 */
function projectToColumns(projectValue, knownAliases) {
    // `_id: 0` is the internal exclude-marker; `_id` mapped to a real
    // expression (e.g. `"$_id"`) is a genuine, explicitly-selected column.
    const entries = Object.entries(projectValue).filter(([k, v]) => !(k === '_id' && v === 0));
    const columns = entries.map(([alias, expr]) => {
        // `{alias: '$alias'}` is the whole-object passthrough produced by
        // `SELECT alias.*` — but only when `alias` really is a declared
        // table/join alias; otherwise it's just an ordinary bare column
        // whose own name happens to match its value (e.g. `sku: '$sku'`).
        if (expr === `$${alias}` && knownAliases.has(alias)) {
            return {expr: {kind: 'star', table: alias}, as: null};
        }
        return {expr: expressionToAST(expr), as: alias};
    });
    return columns;
}

/**
 * Reverses a GROUP BY block: a `$group` stage, its `_id`-unwrap `$project`,
 * and an optional trailing HAVING `$match`.
 * @param {object[]} stages
 * @param {number} i
 * @returns {{result: object, consumed: number}}
 */
function substituteAggregateRefs(value, accumulatorAST) {
    if (typeof value === 'string' && value.startsWith('$_id.')) {
        // the unwrap $project's reference to a GROUP BY column itself — use
        // the real original groupBy expression (it may be a dotted path
        // aliased to a plain name), not a bare column named after the alias
        const key = value.slice('$_id.'.length);
        if (Object.prototype.hasOwnProperty.call(accumulatorAST, key)) {
            return {__astNode: accumulatorAST[key]};
        }
        return {__astNode: {kind: 'column', table: null, name: key}};
    }
    if (typeof value === 'string' && value.startsWith('$') && !value.startsWith('$$')) {
        const key = value.slice(1);
        if (Object.prototype.hasOwnProperty.call(accumulatorAST, key)) {
            return {__astNode: accumulatorAST[key]};
        }
        return value;
    }
    if (Array.isArray(value)) return value.map((v) => substituteAggregateRefs(v, accumulatorAST));
    if (value && typeof value === 'object' && !(value instanceof Date)) {
        const out = {};
        for (const k of Object.keys(value)) out[k] = substituteAggregateRefs(value[k], accumulatorAST);
        return out;
    }
    return value;
}

/**
 * COUNT(DISTINCT field) forces a *second* $group stage: the first groups by
 * the real GROUP BY columns plus a synthetic `_countDistinctTemp` (the
 * distinct target), the second re-groups by just the real columns, turning
 * the distinct-count accumulator back into `{$sum:1}` (now counting distinct
 * sub-groups) while every other accumulator becomes `{$sum:'$alias'}` (or
 * the matching accumulator op) re-aggregating the first group's partials.
 * @param {object[]} stages
 * @param {number} i
 * @returns {{result:object, consumed:number}|null} null if this isn't that shape
 */
function tryParseCountDistinctGroupBlock(stages, i) {
    const group1 = stages[i].$group;
    if (!Object.prototype.hasOwnProperty.call(group1._id, '_countDistinctTemp')) return null;
    const group2Stage = stages[i + 1];
    if (!group2Stage || !group2Stage.$group) return null;
    const group2 = group2Stage.$group;
    let consumed = 2;

    const realIdKeys = Object.keys(group1._id).filter((k) => k !== '_countDistinctTemp');
    const accumulatorAST = {};
    for (const key of realIdKeys) {
        accumulatorAST[key] = expressionToAST(group1._id[key]);
    }
    for (const key of Object.keys(group1)) {
        if (key === '_id') continue;
        const g1 = group1[key];
        const g2 = group2[key];
        const isCountDistinct =
            isPlainObject(g1) &&
            g1.$sum === 1 &&
            isPlainObject(g2) &&
            g2.$sum === 1;
        if (isCountDistinct) {
            accumulatorAST[key] = {
                kind: 'aggr',
                name: 'COUNT',
                distinct: true,
                args: [expressionToAST(group1._id._countDistinctTemp)],
            };
        } else {
            accumulatorAST[key] = groupAccumulatorToAST(g1);
        }
    }

    const groupBy = realIdKeys.map((k) => accumulatorAST[k]);

    let columns;
    const projectStage = stages[i + consumed];
    if (projectStage && projectStage.$project) {
        columns = Object.entries(projectStage.$project)
            .filter(([k]) => k !== '_id')
            .map(([alias, expr]) => ({
                expr: expressionToAST(substituteAggregateRefs(expr, accumulatorAST)),
                as: alias,
            }));
        consumed++;
    } else {
        columns = Object.keys(accumulatorAST).map((key) => ({expr: accumulatorAST[key], as: key}));
    }

    let having = null;
    const havingStage = stages[i + consumed];
    if (havingStage && havingStage.$match) {
        having = matchToAST(havingStage.$match);
        consumed++;
    }

    return {result: {groupBy, having, columns}, consumed};
}

function parseGroupBlock(stages, i) {
    const countDistinctResult = tryParseCountDistinctGroupBlock(stages, i);
    if (countDistinctResult) return countDistinctResult;

    const group = stages[i].$group;
    let consumed = 1;

    const groupBy = Object.keys(group._id).map((k) => expressionToAST(group._id[k]));

    // AST for every $group top-level accumulator, keyed by its field name —
    // used both as a fallback column list (if there's no unwrap $project)
    // and to resolve `_tempAggregateCol_N`-style references the outer
    // projection makes when an aggregate is embedded in a larger expression
    // (e.g. `(MAX(a) + SUM(b)) * MIN(c)`).
    const accumulatorAST = {};
    for (const key of Object.keys(group)) {
        if (key === '_id') continue;
        accumulatorAST[key] = groupAccumulatorToAST(group[key]);
    }
    for (const key of Object.keys(group._id)) {
        accumulatorAST[key] = expressionToAST(group._id[key]);
    }

    let columns;
    const projectStage = stages[i + consumed];
    if (projectStage && projectStage.$project) {
        columns = Object.entries(projectStage.$project)
            .filter(([k]) => k !== '_id')
            .map(([alias, expr]) => ({
                expr: expressionToAST(substituteAggregateRefs(expr, accumulatorAST)),
                as: alias,
            }));
        consumed++;
    } else {
        columns = Object.keys(accumulatorAST).map((key) => ({expr: accumulatorAST[key], as: key}));
    }

    let having = null;
    const havingStage = stages[i + consumed];
    if (havingStage && havingStage.$match) {
        having = matchToAST(havingStage.$match);
        consumed++;
    }

    return {
        result: {groupBy, having, columns},
        consumed,
    };
}

function groupAccumulatorToAST(value) {
    const keys = Object.keys(value);
    if (keys.length !== 1) unsupported(`unrecognized $group accumulator: ${JSON.stringify(value)}`);
    const op = keys[0];
    const raw = value[op];
    if (op === '$sum' && raw === 1) {
        return {kind: 'aggr', name: 'COUNT', args: [{kind: 'star'}]};
    }
    if (op === '$sum') return {kind: 'aggr', name: 'SUM', args: [expressionToAST(raw)]};
    if (op === '$avg') return {kind: 'aggr', name: 'AVG', args: [expressionToAST(raw)]};
    if (op === '$min') return {kind: 'aggr', name: 'MIN', args: [expressionToAST(raw)]};
    if (op === '$max') return {kind: 'aggr', name: 'MAX', args: [expressionToAST(raw)]};
    if (op === '$firstN' || op === '$lastN') {
        // lowercase matters: the grammar only reclassifies a lowercase
        // firstn/lastn function call to an aggregate (see pegjs/noql.pegjs)
        const name = op === '$firstN' ? 'firstn' : 'lastn';
        const nArg = expressionToAST(raw.n);
        if (raw.input === '$$ROOT') {
            return {kind: 'aggr', name, args: [nArg]};
        }
        const fieldName = typeof raw.input === 'string' && raw.input.startsWith('$') ? raw.input.slice(1) : raw.input;
        return {kind: 'aggr', name, args: [nArg, {kind: 'literal', value: fieldName, wrapped: true}]};
    }
    unsupported(`unrecognized $group accumulator operator: ${op}`);
}

/**
 * Recognizes the `{$replaceRoot:{newRoot:{$mergeObjects:['$$ROOT', {...}]}}}`
 * shape produced by `SELECT *, func() AS alias, ...` and reconstructs its
 * column list (a star entry plus the extra computed columns).
 * @param {object} stage
 * @returns {object[]|null}
 */
function replaceRootMergeToColumns(stage) {
    if (!stage.$replaceRoot) return null;
    const newRoot = stage.$replaceRoot.newRoot;
    if (!isPlainObject(newRoot)) return null;
    const keys = Object.keys(newRoot);
    if (keys.length !== 1 || keys[0] !== '$mergeObjects') return null;
    const parts = newRoot.$mergeObjects;
    if (!Array.isArray(parts) || parts.length < 2 || parts[0] !== '$$ROOT') return null;
    const columns = [{expr: {kind: 'star', table: null}, as: null}];
    for (let idx = 1; idx < parts.length; idx++) {
        const part = parts[idx];
        if (!isPlainObject(part)) return null;
        for (const [alias, expr] of Object.entries(part)) {
            columns.push({expr: expressionToAST(expr), as: alias});
        }
    }
    return columns;
}

/**
 * Converts a Mongo aggregation pipeline back into a noql SQL AST.
 * Supports: single FROM table (with alias), INNER/LEFT joins (equi-join and
 * complex ON, with |first/|last/|unwind hints), WHERE, GROUP BY + HAVING,
 * SELECT columns (incl. aggregates/functions/CASE), DISTINCT, ORDER BY,
 * LIMIT/OFFSET, and UNSET(...).
 *
 * NOT supported (throws): FROM sub-selects, UNION/INTERSECT/EXCEPT, PIVOT/
 * UNPIVOT, window functions, FULL JOIN, IN (SELECT ...), array sub-selects.
 * @param {object[]} pipeline
 * @param {string[]} collections
 * @returns {object} a noql SQL AST (see lib/toSQL/astToSQL.js)
 */
function pipelineToAST(pipeline, collections) {
    if (!collections || collections.length === 0) {
        unsupported('no base collection found');
    }
    const stages = pipeline.slice();
    let i = 0;
    const ast = {
        type: 'select',
        distinct: false,
        columns: 'star',
        from: {table: collections[0], as: null},
        joins: [],
        where: null,
        groupBy: null,
        having: null,
        orderBy: null,
        limit: null,
        offset: null,
        unset: null,
    };

    if (stages[i]) {
        const alias = rootWrapAlias(stages[i]);
        if (alias) {
            ast.from.as = alias;
            i++;
        }
    }

    while (stages[i] && stages[i].$lookup) {
        const {join, consumed} = parseJoinGroup(stages, i);
        ast.joins.push(join);
        i += consumed;
    }

    // a $match here (not immediately preceding a $group) is the WHERE clause
    if (stages[i] && stages[i].$match) {
        ast.where = matchToAST(stages[i].$match);
        i++;
    }

    const replaceRootColumns = stages[i] && replaceRootMergeToColumns(stages[i]);
    if (stages[i] && stages[i].$group) {
        const {result, consumed} = parseGroupBlock(stages, i);
        ast.groupBy = result.groupBy;
        ast.having = result.having;
        ast.columns = result.columns;
        i += consumed;
    } else if (replaceRootColumns) {
        ast.columns = replaceRootColumns;
        i++;
    } else if (stages[i] && stages[i].$project) {
        const knownAliases = new Set([ast.from.as, ...ast.joins.map((j) => j.as)].filter(Boolean));
        ast.columns = projectToColumns(stages[i].$project, knownAliases);
        i++;
    }

    // `SELECT <alias> AS '$$ROOT'` — reconstructible only when <alias> is
    // the base FROM alias itself (not some subquery-introduced alias)
    if (
        stages[i] &&
        stages[i].$replaceRoot &&
        ast.from.as &&
        stages[i].$replaceRoot.newRoot === `$${ast.from.as}`
    ) {
        ast.columns = [{expr: {kind: 'column', table: null, name: ast.from.as}, as: '$$ROOT'}];
        i++;
    }

    if (stages[i] && stages[i].$unset) {
        // the internal auto `_id` drop is always the bare string form
        // {$unset:'_id'}; an explicit UNSET(...) SQL call always produces
        // the array form, even for a single field — so only the string
        // form is ever silently dropped here.
        if (stages[i].$unset !== '_id') {
            ast.unset = Array.isArray(stages[i].$unset) ? stages[i].$unset : [stages[i].$unset];
        }
        i++;
    }

    if (stages[i] && stages[i].$sort) {
        ast.orderBy = Object.entries(stages[i].$sort).map(([field, dir]) => ({
            expr: columnRefFromPath(field),
            dir: dir === -1 || dir === '-1' ? 'DESC' : 'ASC',
        }));
        i++;
    }

    if (stages[i] && stages[i].$limit !== undefined) {
        ast.limit = stages[i].$limit;
        i++;
    }
    if (stages[i] && stages[i].$skip !== undefined) {
        ast.offset = stages[i].$skip;
        i++;
    }

    if (i < stages.length) {
        unsupported(`trailing pipeline stage(s) not recognized: ${JSON.stringify(stages[i])}`);
    }

    return ast;
}

module.exports = {pipelineToAST};
