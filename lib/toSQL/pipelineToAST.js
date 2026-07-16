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
 * Prefixes bare (un-aliased) field references in a raw $match filter value
 * with the base-table alias. `optimizeMongoAggregate` hoists a $match above
 * the root-wrap $project and strips the alias prefix off its field names
 * (since it runs before the document gets wrapped under the alias) so it can
 * run against a real index on the raw collection — this reverses that.
 * @param {*} value
 * @param {string} alias
 * @returns {*}
 */
function prefixBareFieldReferences(value, alias) {
    if (Array.isArray(value)) return value.map((v) => prefixBareFieldReferences(v, alias));
    if (value && typeof value === 'object' && !(value instanceof Date)) {
        const out = {};
        for (const [k, v] of Object.entries(value)) {
            const newKey = k.startsWith('$') ? k : `${alias}.${k}`;
            out[newKey] = prefixBareFieldReferences(v, alias);
        }
        return out;
    }
    if (typeof value === 'string' && value.startsWith('$') && !value.startsWith('$$')) {
        return `$${alias}.${value.slice(1)}`;
    }
    return value;
}

/**
 * @param {*} value a raw (not-yet-converted) Mongo expression value
 * @param {Set<string>} varNames `let`-bound variable names (without the `$$`)
 * @returns {boolean} whether this expression tree references any of them
 */
function referencesAnyVar(value, varNames) {
    if (typeof value === 'string') return value.startsWith('$$') && varNames.has(value.slice(2));
    if (Array.isArray(value)) return value.some((v) => referencesAnyVar(v, varNames));
    if (value && typeof value === 'object') return Object.values(value).some((v) => referencesAnyVar(v, varNames));
    return false;
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
    let optimizeHint = false;
    let onCondition;
    let joinTable = lookup.from;
    let joinType = 'LEFT';

    if (lookup.pipeline) {
        let subStages = lookup.pipeline.slice();
        const varMap = {};
        const varNames = new Set(Object.keys(lookup.let || {}));
        for (const [varName, path] of Object.entries(lookup.let || {})) {
            if (typeof path === 'string' && path.startsWith('$')) {
                varMap[varName] = path.slice(1);
            }
        }

        // a trailing {$limit:1} is always the |first hint's limit (the
        // forward converter appends it last regardless of the |optimize
        // hint's match-ordering), never a derived table's own SQL LIMIT
        if (subStages.length && subStages[subStages.length - 1].$limit === 1) {
            hint = 'first';
            subStages = subStages.slice(0, -1);
        }

        // the one $match whose $expr correlates back to the outer query (via
        // a `let`-bound variable) is this join's own ON condition; anything
        // else in the sub-pipeline is the joined derived table's own
        // WHERE/joins/SELECT — i.e. a `JOIN (SELECT ...) alias` construct
        const correlationIdxs = [];
        subStages.forEach((s, idx) => {
            if (s.$match && s.$match.$expr && referencesAnyVar(s.$match.$expr, varNames)) {
                correlationIdxs.push(idx);
            }
        });
        if (correlationIdxs.length !== 1) {
            unsupported(
                `join "${as}" has a $lookup sub-pipeline with ${correlationIdxs.length} correlated $match stage(s) (expected exactly 1)`
            );
        }
        const correlationMatch = subStages[correlationIdxs[0]];
        const remainingStages = subStages.filter((_, idx) => idx !== correlationIdxs[0]);

        if (remainingStages.length > 0) {
            // this join's target is a derived table (`JOIN (SELECT ...)
            // alias`) — any own-side reference in the correlation condition
            // (e.g. a nested join alias from *inside* the sub-pipeline, or
            // its own root-wrap alias) is only reachable from outside the
            // parens via the derived table's outer alias, so requalify it
            // (e.g. "$amspolicies.CustomerId" -> "$SHazards.amspolicies.CustomerId")
            const requalifiedMatch = prefixBareFieldReferences(correlationMatch.$match, as);
            onCondition = castToConvert(matchToAST(requalifiedMatch, varMap));
            const nestedAst = pipelineToAST(remainingStages, [lookup.from]);
            joinTable = {subquery: nestedAst};
            // the |optimize hint unshifts the correlation match to the front
            // of the sub-pipeline (ahead of the derived table's own
            // content); the default (unhinted) form pushes it to the end
            optimizeHint = correlationIdxs[0] === 0;
        } else {
            onCondition = castToConvert(matchToAST(correlationMatch.$match, varMap));
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
            table: joinTable,
            as,
            on: onCondition,
            hint,
            optimizeHint,
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
 * complex ON, with |first/|last/|unwind/|optimize hints, incl. joins against
 * a derived table — `JOIN (SELECT ...) alias`), WHERE (incl. reassembling
 * WHERE fragments that `optimizeJoins`/`optimizeMongoAggregate` hoisted above
 * joins/the root-wrap project), GROUP BY + HAVING, SELECT columns (incl.
 * aggregates/functions/CASE), DISTINCT, ORDER BY, LIMIT/OFFSET, and UNSET(...).
 *
 * NOT supported (throws): FROM sub-selects (the outer query's own FROM, as
 * opposed to a joined derived table), UNION/INTERSECT/EXCEPT, PIVOT/UNPIVOT,
 * window functions, FULL JOIN, IN (SELECT ...), array sub-selects.
 * @param {object[]} pipeline
 * @param {string[]} collections
 * @returns {object} a noql SQL AST (see lib/toSQL/astToSQL.js)
 */
function pipelineToAST(pipeline, collections) {
    if (!collections || collections.length === 0) {
        unsupported('no base collection found');
    }
    let stages = pipeline.slice();
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

    // Locate the root-wrap alias stage anywhere in the pipeline — not just at
    // position 0. `optimizeJoins`/`optimizeMongoAggregate` can hoist WHERE
    // fragments above it (and above joins) for index-friendly filtering, so
    // it doesn't always lead. Stages that originally preceded it had their
    // alias prefix stripped by that hoist (see prefixBareFieldReferences).
    // But a FROM sub-select (`FROM (SELECT ...) alias` — unsupported, should
    // throw, not silently reinterpret) produces this exact same shape: its
    // entire compiled body (which can be $group/$project/joins/anything)
    // precedes its own root-wrap too. Only treat a non-leading root-wrap as
    // an optimizer hoist when *every* stage ahead of it is a plain $match —
    // that's all the optimizer ever hoists; anything else ahead of it means
    // this isn't that pattern, so fall back to only recognizing a leading one.
    let rootWrapIdx = stages.findIndex((s) => rootWrapAlias(s));
    if (rootWrapIdx > 0 && !stages.slice(0, rootWrapIdx).every((s) => !!s.$match)) {
        rootWrapIdx = stages[0] && rootWrapAlias(stages[0]) ? 0 : -1;
    }
    let alias = null;
    if (rootWrapIdx > -1) {
        alias = rootWrapAlias(stages[rootWrapIdx]);
        ast.from.as = alias;
    }
    const preWrap = stages.map((_, idx) => rootWrapIdx > -1 && idx < rootWrapIdx);
    if (rootWrapIdx > -1) {
        stages.splice(rootWrapIdx, 1);
        preWrap.splice(rootWrapIdx, 1);
    }

    let i = 0;
    const whereFragments = [];
    while (stages[i] && (stages[i].$lookup || stages[i].$match)) {
        if (stages[i].$lookup) {
            const {join, consumed} = parseJoinGroup(stages, i);
            ast.joins.push(join);
            i += consumed;
            continue;
        }
        // a stray $match (not consumed as part of a join group above) is a
        // WHERE-clause fragment — possibly one of several the optimizer
        // scattered across the pipeline (only ever splitting the WHERE's
        // top-level AND, never an OR, so AND-ing them back together here is
        // always faithful to the original clause)
        const value = preWrap[i] && alias ? prefixBareFieldReferences(stages[i].$match, alias) : stages[i].$match;
        whereFragments.push(value);
        i++;
    }
    if (whereFragments.length === 1) {
        ast.where = matchToAST(whereFragments[0]);
    } else if (whereFragments.length > 1) {
        ast.where = {kind: 'logical', op: 'AND', args: whereFragments.map((f) => matchToAST(f))};
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
