const {OPERATOR_MAP, CONVERT_TYPE_MAP} = require('./functionReverseMap');

/**
 * Splits a Mongo field-path string (without the leading `$`) into a noql
 * column-ref AST node. A single segment is a bare column; 2+ segments are
 * treated as `<alias>.<rest>` — matching how the forward converter treats
 * any dotted reference, real alias or not.
 * @param {string} path
 * @returns {object}
 */
function columnRefFromPath(path) {
    const segments = path.split('.');
    if (segments.length === 1) {
        return {kind: 'column', table: null, name: segments[0]};
    }
    return {kind: 'column', table: segments[0], name: segments.slice(1).join('.')};
}

/**
 * Detects whether a Mongo regex source string could only have come from a
 * noql LIKE/NOT LIKE translation (see lib/make/sqlStringToRegex.js): all
 * metacharacters pre-escaped, with at most a leading `^`/trailing `$`
 * anchor surviving unescaped.
 * @param {string} source
 * @returns {{pattern: string}|null}
 */
function likePatternFromRegex(source) {
    let body = source;
    let startsWith = false;
    let endsWith = false;
    if (body.startsWith('^')) {
        body = body.slice(1);
        startsWith = true;
    }
    if (body.endsWith('$') && !body.endsWith('\\$')) {
        body = body.slice(0, -1);
        endsWith = true;
    }
    // any surviving unescaped regex metacharacter means this wasn't a plain
    // %-wildcard LIKE pattern
    if (/(?<!\\)[.*+?^${}()|[\]\\]/.test(body)) {
        return null;
    }
    const unescaped = body.replace(/\\(.)/g, '$1');
    const pattern = `${startsWith ? '' : '%'}${unescaped}${endsWith ? '' : '%'}`;
    return {pattern};
}

/**
 * @param {*} value
 * @returns {boolean} whether this value tree contains a `$field`-style
 *   reference anywhere, i.e. it isn't genuinely static data
 */
function containsFieldReference(value) {
    if (typeof value === 'string') return value.startsWith('$');
    if (Array.isArray(value)) return value.some(containsFieldReference);
    if (value && typeof value === 'object' && !(value instanceof Date)) {
        return Object.values(value).some(containsFieldReference);
    }
    return false;
}

/**
 * Converts a Mongo aggregation expression value into a noql SQL AST
 * expression node.
 * @param {*} value - the Mongo expression value
 * @param {object} [varMap] - `$$name` -> raw Mongo path substitutions
 *   (used when reversing a $lookup's `let`-bound pipeline expression)
 * @returns {object}
 */
function expressionToAST(value, varMap = {}) {
    // pre-substituted AST node (see pipelineToAST.js's group-by
    // accumulator-reference substitution) — return it verbatim.
    if (value && typeof value === 'object' && !Array.isArray(value) && value.__astNode) {
        return value.__astNode;
    }
    if (value === null) {
        return {kind: 'literal', value: null};
    }
    if (typeof value === 'string') {
        if (value.startsWith('$$')) {
            if (value === '$$NOW') {
                return {kind: 'func', name: 'CURRENT_DATE', args: []};
            }
            const varName = value.slice(2);
            if (Object.prototype.hasOwnProperty.call(varMap, varName)) {
                return columnRefFromPath(varMap[varName]);
            }
            throw new Error(`Unsupported reverse conversion for variable reference: ${value}`);
        }
        if (value.startsWith('$')) {
            return columnRefFromPath(value.slice(1));
        }
        return {kind: 'literal', value, wrapped: false};
    }
    if (typeof value !== 'object' || value instanceof Date) {
        return {kind: 'literal', value, wrapped: false};
    }
    if (Array.isArray(value)) {
        return {kind: 'literal', value, wrapped: false};
    }

    const keys = Object.keys(value);
    if (keys.length === 1 && keys[0].startsWith('$')) {
        return operatorToAST(keys[0], value[keys[0]], varMap);
    }

    // A plain object with no operator key is usually a genuine data literal
    // (e.g. from PARSE_JSON(...)) — but an object-construction sub-select
    // (`(SELECT a, (SELECT d) AS b)`, unsupported) produces the same "plain
    // object" shape while still containing live `$field` references. Refuse
    // rather than silently reconstruct it as a JSON literal.
    if (containsFieldReference(value)) {
        throw new Error(
            'Unsupported reverse conversion for an object-construction sub-select (a plain object expression containing live field references)'
        );
    }
    return {kind: 'literal', value, wrapped: false};
}

const SYMMETRIC_COMPARISON = {
    $eq: '=',
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $ne: '!=',
};

const ARITHMETIC_SYMBOL = {
    $add: '+',
    $subtract: '-',
    $multiply: '*',
    $divide: '/',
    $mod: '%',
};

const ARITHMETIC_FUNCTION = {
    $add: 'SUM',
    $subtract: 'SUBTRACT',
    $multiply: 'MULTIPLY',
    $divide: 'DIVIDE',
    $mod: 'MOD',
};

const COMPARISON_FUNCTION = {
    $eq: 'EQ',
    $gt: 'GT',
    $gte: 'GTE',
    $lt: 'LT',
    $lte: 'LTE',
    $ne: 'NE',
};

// a $literal-wrapped operand can only be reproduced by re-emitting a
// function-call SQL form (LOWER(x), EQ(a,b), DIVIDE(a,b), ...) — symbol
// operators (a = b, a / b) never re-wrap their literal operands, so using
// one here would silently change the reconstructed pipeline shape.
function hasWrappedLiteral(nodes) {
    return nodes.some((n) => n.kind === 'literal' && n.wrapped);
}

function operatorToAST(op, raw, varMap) {
    if (op === '$literal') {
        return {kind: 'literal', value: raw, wrapped: true};
    }

    if (SYMMETRIC_COMPARISON[op]) {
        const [left, right] = raw;
        const leftAST = expressionToAST(left, varMap);
        const rightAST = expressionToAST(right, varMap);
        if (hasWrappedLiteral([leftAST, rightAST])) {
            return {kind: 'func', name: COMPARISON_FUNCTION[op], args: [leftAST, rightAST]};
        }
        return {kind: 'binary', op: SYMMETRIC_COMPARISON[op], left: leftAST, right: rightAST};
    }

    if (ARITHMETIC_SYMBOL[op]) {
        if (Array.isArray(raw) && raw.length === 2) {
            const leftAST = expressionToAST(raw[0], varMap);
            const rightAST = expressionToAST(raw[1], varMap);
            if (!hasWrappedLiteral([leftAST, rightAST])) {
                return {kind: 'binary', op: ARITHMETIC_SYMBOL[op], left: leftAST, right: rightAST};
            }
            return {kind: 'func', name: ARITHMETIC_FUNCTION[op], args: [leftAST, rightAST]};
        }
        return {
            kind: 'func',
            name: ARITHMETIC_FUNCTION[op],
            args: (Array.isArray(raw) ? raw : [raw]).map((v) => expressionToAST(v, varMap)),
        };
    }

    if (op === '$and' || op === '$or') {
        return {
            kind: 'logical',
            op: op === '$and' ? 'AND' : 'OR',
            args: raw.map((v) => expressionToAST(v, varMap)),
        };
    }

    if (op === '$not') {
        // {$not: [expr]} - unary boolean negation (the only shape noql's
        // forward converter itself produces for a bare NOT)
        const inner = Array.isArray(raw) ? raw[0] : raw;
        if (inner && typeof inner === 'object' && !Array.isArray(inner)) {
            const innerKeys = Object.keys(inner);
            if (innerKeys.length === 1 && innerKeys[0] === '$regexMatch') {
                const like = regexMatchToLike(inner.$regexMatch, varMap, true);
                if (like) return like;
            }
        }
        const innerAST = expressionToAST(inner, varMap);
        // noql's grammar has no general `NOT (expr)` wrapping an IN — only
        // the dedicated `x NOT IN (...)` form — so collapse rather than wrap
        if (innerAST.kind === 'in') {
            return {...innerAST, negate: !innerAST.negate};
        }
        return {kind: 'not', expr: innerAST};
    }

    if (op === '$in' || op === '$nin') {
        const [needle, haystack] = raw;
        return {
            kind: 'in',
            negate: op === '$nin',
            expr: expressionToAST(needle, varMap),
            list: Array.isArray(haystack)
                ? haystack.map((v) => expressionToAST(v, varMap))
                : expressionToAST(haystack, varMap),
        };
    }

    if (op === '$cond') {
        const [ifExpr, thenExpr, elseExpr] = Array.isArray(raw)
            ? raw
            : [raw.if, raw.then, raw.else];
        return {
            kind: 'case',
            branches: [{when: expressionToAST(ifExpr, varMap), then: expressionToAST(thenExpr, varMap)}],
            else: expressionToAST(elseExpr, varMap),
        };
    }

    if (op === '$switch') {
        return {
            kind: 'case',
            branches: raw.branches.map((b) => ({
                when: expressionToAST(b.case, varMap),
                then: expressionToAST(b.then, varMap),
            })),
            else: expressionToAST(raw.default, varMap),
        };
    }

    if (op === '$regexMatch') {
        const like = regexMatchToLike(raw, varMap, false);
        if (like) return like;
        throw new Error('Unsupported reverse conversion for a non-LIKE $regexMatch expression');
    }

    if (op === '$convert') {
        const toType = CONVERT_TYPE_MAP[raw.to];
        if (!toType) {
            throw new Error(`Unsupported reverse conversion for $convert target type: ${raw.to}`);
        }
        // CAST(x AS type): unlike CONVERT(x, 'type') (an ordinary function
        // call, whose literal args get $literal-wrapped), CAST extracts a
        // literal input's raw value directly — matching what a literal
        // $convert.input (unwrapped) requires to round-trip exactly.
        return {
            kind: 'cast',
            expr: expressionToAST(raw.input, varMap),
            dataType: toType,
        };
    }

    if (op === '$first') {
        return {kind: 'func', name: 'FIRST_IN_ARRAY', args: [expressionToAST(raw, varMap)]};
    }
    if (op === '$last') {
        return {kind: 'func', name: 'LAST_IN_ARRAY', args: [expressionToAST(raw, varMap)]};
    }

    if (op === '$dateFromParts') {
        // two SQL functions (date_from_parts / date_from_iso_parts) collapse
        // to this same Mongo operator with different field sets — tell them
        // apart by which fields are actually present.
        const isIso = Object.prototype.hasOwnProperty.call(raw, 'isoWeekYear');
        const order = isIso
            ? ['isoWeekYear', 'isoWeek', 'isoDayOfWeek', 'hour', 'minute', 'second', 'millisecond', 'timezone']
            : ['year', 'month', 'day', 'hour', 'minute', 'second', 'millisecond', 'timezone'];
        return {
            kind: 'func',
            name: isIso ? 'DATE_FROM_ISO_PARTS' : 'DATE_FROM_PARTS',
            args: order
                .filter((k) => raw[k] !== undefined)
                .map((k) => expressionToAST(raw[k], varMap)),
        };
    }

    const mapped = OPERATOR_MAP[op];
    if (mapped) {
        return {
            kind: 'func',
            name: mapped.sqlName,
            args: mapped.argsOf(raw).map((v) => expressionToAST(v, varMap)),
        };
    }

    throw new Error(`Unsupported reverse conversion for Mongo operator: ${op}`);
}

/**
 * @param {{input:*, regex:*, options?:string}} regexMatch
 * @param {object} varMap
 * @param {boolean} negate
 * @returns {object|null}
 */
function regexMatchToLike(regexMatch, varMap, negate) {
    if (typeof regexMatch.regex !== 'string' || regexMatch.options !== 'i') {
        return null;
    }
    const like = likePatternFromRegex(regexMatch.regex);
    if (!like) return null;
    return {
        kind: 'like',
        negate,
        expr: expressionToAST(regexMatch.input, varMap),
        pattern: like.pattern,
    };
}

module.exports = {expressionToAST, columnRefFromPath, likePatternFromRegex};
