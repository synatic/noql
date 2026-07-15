const {expressionToAST, columnRefFromPath, likePatternFromRegex} = require('./expressionToAST');

function literalToAST(value) {
    return {kind: 'literal', value};
}

const COMPARISON_OP_TO_SQL = {
    $eq: '=',
    $gt: '>',
    $gte: '>=',
    $lt: '<',
    $lte: '<=',
    $ne: '!=',
};

/**
 * Converts the value of a plain (non-$expr) field condition into an AST node.
 * @param {string} field
 * @param {*} cond
 * @returns {object}
 */
function fieldConditionToAST(field, cond) {
    // FIELD_EXISTS(col, bool) — a bare column-ref argument — forward-
    // converts to a match key that is itself the `$`-prefixed Mongo field
    // path (e.g. {"$id": {$exists: true}}); FIELD_EXISTS('name', bool) — a
    // string-literal argument — instead produces a bare key. Strip the `$`
    // for the column case; the fieldName-arg branch below re-checks this.
    const fieldHasDollarPrefix = field.startsWith('$');
    const colNode = columnRefFromPath(fieldHasDollarPrefix ? field.slice(1) : field);

    if (cond === null) {
        return {kind: 'isNull', expr: colNode, negate: false};
    }
    if (typeof cond !== 'object' || Array.isArray(cond)) {
        return {kind: 'binary', op: '=', left: colNode, right: literalToAST(cond)};
    }

    const condKeys = Object.keys(cond);

    if (condKeys.length === 1 && condKeys[0] === '$exists') {
        const fieldArg = fieldHasDollarPrefix ? colNode : literalToAST(field);
        return {
            kind: 'func',
            name: 'FIELD_EXISTS',
            args: [fieldArg, literalToAST(cond.$exists)],
        };
    }

    if (condKeys.includes('$regex')) {
        const like = likePatternFromRegex(cond.$regex);
        if (like && (cond.$options === undefined || cond.$options === 'i')) {
            return {kind: 'like', negate: false, expr: colNode, pattern: like.pattern};
        }
    }

    if (condKeys.length === 1 && condKeys[0] === '$not' && cond.$not && typeof cond.$not === 'object') {
        const notKeys = Object.keys(cond.$not);
        if (notKeys.includes('$regex')) {
            const like = likePatternFromRegex(cond.$not.$regex);
            if (like && (cond.$not.$options === undefined || cond.$not.$options === 'i')) {
                return {kind: 'like', negate: true, expr: colNode, pattern: like.pattern};
            }
        }
    }

    if (condKeys.length === 1 && COMPARISON_OP_TO_SQL[condKeys[0]]) {
        const opKey = condKeys[0];
        const val = cond[opKey];
        if (val === null && (opKey === '$eq' || opKey === '$ne')) {
            return {kind: 'isNull', expr: colNode, negate: opKey === '$ne'};
        }
        return {
            kind: 'binary',
            op: COMPARISON_OP_TO_SQL[opKey],
            left: colNode,
            right: literalToAST(val),
        };
    }

    if (condKeys.length === 1 && (condKeys[0] === '$in' || condKeys[0] === '$nin')) {
        return {
            kind: 'in',
            negate: condKeys[0] === '$nin',
            expr: colNode,
            list: cond[condKeys[0]].map(literalToAST),
        };
    }

    throw new Error(
        `Unsupported reverse conversion for match condition on field "${field}": ${JSON.stringify(cond)}`
    );
}

/**
 * Converts a $match stage's filter object (the value of `{$match: <value>}`)
 * into a noql SQL AST WHERE/HAVING expression node.
 * @param {object} matchValue
 * @param {object} [varMap]
 * @returns {object}
 */
function matchToAST(matchValue, varMap = {}) {
    const keys = Object.keys(matchValue);

    if (keys.length === 1 && keys[0] === '$expr') {
        return expressionToAST(matchValue.$expr, varMap);
    }
    if (keys.length === 1 && keys[0] === '$and') {
        const args = matchValue.$and.map((c) => matchToAST(c, varMap));
        return args.length === 1 ? args[0] : {kind: 'logical', op: 'AND', args};
    }
    if (keys.length === 1 && keys[0] === '$or') {
        const args = matchValue.$or.map((c) => matchToAST(c, varMap));
        return args.length === 1 ? args[0] : {kind: 'logical', op: 'OR', args};
    }
    if (keys.length === 1 && keys[0] === '$nor') {
        const inner =
            matchValue.$nor.length === 1
                ? matchToAST(matchValue.$nor[0], varMap)
                : {kind: 'logical', op: 'OR', args: matchValue.$nor.map((c) => matchToAST(c, varMap))};
        return {kind: 'not', expr: inner};
    }

    const conds = keys.map((k) => fieldConditionToAST(k, matchValue[k]));
    return conds.length === 1 ? conds[0] : {kind: 'logical', op: 'AND', args: conds};
}

module.exports = {matchToAST, fieldConditionToAST};
