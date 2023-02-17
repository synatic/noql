const $json = require('@synatic/json-magic');
const _allowableFunctions = require('../MongoFunctions');

/**
 * Check if a group by needs to be forced
 *
 * @param {import('../types').AST} ast -   the ast to check
 * @returns {boolean}
 */
function forceGroupBy(ast) {
    if (ast.groupby) {
        return false;
    }

    const potentialFuncs = [];
    $json.walk(ast, (val, path) => {
        const pathParts = path.split('/').slice(1);
        if (val === 'aggr_func') {
            potentialFuncs.push(
                pathParts.slice(0, pathParts.length - 1).join('.')
            );
        }
    });
    let forceGroupBy = false;

    for (const potentialFunc of potentialFuncs) {
        const funcObj = $json.get(ast, potentialFunc);
        if (funcObj && funcObj.name && !potentialFunc.startsWith('from.')) {
            const definition = _allowableFunctions.functionByNameAndType(
                funcObj.name,
                'aggr_func'
            );
            forceGroupBy =
                forceGroupBy || (definition && definition.forceGroup);
        }
    }

    return forceGroupBy;
}

module.exports = {forceGroupBy};
