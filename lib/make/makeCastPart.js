const $check = require('check-types');
const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const set = require('lodash/set');

exports.makeCastPart = makeCastPart;

/**
 * @typedef {import("json-schema").JSONSchema6TypeName} JSONSchema6TypeName
 * @typedef {import("json-schema").JSONSchema6} JSONSchema6
 */

/**
 * Makes an mongo expression tree from the cast statement
 *
 * @param {import("../types").Expression} expr - the AST expression that is a cast
 * @param {import("../types").NoqlContext} context - The Noql context to use when generating the output
 * @returns {*}
 */
function makeCastPart(expr, context) {
    if (expr.type !== 'cast') {
        throw new Error(`Invalid type for cast:${expr.type}`);
    }
    const convertFunction = _allowableFunctions.functionByName('convert');
    if (!convertFunction) {
        throw new Error('No conversion function found');
    }
    const to = expr.target.dataType.toLowerCase();

    /** @type {string} */
    let from;
    if (expr.expr.column) {
        from = `$${expr.expr.table ? expr.expr.table + '.' : ''}${
            expr.expr.column
        }`;
    } else if (expr.expr.value) {
        from = expr.expr.value;
    } else {
        from = makeProjectionExpressionPartModule.makeProjectionExpressionPart(
            expr.expr,
            context
        );
    }
    if (
        to === 'varchar' &&
        context.schemas &&
        Object.keys(context.schemas).length
    ) {
        const fromCol = from.replace(/\$/g, '');
        const foundSchema = findSchema(context.schemas, fromCol, context);
        if (foundSchema) {
            const projection = buildStringifyProjection(foundSchema, fromCol);
            if (projection) {
                return projection;
            }
            // todo RK, what if you are casting a number column to a varchar, need to be able to check for that and not do a reduce
        }
    }
    return convertFunction.parse([from, to]);
}

/**
 * @param {import("../types").Schemas} schemas
 * @param {string} column
 * @param {import("../types").NoqlContext}context
 * @returns {import("../types").JSONSchema6}
 */
function findSchema(schemas, column, context) {
    // const tableAliases = context.fullAst.ast.from;
    const parts = column.split('.');
    // const colAlias = parts.length > 1 ? parts[0] : null;
    const colName = parts.length > 1 ? parts[1] : column;
    // const foundTableAlias = colAlias
    //     ? tableAliases.find(
    //           (ta) => ta.as && ta.as.toLowerCase() === colAlias.toLowerCase()
    //       )
    //     : null;
    // eslint-disable-next-line guard-for-in
    for (const collectionName in schemas) {
        const schema = schemas[collectionName];
        const result = findSubSchema(schema, colName);
        if (result) {
            return result;
        }
    }
    return null;
}

/**
 * @param {import("../types").JSONSchema6} schema
 * @param {string} column
 * @returns {import("../types").JSONSchema6}
 */
function findSubSchema(schema, column) {
    if (!schema.properties) {
        return null;
    }
    // eslint-disable-next-line guard-for-in
    for (const key in schema.properties) {
        const value = schema.properties[key];
        if ($check.boolean(value)) {
            continue;
        }
        if (key.toLowerCase() === column.toLowerCase()) {
            return value;
        }
        if (
            value.type === 'object' ||
            (Array.isArray(value.type) && value.type.indexOf('object') >= 0)
        ) {
            const res = findSubSchema(value, column);
            if (res) {
                return res;
            }
        }
    }
    return null;
}

/**
 * support for non object/array types
 * update the , between object props to not be there when empty
 */

/**
 *
 * @param {import("../types").JSONSchema6} schema
 * @param {string} from
 * @returns {*}
 */
function buildStringifyProjection(schema, from) {
    const schemaTypes = getTypes(schema.type);
    const typeMap = {};
    for (const type of schemaTypes) {
        switch (type) {
            case 'object':
                typeMap[type] = buildConcatForObject(schema, from);
                break;
            case 'array':
                typeMap[type] = buildConcatForArray(schema, from);
                break;
            case 'boolean':
            case 'integer':
            case 'number':
            case 'string':
            case 'null':
                typeMap[type] = buildConcatForPrimitive(type, from);
                break;
            case 'any':
                throw new Error(`Unsupported JSON Schema type "${type}"`);
            default:
                throw new Error(`Unsupported JSON Schema type "${type}"`);
        }
    }
    const keys = Object.keys(typeMap);
    if (keys.length === 0) {
        return null;
    }
    if (keys.length === 1) {
        return typeMap[keys[0]];
    }
    const conditions = convertTypeMapToConditions(typeMap, from, from, false);
    return conditions.reduce((previous, current, index) => {
        const path = buildPath(index);
        set(previous, path, current);
        return previous;
    }, makeCondition(from, 'missing', null, from, false));
}

/**
 * @param {import("../types").JSONSchema6} schema
 * @param {string} from
 * @returns {*}
 */
function buildConcatForObject(schema, from) {
    const concatItems = [];
    concatItems.push('{');
    let counter = 0;
    const length = Object.keys(schema.properties).length;
    // eslint-disable-next-line guard-for-in
    for (const propName in schema.properties) {
        counter++;
        const isLast = counter === length;
        const includeComma = !isLast;
        const propValue = schema.properties[propName];
        if ($check.boolean(propValue)) {
            continue;
        }

        const fullPropName = `${from}.${propName}`;
        const typeMap = buildTypeMap(propValue, fullPropName, propName, from);

        const conditions = convertTypeMapToConditions(
            typeMap,
            fullPropName,
            propName,
            includeComma
        );
        if (!conditions) {
            continue;
        }
        const reduced = conditions.reduce((previous, current, index) => {
            if (!previous) {
                return current;
            }
            const path = buildPath(index);
            set(previous, path, current);
            return previous;
        });
        concatItems.push(reduced);
    }
    concatItems.push('}');
    return {
        $concat: [
            {
                $cond: [
                    {
                        $eq: [{$type: `$${from}`}, 'missing'],
                    },
                    null,
                    {$concat: concatItems},
                ],
            },
        ],
    };
}

/**
 * @param {JSONSchema6} propValue
 * @param {string} fullPropName
 * @param {string} propName
 * @param {string} from
 * @returns {*}
 */
function buildTypeMap(propValue, fullPropName, propName, from) {
    const propertyTypes = getTypes(propValue.type);
    const typeMap = {};
    for (const type of propertyTypes) {
        switch (type) {
            case 'object':
                {
                    const res = buildConcatForObject(propValue, fullPropName);
                    if (res) {
                        typeMap[type] = [`"${propName}":`, res];
                    }
                }
                break;
            case 'array':
                {
                    const res = buildConcatForArray(propValue, fullPropName);
                    if (res) {
                        typeMap[type] = [`"${propName}":`, res];
                    }
                }
                break;
            case 'boolean':
            case 'integer':
            case 'number':
            case 'string':
                typeMap[type] = buildConcatForPrimitive(type, propName, from);
                break;
            case 'null':
                break;
            case 'any':
                throw new Error(`Unsupported JSON Schema type "${type}"`);
            default:
                throw new Error(`Unsupported JSON Schema type "${type}"`);
        }
    }
    return typeMap;
}

/**
 *
 * @param {object} typeMap
 * @param {string} fullPropName
 * @param {string} propName
 * @param {boolean} includeComma
 * @returns {*[]}
 */
function convertTypeMapToConditions(
    typeMap,
    fullPropName,
    propName,
    includeComma
) {
    const keys = Object.keys(typeMap);
    if (keys.length === 0) {
        return null;
    }
    const conditions = [];
    for (const key of keys) {
        const subPipeline = typeMap[key];
        switch (key) {
            case 'object':
                conditions.push(
                    makeCondition(
                        fullPropName,
                        'object',
                        subPipeline,
                        propName,
                        includeComma
                    )
                );
                break;
            case 'array':
                conditions.push(
                    makeCondition(
                        fullPropName,
                        'array',
                        subPipeline,
                        propName,
                        includeComma
                    )
                );
                break;
            case 'boolean':
                conditions.push(
                    makeCondition(
                        fullPropName,
                        'bool',
                        subPipeline,
                        propName,
                        includeComma
                    )
                );
                break;
            case 'integer':
            case 'number':
                conditions.push(
                    makeCondition(
                        fullPropName,
                        ['double', 'int', 'long', 'decimal'],
                        subPipeline,
                        propName,
                        includeComma
                    )
                );
                break;
            case 'string':
                conditions.push(
                    makeCondition(
                        fullPropName,
                        'string',
                        subPipeline,
                        propName,
                        includeComma
                    )
                );
                break;
            case 'null':
                break;
            // case 'any':
            //     throw new Error(
            //         `Unsupported JSON Schema type "${key}"`
            //     );
            default:
                throw new Error(`Unsupported JSON Schema type "${key}"`);
        }
    }
    return conditions;
}

/**
 *
 * @param {number}index
 * @returns {string}
 */
function buildPath(index) {
    const str = [];
    for (let i = 0; i <= index; i++) {
        str.push('$cond[2]');
    }
    return str.join('.');
}

/**
 *
 * @param {string} fullPropName
 * @param {string|string[]} mongoType
 * @param {*}subPipeline
 * @param {string} propName
 * @param {boolean}includeComma
 */
function makeCondition(
    fullPropName,
    mongoType,
    subPipeline,
    propName,
    includeComma = false
) {
    if (Array.isArray(subPipeline)) {
        subPipeline = {$concat: subPipeline};
    }
    if (includeComma && $check.object(subPipeline) && subPipeline.$concat) {
        subPipeline.$concat.push(',');
    }
    if (!Array.isArray(mongoType)) {
        return {
            $cond: [
                {
                    $eq: [{$type: `$${fullPropName}`}, mongoType],
                },
                subPipeline,
                `"${propName}":null${includeComma ? ',' : ''}`,
            ],
        };
    }
    return {
        $cond: [
            {
                $or: mongoType.map((type) => {
                    return {
                        $eq: [{$type: `$${fullPropName}`}, type],
                    };
                }),
            },
            subPipeline,
            `"${propName}":null${includeComma ? ',' : ''}`,
        ],
    };
}

/**
 *
 * @param {JSONSchema6TypeName} type
 * @param {string} propName
 * @param {string }[from]
 */
function buildConcatForPrimitive(type, propName, from) {
    if (!from) {
        return {
            $concat: [
                type !== 'string'
                    ? {$toString: `$${propName}`}
                    : `$${propName}`,
            ],
        };
    }
    const fullPath = `${from}.${propName}`;
    return {
        $concat: [
            `"${propName}":${type === 'string' ? '"' : ''}`,
            type !== 'string' ? {$toString: `$${fullPath}`} : `$${fullPath}`,
            `${type === 'string' ? '"' : ''}`,
        ],
    };
}

/**
 *
 * @param {JSONSchema6TypeName|JSONSchema6TypeName[]}type
 */
function getTypes(type) {
    if (!type) {
        type = ['any'];
    }
    /** @type {JSONSchema6TypeName[]}*/
    let typeArray = Array.isArray(type) ? type : [type];
    typeArray = typeArray.filter((t) => t !== 'null');
    return typeArray;
}

/**
 * @param {import("../types").JSONSchema6} schema
 * @param {string} from
 * @returns {*}
 */
function buildConcatForArray(schema, from) {
    if (!schema.items) {
        throw new Error(`Schema.items is not set`);
    }
    if ($check.boolean(schema.items)) {
        throw new Error(`Schema.items is a boolean, not yet supported`);
    }
    if (Array.isArray(schema.items)) {
        throw new Error(
            `Schema.items was an array, not yet supported: ${JSON.stringify(
                schema.items,
                null,
                4
            )}`
        );
    }
    const propertyTypes = getTypes(schema.items.type);
    const typeMap = {};
    if (schema.items.properties) {
        if (propertyTypes.indexOf('object') < 0) {
            propertyTypes.push('object');
        }
        const res = buildConcatForObject(schema.items, '$this');
        if (res) {
            typeMap['object'] = res;
        }
    }
    if (schema.items.items) {
        if (propertyTypes.indexOf('array') < 0) {
            propertyTypes.push('array');
        }
        const res = buildConcatForArray(schema.items, '$this');
        if (res) {
            typeMap['array'] = res;
        }
    }

    for (const type of propertyTypes) {
        switch (type) {
            case 'object':
                // handled above
                break;
            case 'array':
                // todo above
                break;
            case 'boolean':
            case 'integer':
            case 'number':
            case 'string':
                typeMap[type] = makePrimitiveArrayProjection(type);
                break;
            case 'null':
                break;
            // case 'any':
            //     throw new Error(`Unsupported JSON Schema type "${type}"`);
            default:
                throw new Error(`Unsupported JSON Schema type "${type}"`);
        }
    }
    const conditions = convertTypeMapToArrayConditions(typeMap, false);
    if (!conditions) {
        return null;
    }
    const reduced = conditions.reduce((previous, current, index) => {
        const path = buildPath(index);
        set(previous, path, current);
        return previous;
    }, makeArrayCondition('missing', '', false));
    return {
        $concat: [
            '[',
            {
                $reduce: {
                    input: `$${from}`,
                    initialValue: '',
                    in: {
                        $concat: [
                            '$$value',
                            {$cond: [{$eq: ['$$value', '']}, '', ',']},
                            reduced,
                        ],
                    },
                },
            },
            ']',
        ],
    };
}
/**
 *
 * @param {JSONSchema6TypeName} type
 * @returns {*[]}
 */
function makePrimitiveArrayProjection(type) {
    return [
        type === 'string' ? '"' : '',
        type === 'string' ? '$$this' : {$toString: '$$this'},
        type === 'string' ? '"' : '',
    ];
}

/**
 *
 * @param {object} typeMap
 * @param {boolean} includeComma
 * @returns {*[]}
 */
function convertTypeMapToArrayConditions(typeMap, includeComma) {
    const keys = Object.keys(typeMap);
    if (keys.length === 0) {
        return null;
    }
    const conditions = [];
    for (const key of keys) {
        const subPipeline = typeMap[key];
        switch (key) {
            case 'object':
                conditions.push(
                    makeArrayCondition('object', subPipeline, includeComma)
                );
                break;
            case 'array':
                conditions.push(
                    makeArrayCondition('array', subPipeline, includeComma)
                );
                break;
            case 'boolean':
                conditions.push(
                    makeArrayCondition('bool', subPipeline, includeComma)
                );
                break;
            case 'integer':
            case 'number':
                conditions.push(
                    makeArrayCondition(
                        ['double', 'int', 'long', 'decimal'],
                        subPipeline,
                        includeComma
                    )
                );
                break;
            case 'string':
                conditions.push(
                    makeArrayCondition('string', subPipeline, includeComma)
                );
                break;
            case 'null':
                break;
            // case 'any':
            //     throw new Error(
            //         `Unsupported JSON Schema type "${key}"`
            //     );
            default:
                throw new Error(`Unsupported JSON Schema type "${key}"`);
        }
    }
    return conditions;
}

/**
 *
 * @param {string|string[]} mongoType
 * @param {*}subPipeline
 * @param {boolean}includeComma
 */
function makeArrayCondition(mongoType, subPipeline, includeComma = false) {
    if (Array.isArray(subPipeline)) {
        subPipeline = {$concat: subPipeline};
    }
    if (includeComma && $check.object(subPipeline) && subPipeline.$concat) {
        subPipeline.$concat.push(',');
    }
    if (!Array.isArray(mongoType)) {
        return {
            $cond: [
                {
                    $eq: [{$type: `$$this`}, mongoType],
                },
                subPipeline,
                // `null${includeComma ? ',' : ''}`,
                '',
            ],
        };
    }
    return {
        $cond: [
            {
                $or: mongoType.map((type) => {
                    return {
                        $eq: [{$type: `$$this`}, type],
                    };
                }),
            },
            subPipeline,
            // `null${includeComma ? ',' : ''}`,
            '',
        ],
    };
}
