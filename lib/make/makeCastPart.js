const $check = require('check-types');
const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');
const set = require('lodash/set');

exports.makeCastPart = makeCastPart;

/** @typedef {import("json-schema").JSONSchema6TypeName} JSONSchema6TypeName */

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
        const foundSchema = findSchema(context.schemas, fromCol);
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
 * @returns {import("../types").JSONSchema6}
 */
function findSchema(schemas, column) {
    // eslint-disable-next-line guard-for-in
    for (const collectionName in schemas) {
        const schema = schemas[collectionName];
        const result = findSubSchema(schema, column);
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
                typeMap[type] = buildProjectionForArray(schema, from);
                break;
            case 'boolean':
            case 'integer':
            case 'number':
            case 'string':
            case 'null':
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
    } else {
        throw new Error('todo');
    }
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

        const propertyTypes = getTypes(propValue.type);
        const typeMap = {};
        const fullPropName = `${from}.${propName}`;
        for (const type of propertyTypes) {
            switch (type) {
                case 'object':
                    {
                        const res = buildConcatForObject(
                            propValue,
                            fullPropName
                        );
                        if (res) {
                            typeMap[type] = [`"${propName}":`, res];
                        }
                    }
                    break;
                case 'array':
                    {
                        const res = buildStringifyProjection(
                            propValue,
                            fullPropName
                        );
                        if (res) {
                            typeMap[type] = [`"${propName}":`, res];
                        }
                    }
                    break;
                case 'boolean':
                case 'integer':
                case 'number':
                case 'string':
                    typeMap[type] = makePrimitiveProjection(
                        type,
                        propName,
                        from
                    );
                    break;
                case 'null':
                    break;
                case 'any':
                    throw new Error(`Unsupported JSON Schema type "${type}"`);
                default:
                    throw new Error(`Unsupported JSON Schema type "${type}"`);
            }
        }
        const keys = Object.keys(typeMap);
        if (keys.length === 0) {
            continue;
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
        const reduced = conditions.reduce((previous, current, index) => {
            const path = buildPath(index);
            set(previous, path, current);
            return previous;
        }, makeCondition(fullPropName, 'missing', '', propName, includeComma));
        concatItems.push(reduced);
    }
    concatItems.push('}');
    return {
        $concat: concatItems,
    };
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
 * @param {string }from
 */
function makePrimitiveProjection(type, propName, from) {
    return {
        $concat: [
            `"${propName}":${type === 'string' ? '"' : ''}`,
            type !== 'string'
                ? {$toString: `$${from}.${propName}`}
                : `$${from}.${propName}`,
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
function buildProjectionForArray(schema, from) {
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
    if (!schema.items.properties) {
        /** @type {import("json-schema").JSONSchema6TypeName} */
        let type;
        if (!schema.items.type) {
            type = 'string';
        } else if (Array.isArray(schema.items.type)) {
            const nonNull = schema.items.type.filter((t) => t !== 'null');
            if (nonNull.length === 1) {
                type = nonNull[0];
            }
            throw new Error(
                `Multiple types for arrays not yet supported ${JSON.stringify(
                    nonNull
                )}`
            );
        } else {
            type = schema.items.type;
        }

        // primitive array
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
                                type === 'string' ? '"' : '',
                                type === 'string'
                                    ? '$$this'
                                    : {$toString: '$$this'},
                                type === 'string' ? '"' : '',
                            ],
                        },
                    },
                },
                ']',
            ],
        };
    }
    const concat = buildConcatForObject(schema.items, '$this');
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
                            ...concat.$concat,
                        ],
                    },
                },
            },
            ']',
        ],
    };
}
