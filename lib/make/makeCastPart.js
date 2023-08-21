const $check = require('check-types');
const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeCastPart = makeCastPart;
exports.buildConcat = buildStringifyProjection;

/** @typedef {import('json-schema').JSONSchema6TypeName} JSONSchema6TypeName */

/**
 * Makes an mongo expression tree from the cast statement
 *
 * @param {import('../types').Expression} expr - the AST expression that is a cast
 * @param {import('../types').NoqlContext} context - The Noql context to use when generating the output
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
        const fromCol = from.replaceAll('$', '');
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
 * @param {import('../types').Schemas} schemas
 * @param {string} column
 * @returns {import('../types').FindSchemaResult}
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
 * @param {import('../types').JSONSchema6} schema
 * @param {string} column
 * @returns {import('../types').FindSchemaResult}
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
            return {
                schema: value,
                required: (schema.required || []).indexOf(key) >= 0,
            };
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
 *
 * @param {import('../types').FindSchemaResult} foundSchema
 * @param {string} from
 * @returns {*}
 */
function buildStringifyProjection({required, schema}, from) {
    const {type} = schema;
    if (!type) {
        return null;
    }

    const mode = getProjectionMode(type);

    if (mode === 'object') {
        return {
            $concat: buildConcat(schema, required, from),
        };
    }
    return {
        $reduce: {
            input: `$${from}`,
            initialValue: '',
            in: {
                $concat: [
                    '$$value',
                    {$cond: [{$eq: ['$$value', '']}, '', ',']},
                    ...buildConcat(schema, required, from),
                ],
            },
        },
    };
    /** @type {string|object[]} */
}

/**
 * @param {import('../types').JSONSchema6} schema
 * @param {boolean} isRequired
 * @param {string} from
 * @returns {*[]}
 */
function buildConcat(schema, isRequired, from) {
    const results = [];
    results.push('{');
    // eslint-disable-next-line guard-for-in
    for (const propName in schema.properties) {
        const propValue = schema.properties[propName];
        if ($check.boolean(propValue)) {
            continue;
        }
        /** @type {import('json-schema').JSONSchema6TypeName} */
        let type;
        let nullable = isRequired ? false : true;
        if (Array.isArray(propValue.type)) {
            const notNullTypes = propValue.type.filter((t) => t !== 'null');
            // const hasArrays = schema.type.some((t) => t === 'array');
            // const hasObjects = schema.type.some((t) => t === 'object');
            if (notNullTypes.length === 1) {
                type = notNullTypes[0];
                nullable = true;
            } else {
                throw new Error(
                    `Multiple types not yet supported ${JSON.stringify(
                        notNullTypes
                    )}`
                );
            }
        } else {
            type = propValue.type;
        }
        if (type === 'string') {
            if (nullable) {
                results.push({
                    $cond: [
                        {$eq: [{$ifNull: [`$${from}.${propName}`, '']}, '']},
                        '',
                        {
                            $concat: [
                                `"${propName}":"`,
                                `$${from}.${propName}`,
                                '"',
                            ],
                        },
                    ],
                });
            } else {
                results.push(`"${propName}":"`);
                results.push(`$${from}.${propName}`);
                results.push('"');
            }
        }
    }
    results.push('}');
    return results;
}

/**
 *
 * @param {JSONSchema6TypeName | JSONSchema6TypeName[]} type
 * @returns {'array'|'object'}
 */
function getProjectionMode(type) {
    if (Array.isArray(type)) {
        const containsObject = type.indexOf('object') >= 0;
        const containsArray = type.indexOf('array') >= 0;
        if (!containsObject && !containsArray) {
            throw new Error('not an object or array');
        }
        if (containsArray && containsObject) {
            throw new Error('Not sure how to build concat, is array or object');
        }
        if (containsArray) {
            return 'array';
        }
        return 'object';
    }
    if (type === 'object') {
        return 'object';
    }
    if (type === 'array') {
        return 'array';
    }
    throw new Error('not an object or array');
}
/**
 *
 * @param {import('../types').JSONSchema6} schema
 * @returns {*[]}
 */
function buildConcatOld(schema) {
    /** @type {string|object[]} */
    const results = ['{'];
    for (const schema of schemas) {
        /** @type {import('json-schema').JSONSchema6TypeName} */
        let type;
        let nullable = false;
        if (Array.isArray(schema.type)) {
            const notNullTypes = schema.type.filter((t) => t !== 'null');
            const hasArrays = schema.type.some((t) => t === 'array');
            const hasObjects = schema.type.some((t) => t === 'object');
            if (notNullTypes.length === 1) {
                type = notNullTypes[0];
                nullable = true;
            }
            // else {
            //     throw new Error(
            //         `Multiple types not yet supported ${JSON.stringify(
            //             notNullTypes
            //         )}`
            //     );
            // }
        } else {
            type = schema.type;
        }
        const fieldName = schema.path;
        if (!type) {
            throw new Error(`Type not set`);
        }
        // todo get the correct field name for nested objects
        switch (type) {
            case 'string':
                results.push(`"${fieldName}":"`);
                if (nullable) {
                    results.push({
                        $cond: [
                            {$eq: ['$$value', '']},
                            `$$this.${fieldName}`,
                            'null',
                        ],
                    });
                } else {
                    results.push(`"$$this.${fieldName}"`);
                }
                results.push('"');
                break;
            case 'integer':
            case 'number':
            case 'boolean':
                results.push(`"${fieldName}":"`);
                if (nullable) {
                    results.push({
                        $cond: [
                            {$eq: ['$$value', '']},
                            {$toString: `$$this.${fieldName}`},
                            'null',
                        ],
                    });
                } else {
                    results.push({
                        $toString: `{ $toString: "$$this.${fieldName}" }`,
                    });
                }
                results.push('"');
                break;
            case 'array':
                console.log(schema);
                break;
            default:
                throw new Error(`Type not yet catered for: "${type}"`);
        }
    }
    results.push('}');
    return results;
}

// buildConcatForObjects, buildConcatForObjectArrays
/**
 *
 * {
  valuesString: {
    $reduce: {
      input: "$jsonArrayValues",
      initialValue: "",
      in: {
        $concat: [
          "$$value",
          {
            $cond: [
              {
                $eq: ["$$value", ""],
              },
              "",
              ",",
            ],
          },
          "{",
          '"arrayInt": [',
          {
            $reduce: {
              input: "$$this.arrayInt",
              initialValue: "",
              in: {
                $concat: [
                  "$$value",
                  {
                    $cond: [
                      {
                        $eq: ["$$value", ""],
                      },
                      "",
                      ",",
                    ],
                  },
                  {
                    $toString: "$$this",
                  },
                ],
              },
            },
          },
          "]",
          "}"
        ],
      },
    },
  },
}
 */
