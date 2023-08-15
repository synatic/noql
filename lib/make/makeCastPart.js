const {nonEmptyArray} = require('check-types');
const _allowableFunctions = require('../MongoFunctions');
const makeProjectionExpressionPartModule = require('./makeProjectionExpressionPart');

exports.makeCastPart = makeCastPart;
exports.buildConcat = buildConcat;

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
        /** @type {import('../types').FlattenedSchemas} */
        const relevantSchemas = {};
        // eslint-disable-next-line guard-for-in
        for (const collectionName in context.schemas) {
            const schemas = context.schemas[collectionName];
            for (const schema of schemas) {
                if (
                    schema.path.startsWith(fromCol) &&
                    schema.path.indexOf('.n.') >= 0
                ) {
                    relevantSchemas[collectionName] =
                        relevantSchemas[collectionName] || [];
                    relevantSchemas[collectionName].push({
                        ...schema,
                        path: schema.path.replace(`${fromCol}.n.`, ''),
                    });
                }
            }
        }
        if (Object.keys(relevantSchemas).length) {
            if (Object.keys(relevantSchemas).length > 1) {
                console.warn(
                    `Multiple tables and schemas with a column "${fromCol}", using the first one`
                );
            }
            // todo RK, what if you are casting a number column to a varchar, need to be able to check for that and not do a reduce
            return {
                $reduce: {
                    input: from,
                    initialValue: '',
                    in: {
                        $concat: [
                            '$$value',
                            {$cond: [{$eq: ['$$value', '']}, '', ',']},
                            ...buildConcat(Object.values(relevantSchemas)[0]),
                        ],
                    },
                },
            };
        }
    }
    return convertFunction.parse([from, to]);
}

/**
 *
 * @param {import('../types').FlattenedSchema[]} schemas
 * @returns {*[]}
 */
function buildConcat(schemas) {
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
