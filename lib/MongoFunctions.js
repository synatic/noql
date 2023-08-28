const $check = require('check-types');
const $convert = require('@synatic/type-magic');
const {ObjectId} = require('bson');

// https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#arithmetic-expression-operators

/**
 * Manages mapping functions from sql to mongo
 *
 */
class AllowableFunctions {
    // eslint-disable-next-line jsdoc/require-returns
    /**
     * The type mapping from sql to mongo for cast
     */
    static get _sqlTypeMapping() {
        return {
            double: 'double',
            string: 'string',
            bool: 'bool',
            date: 'date',
            int: 'int',
            objectId: 'objectId',
            long: 'long',
            decimal: 'decimal',
            varchar: 'string',
            datetime: 'date',
            time: 'date',
            float: 'number',
            char: 'string',
            nchar: 'string',
            text: 'string',
        };
    }

    // eslint-disable-next-line jsdoc/require-returns
    /**
     * The type mapping from sql to mongo for cast
     *
     * @returns {import("./types").JsonSchemaTypeMap}
     */
    static get _jsonSchemaTypeMapping() {
        return {
            double: 'number',
            string: 'string',
            bool: 'boolean',
            date: 'date',
            int: 'number',
            integer: 'number',
            objectId: 'string',
            long: 'number',
            decimal: 'number',
            varchar: 'string',
            datetime: 'date',
            time: 'date',
            float: 'number',
            char: 'string',
            nchar: 'string',
            text: 'string',
            object: 'object',
            number: 'number',
        };
    }

    /**
     * Gets an allowed function by name, returns null if not found
     *
     * @param name
     * @readonly
     * @static
     * @memberof AllowableFunctions
     * @returns {import("./types").MongoQueryFunction|null}
     */
    static functionByName(name) {
        if (!name) {
            return null;
        }
        const lowerName = name.toLowerCase();
        return AllowableFunctions.functionMappings.find((fn) =>
            filterFunctionsByName(fn, lowerName)
        );
    }

    static functionByNameAndType(name, type) {
        if (!name) {
            return null;
        }
        if (!type) {
            return AllowableFunctions.functionByName(name);
        }
        const lowerName = name.toLowerCase();
        const lowerType = type.toLowerCase();
        return AllowableFunctions.functionMappings.find(
            (fn) =>
                filterFunctionsByName(fn, lowerName) &&
                filterFunctionsByType(fn, lowerType)
        );
    }

    static functionByNameAndTypeThatAllowsQuery(name, type) {
        if (!name) {
            return null;
        }
        if (!type) {
            return AllowableFunctions.functionByName(name);
        }
        const lowerName = name.toLowerCase();
        const lowerType = type.toLowerCase();
        return AllowableFunctions.functionMappings.find(
            (fn) =>
                filterFunctionsByName(fn, lowerName) &&
                filterFunctionsByType(fn, lowerType) &&
                fn.allowQuery
        );
    }

    /**
     * Gets the list of function mappings between sql and mongo
     *
     * @returns {import("./types").MongoQueryFunction[]}
     */
    static get functionMappings() {
        return [
            /* #region Columns */
            {
                name: 'field_exists',
                allowQuery: true,
                parse: (parameters) => {
                    return {[parameters[0]]: {$exists: parameters[1]}};
                },
                jsonSchemaReturnType: 'boolean',
            },
            /* #endregion */

            /* #region Object Operators */
            {
                name: 'parse_json',
                allowQuery: true,
                parse: (parameters) => {
                    const jsonToParse =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (jsonToParse.$literal) {
                        return {$literal: JSON.parse(jsonToParse.$literal)};
                    } else {
                        return JSON.parse(jsonToParse);
                    }
                },
                jsonSchemaReturnType: 'object',
            },
            {
                name: 'merge_objects',
                allowQuery: true,
                parse: (parameters) => {
                    return {$mergeObjects: parameters};
                },
                jsonSchemaReturnType: 'object',
            },
            {
                name: 'empty_object',
                allowQuery: true,
                parse: (parameters) => {
                    return {$literal: {}};
                },
                jsonSchemaReturnType: 'object',
            },

            /* #endregion */

            /* #region Arithmetic Operators */
            {
                name: 'avg',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$avg: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'abs',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $abs: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'acos',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $acos: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'acosh',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $acosh: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'asin',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $asin: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'asinh',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $asinh: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'atan',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $atan: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'atan2',
                allowQuery: true,
                parse: (parameters) => {
                    return {$atan2: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'atanh',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $atanh: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'binary_size',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $binarySize:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'ceil',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $ceil: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'degrees_to_radians',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $degreesToRadians:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'divide',
                allowQuery: true,
                parse: (parameters) => {
                    return {$divide: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'exp',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $exp: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'floor',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $floor: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'ln',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $ln: AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'log',
                allowQuery: true,
                parse: (parameters) => {
                    return {$log: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'log10',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $log10: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'max',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$max: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'min',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$min: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'mod',
                allowQuery: true,
                parse: (parameters) => {
                    return {$mod: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'multiply',
                allowQuery: true,
                parse: (parameters) => {
                    return {$multiply: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'pow',
                allowQuery: true,
                parse: (parameters) => {
                    return {$pow: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'radians_to_degrees',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $radiansToDegrees:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'rand',
                allowQuery: true,
                parse: (parameters) => {
                    return {$rand: {}};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'round',
                allowQuery: true,
                parse: (parameters) => {
                    return {$round: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'sin',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $sin: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'sinh',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $sinh: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'sqrt',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $sqrt: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'subtract',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$subtract: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'sum',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$add: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'tan',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $tan: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'tanh',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $tanh: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'trunc',
                allowQuery: true,
                parse: (parameters) => {
                    return {$trunc: parameters};
                },
                jsonSchemaReturnType: 'number',
            },

            /* #endregion */

            /* #region Aggregate Functions */
            {
                name: 'sum',
                type: 'aggr_func',
                allowQuery: false,
                forceGroup: true,
                parse: (parameters) => {
                    return {
                        $sum: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'avg',
                allowQuery: false,
                type: 'aggr_func',
                forceGroup: true,
                parse: (parameters) => {
                    return {
                        $avg: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'min',
                allowQuery: false,
                type: 'aggr_func',
                forceGroup: true,
                parse: (parameters) => {
                    return {
                        $min: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'max',
                type: 'aggr_func',
                allowQuery: false,
                forceGroup: true,
                parse: (parameters) => {
                    return {
                        $max: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                // todo check count
                name: 'count',
                allowQuery: false,
                forceGroup: true,
                type: 'function',
                parse: (parameter) => {
                    return {$sum: 1};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                // todo check count
                name: 'count',
                allowQuery: false,
                forceGroup: true,
                type: 'aggr_func',
                parse: (parameter) => {
                    return {$sum: 1};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'firstn',
                allowQuery: false,
                forceGroup: true,
                type: 'aggr_func',
                parse: (parameters) => {
                    if (!$check.array(parameters)) {
                        throw new Error('Invalid parameters for substring');
                    }
                    if (parameters.length < 1) {
                        throw new Error('Invalid parameters for FirstN');
                    }
                    return {
                        $firstN: {
                            input: $check.assigned(parameters[1])
                                ? '$' +
                                  AllowableFunctions._getLiteral(parameters[1])
                                : '$$ROOT',
                            n: parameters[0],
                        },
                    };
                },
                jsonSchemaReturnType: (parameters) => {
                    if (!$check.array(parameters)) {
                        throw new Error('Invalid parameters for substring');
                    }
                    if (parameters.length < 1) {
                        throw new Error('Invalid parameters for FirstN');
                    }
                    if ($check.assigned(parameters[1])) {
                        return {
                            type: 'fieldName',
                            fieldName: parameters[1].value,
                        };
                    }
                    return {
                        type: 'jsonSchemaValue',
                        jsonSchemaValue: 'object',
                        isArray: true,
                    };
                },
            },
            {
                name: 'lastn',
                allowQuery: false,
                forceGroup: true,
                type: 'aggr_func',
                parse: (parameters) => {
                    if (!$check.array(parameters)) {
                        throw new Error('Invalid parameters for substring');
                    }
                    if (parameters.length < 1) {
                        throw new Error('Invalid parameters for LastN');
                    }
                    return {
                        $lastN: {
                            input: $check.assigned(parameters[1])
                                ? '$' +
                                  AllowableFunctions._getLiteral(parameters[1])
                                : '$$ROOT',
                            n: parameters[0],
                        },
                    };
                },
                jsonSchemaReturnType: (parameters) => {
                    if (!$check.array(parameters)) {
                        throw new Error('Invalid parameters for substring');
                    }
                    if (parameters.length < 1) {
                        throw new Error('Invalid parameters for FirstN');
                    }
                    if ($check.assigned(parameters[1])) {
                        return {
                            type: 'fieldName',
                            fieldName: parameters[1].value,
                        };
                    }
                    return {
                        type: 'jsonSchemaValue',
                        jsonSchemaValue: 'object',
                        isArray: true,
                    };
                },
            },
            // ToDo:add aggregate function with
            // $addToSet

            /* #endregion */

            /* #region String Functions */

            /*
            $strcasecmp
             */
            {
                name: 'wrapParam',
                allowQuery: true,
                parse: (parameters) => {
                    const value = parameters[0];
                    const forceString = parameters[1]
                        ? parameters[1].$literal
                        : false;
                    let unescaped;
                    if (typeof value === 'object' && value.$literal) {
                        unescaped = value.$literal;
                    } else {
                        unescaped = value.substring(1);
                    }
                    unescaped = unescaped
                        .replace(/\\"/g, '"')
                        .replace(/\\'/g, "'")
                        .replace(/\\\\/g, '\\');
                    if (forceString) {
                        return unescaped;
                    }
                    return {
                        $literal: unescaped,
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'concat',
                parsedName: '$concat',
                allowQuery: true,
                parse: (parameters) => {
                    return {$concat: parameters};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'join',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for join');
                    if (parameters.length !== 2) {
                        throw new Error(
                            `Invalid parameter length for join, should be two but was ${parameters.length}`
                        );
                    }
                    const input = parameters[0];
                    if ($check.emptyString(input) || !$check.string(input)) {
                        throw new Error(
                            `The first parameter passed to join should be a non empty string but was ${input}`
                        );
                    }
                    const joinCharacter = parameters[1].$literal;
                    if (
                        $check.emptyString(joinCharacter) ||
                        !$check.string(joinCharacter)
                    ) {
                        throw new Error(
                            `The second parameter passed to join should be a non empty string but was ${joinCharacter}`
                        );
                    }
                    return {
                        $reduce: {
                            input,
                            initialValue: '',
                            in: {
                                $concat: [
                                    '$$value',
                                    {
                                        $cond: [
                                            {$eq: ['$$value', '']},
                                            '',
                                            joinCharacter,
                                        ],
                                    },
                                    '$$this',
                                ],
                            },
                        },
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'trim',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        input: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if (parameters[1]) {
                        params.chars = parameters[1];
                    }
                    return {$trim: params};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'ltrim',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        input: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if (parameters[1]) {
                        params.chars = parameters[1];
                    }
                    return {$ltrim: params};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'rtrim',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        input: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if (parameters[1]) {
                        params.chars = parameters[1];
                    }
                    return {$rtrim: params};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'substr',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 3) {
                        throw new Error(
                            'Invalid parameters required for substring'
                        );
                    }

                    return {$substr: parameters};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'starts_with',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 2) {
                        throw new Error('Invalid parameters starts_with');
                    }

                    return {
                        $regexMatch: {
                            input: parameters[0],
                            regex: {$concat: [parameters[1], {$literal: '$'}]},
                        },
                    };
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'strpos',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 2) {
                        throw new Error('Invalid parameters starts_with');
                    }

                    return {
                        $add: [{$indexOfCP: parameters}, 1],
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'locate',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 2) {
                        throw new Error('Invalid parameters starts_with');
                    }

                    return {
                        $add: [{$indexOfCP: [parameters[1], parameters[0]]}, 1],
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'substr_bytes',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 3) {
                        throw new Error(
                            'Invalid parameters required for substring'
                        );
                    }

                    return {$substrBytes: parameters};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'substr_cp',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 3) {
                        throw new Error(
                            'Invalid parameters required for substring'
                        );
                    }

                    return {$substrCP: parameters};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'to_upper',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $toUpper:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'upper',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $toUpper:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'to_lower',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $toLower:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'lower',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $toLower:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'replace',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 3) {
                        throw new Error(
                            'Invalid parameters required for substring'
                        );
                    }
                    return {
                        $replaceOne: {
                            input: parameters[0],
                            find: parameters[1],
                            replacement: parameters[2],
                        },
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'replace_all',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for substring');
                    if (parameters.length !== 3) {
                        throw new Error(
                            'Invalid parameters required for substring'
                        );
                    }
                    return {
                        $replaceAll: {
                            input: parameters[0],
                            find: parameters[1],
                            replacement: parameters[2],
                        },
                    };
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'strlen',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $strLenBytes:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'strlen_cp',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $strLenCP:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'split',
                allowQuery: true,
                parse: (parameters) => {
                    const params = [
                        AllowableFunctions._getSingleParameter(parameters),
                    ];
                    if (
                        $check.array(parameters) &&
                        parameters[1] &&
                        parameters[1].$literal !== null
                    ) {
                        params.push(parameters[1]);
                    }
                    return {$split: params};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'fieldName',
                        fieldName: parameters[0].column,
                        isArray: true,
                    };
                },
            },
            /* #endregion */

            /* #region Conversion Functions */
            {
                name: 'convert',
                allowQuery: true,
                type: 'function',
                parse: (parameters, depth, forceLiteralParse) => {
                    const toSQLType = parameters[1]
                        ? parameters[1].$literal || parameters[1]
                        : null;
                    if (!$check.string(toSQLType)) {
                        throw new Error('Type not specified for convert');
                    }
                    const toType =
                        AllowableFunctions._sqlTypeMapping[
                            toSQLType.toLowerCase()
                        ] || toSQLType;
                    if (!toType) {
                        throw new Error(`Invalid type for convert:${toType}`);
                    }
                    return {$convert: {input: parameters[0], to: toType}};
                },
                jsonSchemaReturnType: (parameters) => {
                    const toSQLType = parameters[1]
                        ? parameters[1].$literal || parameters[1].value
                        : null;
                    if (!$check.string(toSQLType)) {
                        throw new Error('Type not specified for convert');
                    }
                    const toType =
                        AllowableFunctions._jsonSchemaTypeMapping[
                            toSQLType.toLowerCase()
                        ] || toSQLType;
                    if (!toType) {
                        throw new Error(`Invalid type for convert:${toType}`);
                    }
                    return {
                        type: 'jsonSchemaValue',
                        jsonSchemaValue: toType,
                        isArray: false,
                    };
                },
            },
            {
                name: 'to_date',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'date'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to date`
                            );
                        }
                    } else {
                        return {
                            $toDate:
                                AllowableFunctions._getSingleParameter(
                                    parameters
                                ),
                        };
                    }
                },
                jsonSchemaReturnType: 'date',
            },
            {
                name: 'to_string',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'string'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to string`
                            );
                        }
                    } else {
                        return {$toString: paramVal};
                    }
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'to_decimal',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'number'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to number`
                            );
                        }
                    } else {
                        return {$toDecimal: paramVal};
                    }
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'to_double',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'number'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to number`
                            );
                        }
                    } else {
                        return {$toDouble: paramVal};
                    }
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'to_int',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'integer'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to integer`
                            );
                        }
                    } else {
                        return {$toInt: paramVal};
                    }
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'to_long',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'integer'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to integer`
                            );
                        }
                    } else {
                        return {$toLong: paramVal};
                    }
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'to_bool',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            return $convert.convert(
                                AllowableFunctions._getLiteral(paramVal),
                                'boolean'
                            );
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to boolean`
                            );
                        }
                    } else {
                        return {$toBool: paramVal};
                    }
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'to_objectid',
                allowQuery: true,
                parse: (parameters, depth, forceLiteralParse) => {
                    const paramVal =
                        AllowableFunctions._getSingleParameter(parameters);
                    if (
                        forceLiteralParse &&
                        AllowableFunctions._isLiteral(paramVal)
                    ) {
                        try {
                            const param =
                                AllowableFunctions._getLiteral(paramVal);
                            return new ObjectId(param);
                        } catch (exp) {
                            throw new Error(
                                `Error converting ${AllowableFunctions._getLiteral(
                                    paramVal
                                )} to ObjectId`
                            );
                        }
                    } else {
                        return {$toObjectId: paramVal};
                    }
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'typeof',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $type: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },

                jsonSchemaReturnType: 'string',
            },
            {
                name: 'ifnull',
                aliases: ['coalesce'],
                allowQuery: true,
                parse: (parameters) => {
                    return {$ifNull: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    if (parameters[0].type === 'column_ref') {
                        return {
                            type: 'fieldName',
                            fieldName: parameters[0].column,
                        };
                    }
                    if (parameters[0].type === 'null') {
                        if (
                            [
                                'single_quote_string',
                                'string',
                                'backticks_quote_string',
                            ].includes(parameters[1].type)
                        ) {
                            return {
                                type: 'jsonSchemaValue',
                                jsonSchemaValue: 'string',
                            };
                        }
                        if (parameters[1].type) {
                            return {
                                type: 'jsonSchemaValue',
                                jsonSchemaValue: parameters[1].type,
                            };
                        }
                        return {
                            type: 'jsonSchemaValue',
                            jsonSchemaValue: 'object',
                        };
                    }
                    throw new Error('not implemented');
                },
            },

            /* #endregion */

            /* #region Date Functions */
            {
                name: 'date_from_string',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        dateString:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                    if (
                        $check.array(parameters) &&
                        parameters[1] &&
                        parameters[1].$literal !== null
                    ) {
                        params.format = parameters[1];
                    }
                    if (
                        $check.array(parameters) &&
                        parameters[2] &&
                        parameters[2].$literal !== null
                    ) {
                        params.timezone = parameters[2];
                    }
                    if (
                        $check.array(parameters) &&
                        parameters[3] &&
                        parameters[3].$literal !== null
                    ) {
                        params.onError = parameters[3];
                    }
                    if (
                        $check.array(parameters) &&
                        parameters[4] &&
                        parameters[4].$literal !== null
                    ) {
                        params.onNull = parameters[4];
                    }
                    return {$dateFromString: params};
                },
                jsonSchemaReturnType: 'date',
            },
            {
                name: 'date_from_parts',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        year: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[1]) &&
                        parameters[1].$literal !== null
                    ) {
                        params.month = parameters[1];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[2]) &&
                        parameters[2].$literal !== null
                    ) {
                        params.day = parameters[2];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[3]) &&
                        parameters[3].$literal !== null
                    ) {
                        params.hour = parameters[3];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[4]) &&
                        parameters[4].$literal !== null
                    ) {
                        params.minute = parameters[4];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[5]) &&
                        parameters[5].$literal !== null
                    ) {
                        params.second = parameters[5];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[6]) &&
                        parameters[6].$literal !== null
                    ) {
                        params.millisecond = parameters[6];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[7]) &&
                        parameters[7].$literal !== null
                    ) {
                        params.timezone = parameters[7];
                    }
                    return {$dateFromParts: params};
                },
                jsonSchemaReturnType: 'date',
            },
            {
                name: 'date_from_iso_parts',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        isoWeekYear:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[1]) &&
                        parameters[1].$literal !== null
                    ) {
                        params.isoWeek = parameters[1];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[2]) &&
                        parameters[2].$literal !== null
                    ) {
                        params.isoDayOfWeek = parameters[2];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[3]) &&
                        parameters[3].$literal !== null
                    ) {
                        params.hour = parameters[3];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[4]) &&
                        parameters[4].$literal !== null
                    ) {
                        params.minute = parameters[4];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[5]) &&
                        parameters[5].$literal !== null
                    ) {
                        params.second = parameters[5];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[6]) &&
                        parameters[6].$literal !== null
                    ) {
                        params.millisecond = parameters[6];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[7]) &&
                        parameters[7].$literal !== null
                    ) {
                        params.timezone = parameters[7];
                    }
                    return {$dateFromParts: params};
                },
                jsonSchemaReturnType: 'date',
            },
            {
                name: 'date_to_string',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if (
                        $check.array(parameters) &&
                        parameters[1] &&
                        parameters[1].$literal !== null
                    ) {
                        params.format = parameters[1];
                    }
                    if (
                        $check.array(parameters) &&
                        parameters[2] &&
                        parameters[2].$literal !== null
                    ) {
                        params.timezone = parameters[2];
                    }
                    if (
                        $check.array(parameters) &&
                        parameters[3] &&
                        parameters[3].$literal !== null
                    ) {
                        params.onNull = parameters[3];
                    }
                    return {$dateToString: params};
                },
                jsonSchemaReturnType: 'string',
            },
            {
                name: 'date_to_parts',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if (
                        $check.array(parameters) &&
                        parameters[1] &&
                        parameters[1].$literal !== null
                    ) {
                        params.timezone = parameters[1];
                    }
                    if (
                        $check.array(parameters) &&
                        $check.assigned(parameters[2]) &&
                        parameters[2].$literal !== null
                    ) {
                        params.iso8601 = parameters[2];
                    }
                    return {$dateToParts: params};
                },
                jsonSchemaReturnType: 'object',
            },
            {
                name: 'day_of_month',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$dayOfMonth: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'day_of_week',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$dayOfWeek: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'day_of_year',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$dayOfYear: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'iso_day_of_week',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$isoDayOfWeek: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'iso_week',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$isoWeek: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'iso_week_year',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$isoWeekYear: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'hour',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$hour: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'millisecond',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$millisecond: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'minute',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$minute: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'month',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$month: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'second',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$second: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'week',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$week: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'year',
                allowQuery: true,
                parse: (parameters) => {
                    const params = {
                        date: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                    if ($check.array(parameters) && parameters[1]) {
                        params.timezone = parameters[1];
                    }
                    return {$year: params};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'date_trunc',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for date_trunc');
                    if (parameters.length < 2) {
                        throw new Error(
                            'Invalid parameters required for date_trunc'
                        );
                    }
                    return {
                        $dateTrunc: {date: parameters[0], unit: parameters[1]},
                    };
                },
                jsonSchemaReturnType: 'date',
            },
            /* #endregion */

            /* #region Arithmetic Expression Operators */

            {
                name: '+',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$add: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: '/',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$divide: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: '*',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$multiply: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'number',
            },

            {
                name: '-',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$subtract: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: '%',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$mod: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'number',
            },

            /* #endregion */

            /* #region Array Expression Operators */

            {
                name: 'is_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $isArray:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'all_elements_true',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$allElementsTrue: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'any_element_true',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$anyElementTrue: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'size_of_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $size: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'first_in_array',
                parsedName: '$first',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $first: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'fieldName',
                        fieldName: parameters[0].column,
                        isArray: false,
                    };
                },
            },
            {
                name: 'last_in_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $last: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'fieldName',
                        fieldName: parameters[0].column,
                        isArray: false,
                    };
                },
            },
            {
                name: 'reverse_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $reverseArray:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'fieldName',
                        fieldName: parameters[0].column,
                        isArray: true,
                    };
                },
            },
            {
                name: 'array_elem_at',
                allowQuery: true,
                parse: (parameters) => {
                    return {$arrayElemAt: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'fieldName',
                        fieldName: parameters[0].column,
                        isArray: false,
                    };
                },
            },
            {
                name: 'indexof_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {$indexOfArray: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'array_range',
                allowQuery: true,
                parse: (parameters) => {
                    return {$range: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'jsonSchemaValue',
                        isArray: true,
                        jsonSchemaValue: 'number',
                    };
                },
            },
            {
                name: 'zip_array',
                allowQuery: true,
                parse: (parameters) => {
                    if (!$check.array(parameters))
                        throw new Error('Invalid parameters for zip_array');

                    return {$zip: {inputs: parameters}};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'jsonSchemaValue',
                        jsonSchemaValue: 'object',
                        isArray: true,
                    };
                },
            },
            {
                name: 'concat_arrays',
                allowQuery: true,
                parse: (parameters) => {
                    return {$concatArrays: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'jsonSchemaValue',
                        jsonSchemaValue: 'object',
                        isArray: true,
                    };
                },
            },
            {
                name: 'object_to_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $objectToArray:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: () => {
                    return {
                        type: 'jsonSchemaValue',
                        isArray: true,
                        jsonSchemaValue: 'object',
                    };
                },
            },
            {
                name: 'array_to_object',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $arrayToObject:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
                jsonSchemaReturnType: 'object',
            },
            {
                name: 'set_union',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setUnion: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'jsonSchemaValue',
                        isArray: true,
                        jsonSchemaValue: 'object',
                    };
                },
            },
            {
                name: 'set_difference',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setDifference: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'jsonSchemaValue',
                        isArray: true,
                        jsonSchemaValue: 'object',
                    };
                },
            },
            {
                name: 'set_intersection',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setIntersection: parameters};
                },
                jsonSchemaReturnType: (parameters) => {
                    return {
                        type: 'jsonSchemaValue',
                        isArray: true,
                        jsonSchemaValue: 'object',
                    };
                },
            },
            {
                name: 'set_equals',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setEquals: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'set_is_subset',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setIsSubset: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'sum_array',
                description: 'Sums the elements in an array',
                allowQuery: true,
                parse: (parameters, depth = 0) => {
                    // if(parameters.length<2)throw new Error("Invalid parameters, requires at least the array field and ids");
                    if (
                        !(
                            (parameters[0].startsWith &&
                                parameters[0].startsWith('$')) ||
                            parameters[0].$map
                        )
                    )
                        throw new Error(
                            'Invalid parameters, first parameter must be a column reference'
                        );

                    let reduceInput = parameters[0];
                    if (depth > 0 && $check.string(reduceInput)) {
                        reduceInput = `$$this.${
                            reduceInput.startsWith('$')
                                ? reduceInput.substring(1)
                                : reduceInput
                        }`;
                    }

                    const reduce = {
                        $reduce: {
                            input: reduceInput,
                            initialValue: 0,
                            in: {
                                $sum: ['$$value'],
                            },
                        },
                    };
                    if (parameters.length === 1) {
                        reduce.$reduce.in.$sum.push('$$this');
                    }
                    let curReduce = reduce;
                    for (let i = 1; i < parameters.length; i++) {
                        const fieldName =
                            parameters[i] && parameters[i].$literal
                                ? parameters[i].$literal
                                : parameters[i].startsWith &&
                                  parameters[i].startsWith('$')
                                ? parameters[i].substring(1)
                                : '';
                        if (!fieldName)
                            throw new Error(
                                'Invalid parameter for field names'
                            );

                        if (i === parameters.length - 1) {
                            curReduce.$reduce.in.$sum.push(
                                `$$this.${fieldName}`
                            );
                        } else {
                            const reduce = {
                                $reduce: {
                                    input: `$$this.${fieldName}`,
                                    initialValue: 0,
                                    in: {$sum: ['$$value']},
                                },
                            };
                            curReduce.$reduce.in.$sum.push(reduce);
                            curReduce = reduce;
                        }
                    }
                    return reduce;
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'avg_array',
                description: 'Averages the elements in an array',
                allowQuery: true,
                parse: (parameters, depth = 0) => {
                    // if(parameters.length<2)throw new Error("Invalid parameters, requires at least the array field and ids");
                    if (
                        !(
                            (parameters[0].startsWith &&
                                parameters[0].startsWith('$')) ||
                            parameters[0].$map
                        )
                    )
                        throw new Error(
                            'Invalid parameters, first parameter must be a column reference'
                        );

                    let reduceInput = parameters[0];
                    if (depth > 0 && $check.string(reduceInput)) {
                        reduceInput = `$$this.${
                            reduceInput.startsWith('$')
                                ? reduceInput.substring(1)
                                : reduceInput
                        }`;
                    }

                    const reduce = {
                        $reduce: {
                            input: reduceInput,
                            initialValue: 0,
                            in: {
                                $avg: ['$$value'],
                            },
                        },
                    };
                    if (parameters.length === 1) {
                        reduce.$reduce.in.$avg.push('$$this');
                    }

                    let curReduce = reduce;
                    for (let i = 1; i < parameters.length; i++) {
                        const fieldName =
                            parameters[i] && parameters[i].$literal
                                ? parameters[i].$literal
                                : parameters[i].startsWith &&
                                  parameters[i].startsWith('$')
                                ? parameters[i].substring(1)
                                : '';
                        if (!fieldName)
                            throw new Error(
                                'Invalid parameter for field names'
                            );

                        if (i === parameters.length - 1) {
                            curReduce.$reduce.in.$avg.push(
                                `$$this.${fieldName}`
                            );
                        } else {
                            const reduce = {
                                $reduce: {
                                    input: `$$this.${fieldName}`,
                                    initialValue: 0,
                                    in: {$avg: ['$$value']},
                                },
                            };
                            curReduce.$reduce.in.$avg.push(reduce);
                            curReduce = reduce;
                        }
                    }
                    return reduce;
                },
                jsonSchemaReturnType: 'number',
            },

            // todo implement
            // {
            //     name: 'max_array',
            //     allowQuery: true,
            //     parse: (parameters) => {
            //         return {
            //             $last: AllowableFunctions._getSingleParameter(
            //                 parameters
            //             ),
            //         };
            //     },
            // },
            // {
            //     name: 'min_array',
            //     allowQuery: true,
            //     parse: (parameters) => {
            //         return {
            //             $first: AllowableFunctions._getSingleParameter(
            //                 parameters
            //             ),
            //         };
            //     },
            // },

            /* #endregion */

            /* #region Boolean Expression Operators */
            {
                name: 'and',
                parsedName: '$and',
                parse: (item) => {
                    return {
                        $and: [
                            AllowableFunctions.checkElementBasicType(item[0]),
                            AllowableFunctions.checkElementBasicType(item[1]),
                        ],
                    };
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'not',
                parsedName: '$not',
                parse: (item) => {
                    return {
                        $not: [
                            AllowableFunctions.checkElementBasicType(item[0]),
                            AllowableFunctions.checkElementBasicType(item[1]),
                        ],
                    };
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'or',
                parsedName: '$or',
                parse: (item) => {
                    return {
                        $or: [
                            AllowableFunctions.checkElementBasicType(item[0]),
                            AllowableFunctions.checkElementBasicType(item[1]),
                        ],
                    };
                },
                jsonSchemaReturnType: 'boolean',
            },

            /* #endregion */

            /* #region Comparison Expression Operators */
            {
                name: 'cmp',
                allowQuery: true,
                parse: (parameters) => {
                    return {$cmp: parameters};
                },
                jsonSchemaReturnType: 'number',
            },
            {
                name: 'eq',
                allowQuery: true,
                parse: (parameters) => {
                    return {$eq: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: '=',
                parse: (numerator, denominator) => {
                    return {$eq: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'gt',
                allowQuery: true,
                parse: (parameters) => {
                    return {$gt: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: '>',
                parse: (numerator, denominator) => {
                    return {$gt: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'gte',
                allowQuery: true,
                parse: (parameters) => {
                    return {$gte: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: '>=',
                parse: (numerator, denominator) => {
                    return {$gte: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'lt',
                allowQuery: true,
                parse: (parameters) => {
                    return {$lt: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: '<',
                parse: (numerator, denominator) => {
                    return {$lt: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'lte',
                allowQuery: true,
                parse: (parameters) => {
                    return {$lte: parameters};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: '<=',
                parse: (numerator, denominator) => {
                    return {$lte: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'ne',
                allowQuery: true,
                parse: (parameters) => {
                    return {$ne: parameters};
                },

                jsonSchemaReturnType: 'boolean',
            },
            {
                name: '!=',
                parse: (numerator, denominator) => {
                    return {$ne: [numerator, denominator]};
                },
                jsonSchemaReturnType: 'boolean',
            },
            {
                name: 'unset',
                allowQuery: true,
                parse: (fields) => {
                    return {$unset: fields.map((f) => f.replace('$', ''))};
                },
                requiresAs: false,
                jsonSchemaReturnType: (parameters) => {
                    return parameters.map((p) => {
                        return {
                            type: 'unset',
                            fieldName: p.column,
                        };
                    });
                },
            },
            /* #endregion */

            // separate action
            // {
            //     name: "unwind",
            //     parsedName: "$unwind",
            //     parse: (items) => {
            //         return { $unwind: items[0] }
            //     },
            // },

            {
                name: 'current_date',
                parse: () => {
                    return '$$NOW';
                },
                jsonSchemaReturnType: 'date',
                doesNotNeedArgs: true,
            },
        ];
    }

    /**
     * Returns a single parameters form the parameters array
     *
     * @param {any} parameters - The parameters to get a single from
     * @returns {*}
     */
    static _getSingleParameter(parameters) {
        return $check.array(parameters) ? parameters[0] : parameters;
    }

    /**
     * Checks whether the value is a literal
     *
     * @param {*} val the value to check
     * @returns {*}
     */
    static _isLiteral(val) {
        return (
            ($check.primitive(val) && $check.string(val)
                ? !val.startsWith('$')
                : true) ||
            ($check.object(val) && !$check.undefined(val.$literal))
        );
    }

    /**
     * Retrieves the literal value
     *
     * @param {*} val the value to check
     * @returns {*}
     */
    static _getLiteral(val) {
        if ($check.primitive(val)) {
            return val;
        } else if ($check.object(val) && !$check.undefined(val.$literal)) {
            return val.$literal;
        } else {
            return val;
        }
    }

    /**
     * Checks the type of an element
     *
     * @param {any} element
     * @returns {string}
     */
    static checkElementBasicType(element) {
        let result;

        if (typeof element === 'string') {
            result = `${element}`;
        } else {
            result = element;
        }

        return result;
    }
}

module.exports = AllowableFunctions;

/**
 * Filters out functions where the name or alias matches
 *
 * @param {import("./types").MongoQueryFunction} fn The function to be evaluated
 * @param {string} lowerName the lowercase name of the function to look for
 * @returns {boolean} If the result matches or not
 */
function filterFunctionsByName(fn, lowerName) {
    return (
        fn.name.toLowerCase() === lowerName ||
        (fn.aliases &&
            fn.aliases.map((a) => a.toLowerCase()).indexOf(lowerName) >= 0)
    );
}

/**
 * Filters out the functions based on their type
 *
 * @param {import("./types").MongoQueryFunction} fn The function to be evaluated
 * @param {string} lowerType the lowercase type of the function to look for
 * @returns {boolean} If the result matches or not
 */
function filterFunctionsByType(fn, lowerType) {
    return !fn.type || fn.type.toLowerCase() === lowerType;
}
