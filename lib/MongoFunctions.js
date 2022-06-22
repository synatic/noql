const $check = require('check-types');
const $convert = require('@synatic/type-magic');
const ObjectID = require('bson-objectid');

// https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#arithmetic-expression-operators

/**
 * Manages mapping functions from sql to mongo
 *
 */
class AllowableFunctions {
    /**
     * The type mapping from sql to mongo for cast
     *
     * @returns {{datetime: string, varchar: string, char: string, nchar: string, time: string, decimal: string, float: string, int: string}}
     */
    static get _sqlTypeMapping() {
        return {
            varchar: 'string',
            decimal: 'decimal',
            int: 'int',
            datetime: 'date',
            time: 'date',
            float: 'number',
            char: 'string',
            nchar: 'string',
        };
    }
    /**
     * Gets an allowed function by name, returns null if not found
     *
     * @param name
     * @readonly
     * @static
     * @memberof AllowableFunctions
     * @returns {import('./types').MongoQueryFunction|null}
     */
    static functionByName(name) {
        if (!name) {
            return null;
        }
        return AllowableFunctions.functionMappings.find(
            (fn) => fn.name.toLowerCase() === name.toLowerCase()
        );
    }

    /**
     * Gets the list of function mappings between sql and mongo
     *
     * @returns {import('./types').MongoQueryFunction[]}
     */
    static get functionMappings() {
        return [
            // region Columns
            {
                name: 'field_exists',
                allowQuery: true,
                parse: (parameters) => {
                    return {[parameters[0]]: {$exists: parameters[1]}};
                },
            },
            // endregion

            // region Object Operators
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
            },
            {
                name: 'merge_objects',
                allowQuery: true,
                parse: (parameters) => {
                    return {$mergeObjects: parameters};
                },
            },

            // endregion

            // region Arithmetic Operators
            {
                name: 'avg',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$avg: parameters};
                },
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
            },
            {
                name: 'atan2',
                allowQuery: true,
                parse: (parameters) => {
                    return {$atan2: parameters};
                },
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
            },
            {
                name: 'divide',
                allowQuery: true,
                parse: (parameters) => {
                    return {$divide: parameters};
                },
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
            },
            {
                name: 'ln',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $ln: AllowableFunctions._getSingleParameter(parameters),
                    };
                },
            },
            {
                name: 'log',
                allowQuery: true,
                parse: (parameters) => {
                    return {$log: parameters};
                },
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
            },
            {
                name: 'max',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$max: parameters};
                },
            },
            {
                name: 'min',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$min: parameters};
                },
            },
            {
                name: 'mod',
                allowQuery: true,
                parse: (parameters) => {
                    return {$mod: parameters};
                },
            },
            {
                name: 'multiply',
                allowQuery: true,
                parse: (parameters) => {
                    return {$multiply: parameters};
                },
            },
            {
                name: 'pow',
                allowQuery: true,
                parse: (parameters) => {
                    return {$pow: parameters};
                },
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
            },
            {
                name: 'rand',
                allowQuery: true,
                parse: (parameters) => {
                    return {$rand: {}};
                },
            },
            {
                name: 'round',
                allowQuery: true,
                parse: (parameters) => {
                    return {$round: parameters};
                },
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
            },
            {
                name: 'subtract',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$subtract: parameters};
                },
            },
            {
                name: 'sum',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$add: parameters};
                },
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
            },
            {
                name: 'trunc',
                allowQuery: true,
                parse: (parameters) => {
                    return {$trunc: parameters};
                },
            },

            // endregion

            // region Aggregate Functions
            {
                name: 'sum',
                type: 'aggr_func',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $sum: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
            },
            {
                name: 'avg',
                allowQuery: true,
                type: 'aggr_func',
                parse: (parameters) => {
                    return {
                        $avg: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
            },
            {
                name: 'min',
                allowQuery: true,
                type: 'aggr_func',
                parse: (parameters) => {
                    return {
                        $min: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
            },
            {
                name: 'max',
                allowQuery: true,
                type: 'aggr_func',
                parse: (parameters) => {
                    return {
                        $max: AllowableFunctions._getSingleParameter(
                            parameters
                        ),
                    };
                },
            },
            {
                // todo check count
                name: 'count',
                parse: (parameter) => {
                    return {$sum: 1};
                },
            },

            // ToDo:add aggregate function with
            // $addToSet

            // endregion

            // region String Functions

            /*
            $strcasecmp
             */

            {
                name: 'concat',
                parsedName: '$concat',
                allowQuery: true,
                parse: (parameters) => {
                    return {$concat: parameters};
                },
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
            },
            // endregion

            // region Conversion Functions
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
                    if (
                        ![
                            'double',
                            'string',
                            'bool',
                            'date',
                            'int',
                            'objectId',
                            'long',
                            'decimal',
                        ].includes(toType)
                    )
                        throw new Error(`Invalid type for convert:${toType}`);
                    return {$convert: {input: parameters[0], to: toType}};
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
                            return new ObjectID(
                                AllowableFunctions._getLiteral(paramVal)
                            );
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
            },
            {
                name: 'ifnull',
                allowQuery: true,
                parse: (parameters) => {
                    return {$ifNull: parameters};
                },
            },

            // endregion

            // region Date Functions
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
            },
            // endregion

            // region Arithmetic Expression Operators

            {
                name: '+',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$add: [numerator, denominator]};
                },
            },
            {
                name: '/',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$divide: [numerator, denominator]};
                },
            },
            {
                name: '*',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$multiply: [numerator, denominator]};
                },
            },

            {
                name: '-',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$subtract: [numerator, denominator]};
                },
            },
            {
                name: '%',
                allowQuery: true,
                parse: (numerator, denominator) => {
                    return {$mod: [numerator, denominator]};
                },
            },

            // endregion

            // region Array Expression Operators

            {
                name: 'is_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {
                        $isArray:
                            AllowableFunctions._getSingleParameter(parameters),
                    };
                },
            },
            {
                name: 'all_elements_true',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$allElementsTrue: parameters};
                },
            },
            {
                name: 'any_element_true',
                allowQuery: true,
                type: 'function',
                parse: (parameters) => {
                    return {$anyElementTrue: parameters};
                },
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
            },
            {
                name: 'array_elem_at',
                allowQuery: true,
                parse: (parameters) => {
                    return {$arrayElemAt: parameters};
                },
            },
            {
                name: 'indexof_array',
                allowQuery: true,
                parse: (parameters) => {
                    return {$indexOfArray: parameters};
                },
            },
            {
                name: 'array_range',
                allowQuery: true,
                parse: (parameters) => {
                    return {$range: parameters};
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
            },
            {
                name: 'concat_arrays',
                allowQuery: true,
                parse: (parameters) => {
                    return {$concatArrays: parameters};
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
            },
            {
                name: 'set_union',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setUnion: parameters};
                },
            },
            {
                name: 'set_difference',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setDifference: parameters};
                },
            },
            {
                name: 'set_intersection',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setIntersection: parameters};
                },
            },
            {
                name: 'set_equals',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setEquals: parameters};
                },
            },
            {
                name: 'set_is_subset',
                allowQuery: true,
                parse: (parameters) => {
                    return {$setIsSubset: parameters};
                },
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
            },

            // endregion

            // region Boolean Expression Operators
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
            },

            // endregion

            // region Comparison Expression Operators
            {
                name: 'cmp',
                allowQuery: true,
                parse: (parameters) => {
                    return {$cmp: parameters};
                },
            },
            {
                name: 'eq',
                allowQuery: true,
                parse: (parameters) => {
                    return {$eq: parameters};
                },
            },
            {
                name: '=',
                parse: (numerator, denominator) => {
                    return {$eq: [numerator, denominator]};
                },
            },
            {
                name: 'gt',
                allowQuery: true,
                parse: (parameters) => {
                    return {$gt: parameters};
                },
            },
            {
                name: '>',
                parse: (numerator, denominator) => {
                    return {$gt: [numerator, denominator]};
                },
            },
            {
                name: 'gte',
                allowQuery: true,
                parse: (parameters) => {
                    return {$gte: parameters};
                },
            },
            {
                name: '>=',
                parse: (numerator, denominator) => {
                    return {$gte: [numerator, denominator]};
                },
            },
            {
                name: 'lt',
                allowQuery: true,
                parse: (parameters) => {
                    return {$lt: parameters};
                },
            },
            {
                name: '<',
                parse: (numerator, denominator) => {
                    return {$lt: [numerator, denominator]};
                },
            },
            {
                name: 'lte',
                allowQuery: true,
                parse: (parameters) => {
                    return {$lte: parameters};
                },
            },
            {
                name: '<=',
                parse: (numerator, denominator) => {
                    return {$lte: [numerator, denominator]};
                },
            },
            {
                name: 'ne',
                allowQuery: true,
                parse: (parameters) => {
                    return {$ne: parameters};
                },
            },
            {
                name: '!=',
                parse: (numerator, denominator) => {
                    return {$ne: [numerator, denominator]};
                },
            },
            {
                name: 'unset',
                allowQuery: true,
                parse: (fields) => {
                    return {$unset: fields};
                },
                requiresAs: false,
            },
            // endregion

            // separate action
            // {
            //     name: "unwind",
            //     parsedName: "$unwind",
            //     parse: (items) => {
            //         return { $unwind: items[0] }
            //     },
            // },
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
