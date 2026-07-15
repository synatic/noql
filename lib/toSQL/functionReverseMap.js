// Maps a Mongo aggregation operator key to the noql SQL function name used
// to reconstruct it, plus how to pull the ordered SQL argument list out of
// the operator's raw (not-yet-converted) value.
//
// Many SQL names collapse to the same Mongo operator (see the reverse-
// mapping collisions documented during the design of this module); where
// that happens we pick one canonical SQL spelling. Round-tripping through
// the forward converter still produces the same pipeline shape either way.

function args(value) {
    return Array.isArray(value) ? value : [value];
}

function simple(sqlName) {
    return {sqlName, argsOf: args};
}

// several date operators accept either a bare expression or a
// `{date, [timezone]}` object; the forward converter emits the bare form
// when reached via EXTRACT(...) and the object form via a direct function
// call, so unwrap defensively either way.
function dateArg(v) {
    return v && typeof v === 'object' && !Array.isArray(v) && 'date' in v ? v.date : v;
}

// operator -> {sqlName, argsOf(value) => raw mongo sub-expressions in SQL arg order}
const OPERATOR_MAP = {
    // arithmetic / math (unary and n-ary)
    $abs: simple('ABS'),
    $acos: simple('ACOS'),
    $acosh: simple('ACOSH'),
    $asin: simple('ASIN'),
    $asinh: simple('ASINH'),
    $atan: simple('ATAN'),
    $atan2: simple('ATAN2'),
    $atanh: simple('ATANH'),
    $ceil: simple('CEIL'),
    $degreesToRadians: simple('DEGREES_TO_RADIANS'),
    $exp: simple('EXP'),
    $floor: simple('FLOOR'),
    $ln: simple('LN'),
    $log10: simple('LOG10'),
    $log: simple('LOG'),
    $mod: simple('MOD'),
    $pow: simple('POW'),
    $radiansToDegrees: simple('RADIANS_TO_DEGREES'),
    $rand: {sqlName: 'RAND', argsOf: () => []},
    $round: simple('ROUND'),
    $sin: simple('SIN'),
    $sinh: simple('SINH'),
    $sqrt: simple('SQRT'),
    $tan: simple('TAN'),
    $tanh: simple('TANH'),
    $trunc: simple('TRUNC'),
    $binarySize: simple('BINARY_SIZE'),

    // object operators
    $mergeObjects: simple('MERGE_OBJECTS'),

    // string functions
    $concat: simple('CONCAT'),
    $trim: {sqlName: 'TRIM', argsOf: (v) => [v.input, ...(v.chars ? [v.chars] : [])]},
    $ltrim: {sqlName: 'LTRIM', argsOf: (v) => [v.input, ...(v.chars ? [v.chars] : [])]},
    $rtrim: {sqlName: 'RTRIM', argsOf: (v) => [v.input, ...(v.chars ? [v.chars] : [])]},
    $substrCP: simple('SUBSTR_CP'),
    $substrBytes: simple('SUBSTR_BYTES'),
    $toUpper: simple('UPPER'),
    $toLower: simple('LOWER'),
    $replaceOne: {sqlName: 'REPLACE', argsOf: (v) => [v.input, v.find, v.replacement]},
    $replaceAll: {sqlName: 'REPLACE_ALL', argsOf: (v) => [v.input, v.find, v.replacement]},
    $strLenBytes: simple('STRLEN'),
    $strLenCP: simple('STRLEN_CP'),
    $split: {sqlName: 'SPLIT', argsOf: (v) => v},

    // conversion functions
    $convert: {sqlName: 'CONVERT', argsOf: (v) => [v.input, v.to]},
    $toDate: simple('TO_DATE'),
    $toString: simple('TO_STRING'),
    $toDecimal: simple('TO_DECIMAL'),
    $toDouble: simple('TO_DOUBLE'),
    $toInt: simple('TO_INT'),
    $toLong: simple('TO_LONG'),
    $toBool: simple('TO_BOOL'),
    $toObjectId: simple('TO_OBJECTID'),
    $type: simple('TYPEOF'),
    $ifNull: simple('COALESCE'),

    // date functions
    $dateFromString: {
        sqlName: 'DATE_FROM_STRING',
        argsOf: (v) => [v.dateString, ...(v.format ? [v.format] : [])],
    },
    $dateToString: {
        sqlName: 'DATE_TO_STRING',
        argsOf: (v) => [v.date, ...(v.format ? [v.format] : [])],
    },
    $dateToParts: {sqlName: 'DATE_TO_PARTS', argsOf: (v) => [dateArg(v)]},
    $dayOfMonth: {sqlName: 'DAY_OF_MONTH', argsOf: (v) => [dateArg(v)]},
    $dayOfWeek: {sqlName: 'DAY_OF_WEEK', argsOf: (v) => [dateArg(v)]},
    $dayOfYear: {sqlName: 'DAY_OF_YEAR', argsOf: (v) => [dateArg(v)]},
    $isoDayOfWeek: {sqlName: 'ISO_DAY_OF_WEEK', argsOf: (v) => [dateArg(v)]},
    $isoWeek: {sqlName: 'ISO_WEEK', argsOf: (v) => [dateArg(v)]},
    $isoWeekYear: {sqlName: 'ISO_WEEK_YEAR', argsOf: (v) => [dateArg(v)]},
    $hour: {sqlName: 'HOUR', argsOf: (v) => [dateArg(v)]},
    $millisecond: {sqlName: 'MILLISECOND', argsOf: (v) => [dateArg(v)]},
    $minute: {sqlName: 'MINUTE', argsOf: (v) => [dateArg(v)]},
    $month: {sqlName: 'MONTH', argsOf: (v) => [dateArg(v)]},
    $second: {sqlName: 'SECOND', argsOf: (v) => [dateArg(v)]},
    $week: {sqlName: 'WEEK', argsOf: (v) => [dateArg(v)]},
    $year: {sqlName: 'YEAR', argsOf: (v) => [dateArg(v)]},
    $dateTrunc: {sqlName: 'DATE_TRUNC', argsOf: (v) => [v.date, v.unit]},
    $dateAdd: {
        sqlName: 'DATE_ADD',
        argsOf: (v) => [v.startDate, v.unit, v.amount],
    },
    $dateSubtract: {
        sqlName: 'DATE_SUBTRACT',
        argsOf: (v) => [v.startDate, v.unit, v.amount],
    },
    $dateDiff: {
        sqlName: 'DATE_DIFF',
        argsOf: (v) => [v.startDate, v.endDate, v.unit],
    },

    // array operators
    $isArray: simple('IS_ARRAY'),
    $allElementsTrue: simple('ALL_ELEMENTS_TRUE'),
    $anyElementTrue: simple('ANY_ELEMENT_TRUE'),
    $size: simple('SIZE_OF_ARRAY'),
    $reverseArray: simple('REVERSE_ARRAY'),
    $arrayElemAt: simple('ARRAY_ELEM_AT'),
    $indexOfArray: simple('INDEXOF_ARRAY'),
    $range: simple('ARRAY_RANGE'),
    $concatArrays: simple('CONCAT_ARRAYS'),
    $objectToArray: simple('OBJECT_TO_ARRAY'),
    $arrayToObject: simple('ARRAY_TO_OBJECT'),
    $setUnion: simple('SET_UNION'),
    $setDifference: simple('SET_DIFFERENCE'),
    $setIntersection: simple('SET_INTERSECTION'),
    $setEquals: simple('SET_EQUALS'),
    $setIsSubset: simple('SET_IS_SUBSET'),

    // comparison (function form; expressionToAST prefers symbol form and
    // only falls back to these when reconstructing inside CASE/JOIN trees
    // that always use the operator-object shape anyway)
    $cmp: simple('CMP'),
};

// $convert.to (Mongo BSON type alias) -> SQL CAST/CONVERT type name.
// Several SQL type names collapse to the same Mongo type; we pick one
// canonical spelling per Mongo type.
const CONVERT_TYPE_MAP = {
    double: 'DOUBLE',
    string: 'VARCHAR',
    bool: 'BOOL',
    date: 'DATETIME',
    int: 'INT',
    objectId: 'OBJECTID',
    long: 'LONG',
    decimal: 'DECIMAL',
    number: 'FLOAT',
};

module.exports = {OPERATOR_MAP, CONVERT_TYPE_MAP};
