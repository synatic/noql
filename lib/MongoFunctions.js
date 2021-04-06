//https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#arithmetic-expression-operators

const { parseSQL } = require("./SQLParser");

//list of allowable expression operators that need to be mapped
class AllowableFunctions {
    static _list = [
        // Arithmetic Expression Operators
        {
            name: "abs",
            parsedName: "$abs",
            parse: (item) => {
                return { $abs: `$${item}` }
            },
        },
        {
            name: "+",
            parse: (numerator, denominator) => {
                return { $add: [numerator, denominator] }
            },
        },
        {
            name: "ceiling",
            parsedName: "$ceil",
            parse: (item) => {
                return { $ceil: `$${item}` }
            },
        },
        {
            name: "/",
            parse: (numerator, denominator) => {
                return { $divide: [numerator, denominator] }
            },
        },
        {
            name: "exp",
            parsedName: "$exp",
            parse: (item) => {
                return { $exp: `$${item}` }
            },
        },
        {
            name: "floor",
            parsedName: "$floor",
            parse: (item) => {
                return { $floor: `$${item}` }
            },
        },
        {
            name: "log",
            parsedName: "$ln",
            parse: (item) => {
                return { $log: [AllowableFunctions.checkElementBasicType(item[1]), AllowableFunctions.checkElementBasicType(item[0])] }
            },
        },
        {
            name: "log10",
            parsedName: "$log10",
            parse: (item) => {
                return { $log10: `$${item}` }
            },
        },
        {
            name: "mod",
            parsedName: "$mod",
            parse: (item) => {
                return { $mod: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "*",
            parse: (numerator, denominator) => {
                return { $multiply: [numerator, denominator] }
            },
        },
        {
            name: "pow",
            parsedName: "$pow",
            parse: (item) => {
                return { $pow: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "round",
            parsedName: "$round",
            parse: (item) => {
                return { $round: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "sqrt",
            parsedName: "$sqrt",
            parse: (item) => {
                return { $sqrt: `$${item}` }
            },
        },
        {
            name: "-",
            parse: (numerator, denominator) => {
                return { $subtract: [numerator, denominator] }
            },
        },
        {
            name: "trunc",
            parsedName: "$trunc",
            parse: (item) => {
                return { $trunc: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },


        // Array Expression Operators
        {
            name: "arrayElemAt",
            parsedName: "$arrayElemAt",
            parse: (item) => {
                return { $arrayElemAt: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "arrayToObject",
            parsedName: "$arrayToObject",
            parse: (item) => {
                return { $arrayToObject: `$${item}` }
            },
        },
        {
            name: "concatArrays",
            parsedName: "$concatArrays",
            parse: (item) => {
                return { $concatArrays: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "first",
            parsedName: "$first",
            parse: (item) => {
                return { $first: `$${item}` }
            },
        },
        {
            name: "in",
            parsedName: "$in",
            parse: (item) => {
                return { $in: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "indexOfArray",
            parsedName: "$indexOfArray",
            parse: (item) => {
                return { $indexOfArray: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "isArray",
            parsedName: "$isArray",
            parse: (item) => {
                return { $isArray: `$${item}` }
            },
        },
        {
            name: "last",
            parsedName: "$last",
            parse: (item) => {
                return { $last: `$${item}` }
            },
        },
        {
            name: "map",
            parsedName: "$map",
            parse: (item) => {
                return { $map: `$${item}` }
            },
        },
        {
            name: "objectToArray",
            parsedName: "$objectToArray",
            parse: (item) => {
                return { $objectToArray: `$${item}` }
            },
        },
        {
            name: "range",
            parsedName: "$range",
            parse: (item) => {
                return { $range: `$${item}` }
            },
        },
        {
            name: "reduce",
            parsedName: "$reduce",
            parse: (item) => {
                return { $reduce: `$${item}` }
            },
        },
        {
            name: "reverseArray",
            parsedName: "$reverseArray",
            parse: (item) => {
                return { $reverseArray: `$${item}` }
            },
        },
        {
            name: "size",
            parsedName: "$size",
            parse: (item) => {
                return { $size: `$${item}` }
            },
        },
        {
            name: "slice",
            parsedName: "$slice",
            parse: (item) => {
                return { $slice: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "zip",
            parsedName: "$zip",
            parse: (item) => {
                return { $zip: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },



        // Boolean Expression Operators
        {
            name: "and",
            parsedName: "$and",
            parse: (item) => {
                return { $and: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "not",
            parsedName: "$not",
            parse: (item) => {
                return { $not: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "or",
            parsedName: "$or",
            parse: (item) => {
                return { $or: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },



        // Comparison Expression Operators
        {
            name: "cmp",
            parsedName: "$cmp",
            parse: (item) => {
                return { $cmp: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "eq",
            parsedName: "$eq",
            parse: (item) => {
                return { $eq: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "gt",
            parsedName: "$gt",
            parse: (item) => {
                return { $gt: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "gte",
            parsedName: "$gte",
            parse: (item) => {
                return { $gte: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "lt",
            parsedName: "$lt",
            parse: (item) => {
                return { $lt: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "lte",
            parsedName: "$lte",
            parse: (item) => {
                return { $lte: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },
        {
            name: "ne",
            parsedName: "$ne",
            parse: (item) => {
                return { $ne: [AllowableFunctions.checkElementBasicType(item[0]), AllowableFunctions.checkElementBasicType(item[1])] }
            },
        },



        {
            name: "unwind",
            parsedName: "$unwind",
            parse: (items) => {
                return { $unwind: items[0] }
            },
        },
        {
            name: "avg",
            parsedName: "$avg",
            parse: (item) => {
                return { $avg: { ...item } }
            },
        },
        {
            name: "subtract",
            parse: (items) => {
                return { $subtract: items }
            },
        },
        {
            name: "count",
            parse: (items) => {
                return { $count: items }
            },
        },
        {
            name: "convert",
            parse: (items) => {
                return { $convert: { input: items[0], to: items[1].$literal } }
            },
        }
    ];

    static checkElementBasicType(element) {
        let result;

        if (typeof element === 'string') {
            result = `$${element}`;
        } else {
            result = element;
        }

        return result;
    }

    static parse(func, element) {
        let result;

        if (typeof element === 'object') {
            if (element.length) {
                result = func.parse(element)
            } else {
                result = {
                    [func.parsedName]: element
                }
            }
        } else {
            result = {
                [func.parsedName]: AllowableFunctions.checkElementBasicType(element)
            };
        }

        return result;
    }
};

module.exports = AllowableFunctions;
