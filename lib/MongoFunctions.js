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
                return { $ln: `$${item}` }
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
            name: "*",
            parse: (numerator, denominator) => {
                return { $multiply: [numerator, denominator] }
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
            name: "first",
            parsedName: "$first",
            parse: (items) => {
                return { $first: items[0] }
            },
        },
        {
            name: "zip",
            parsedName: "$zip",
            parse: (items) => {
                return { $zip: items }
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
            name: "convert",
            parse: (items) => {
                return { $convert: { input: items[0], to: items[1].$literal } }
            },
        },
        {
            name: "size",
            parsedName: "$size",
        }];

    static parse(func, element) {
        let result;

        if (typeof element === 'string') {
            result = {
                [func.parsedName]: `$${element}`
            };
        } else if (typeof element === 'object') {
            result = {
                [func.parsedName]: element
            }
        }

        return result;
    }
};

module.exports = AllowableFunctions;
