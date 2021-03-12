//https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#arithmetic-expression-operators
//list of allowable expression operators that need to be mapped
const _allowableFunctions = [
    // Arithmetic Expression Operators
    {
        name: "abs",
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
        parse: (item) => {
            return { $exp: `$${item}` }
        },
    },
    {
        name: "floor",
        parse: (item) => {
            return { $floor: `$${item}` }
        },
    },
    {
        name: "log",
        parse: (item) => {
            return { $ln: `$${item}` }
        },
    },
    {
        name: "log10",
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
        parse: (items) => {
            return { $first: items[0] }
        },
    },
    {
        name: "zip",
        parse: (items) => {
            return { $zip: items }
        },
    },

    {
        name: "avg",
        parse: (item) => {
            return { $avg: `$${item}` }
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
        parse: (items) => {
            return { $size:items[0] }
        },
    }

];

module.exports = _allowableFunctions;
