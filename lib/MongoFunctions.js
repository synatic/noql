//https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#arithmetic-expression-operators
//list of allowable expression operators that need to be mapped
const _allowableFunctions = [

    {
        name: "sum",
        parse: (items) => {
            return { $add: items }
        },
    },
    {
        name: "abs",
        parse: (items) => {
            return { $abs: items[0] }
        },
    },
    {
        name: "sqrt",
        parse: (items) => {
            return { $sqrt: items[0] }
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
    }

];

module.exports=_allowableFunctions;
