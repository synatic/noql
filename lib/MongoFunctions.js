const $check=require('check-types');

//https://docs.mongodb.com/manual/meta/aggregation-quick-reference/#arithmetic-expression-operators

/*
function definition

{
    name: "the name of the function in SQL statement e.g. abs",
    parsedName: "the mongo function name: $abs",
    allowQuery: (boolean) allow it to be used in mongo query/find and not just aggregates,
    function that takes in the parameters from the queries
    parse: (parameters) => {
        return { $abs: parameters[0] }
    },
}

*/
//list of allowable expression operators that need to be mapped
class AllowableFunctions {

    static _sqlTypeMapping={
        'varchar':'string',
        'decimal':'decimal',
        'int':'int',
        'datetime':'date',
        'time':'date',
        'float':'number',
        'char':'string',
        'nchar':'string'
    };

    static _list = [
        // Arithmetic Expression Operators
        {
            name: "abs",
            parsedName: "$abs",
            allowQuery: true,
            parse: (parameters) => {
                return { $abs: parameters[0] }
            },
        },
        {
            name: "+",
            allowQuery: true,
            parse: (numerator, denominator) => {
                return { $add: [numerator, denominator] }
            },
        },
        //not right
        {
            name: "sum",
            allowQuery: true,
            type: "function",
            parse: (parameters) => {
                return { $add: parameters }
            },
        },
        {
            name: "sum",
            type: "aggr_func",
            parse: (item) => {
                return { $add: `$${item}` }
            },
        },
        {
            name: "ceiling",
            parsedName: "$ceil",
            allowQuery: true,
            parse: (parameters) => {
                return { $ceil: parameters[0] }
            },
        },
        {
            name: "/",
            allowQuery: true,
            parse: (numerator, denominator) => {
                return { $divide: [numerator, denominator] }
            },
        },
        {
            name: "exp",
            parsedName: "$exp",
            allowQuery: true,
            parse: (parameters) => {
                return { $exp: parameters[0] }
            },
        },
        {
            name: "floor",
            parsedName: "$floor",
            allowQuery: true,
            parse: (parameters) => {
                return { $floor: parameters[0] }
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
            parse: (parameters) => {
                return { $log10: parameters[0] }
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
            allowQuery: true,
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
            parse: (items) => {
                return { $concatArrays: items.map(i => AllowableFunctions.checkElementBasicType(i)) }
            },
        },
        {
            name: "arrayFirst",
            parsedName: "$first",
            allowQuery: true,
            parse: (parameters) => {
                return { $first: parameters[0] }
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
            parse: (parameters) => {
                return { $isArray: parameters[0] }
            },
        },
        {
            name: "lastInArray",
            parsedName: "$last",
            allowQuery: true,
            parse: (parameters) => {
                return { $last: parameters[0] }
            },
        },
        {
            name: "firstInArray",
            parsedName: "$first",
            allowQuery: true,
            parse: (parameters) => {
                return { $first: parameters[0] }
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
            name: "reverseArray",
            parsedName: "$reverseArray",
            parse: (item) => {
                return { $reverseArray: `$${item}` }
            },
        },
        {
            name: "lengthOfArray",
            description:"Returns the number of elements in an array",
            parsedName: "$size",
            allowQuery: true,
            parse: (parameters) => {
                return { $size: parameters[0] }
            },
        },
        {
            name: "sumArray",
            description:"Sums the elements in an array",
            allowQuery:true,
            parse: (parameters,depth=0) => {
                if(parameters.length<2)throw new Error("Invalid parameters, requires at least the array field and ids");
                if(!((parameters[0].startsWith&&parameters[0].startsWith('$'))||(parameters[0].$map)))throw new Error("Invalid parameters, first parameter must be a column reference");

                let reduceInput=parameters[0];
                if(depth>0&&$check.string(reduceInput)){
                    reduceInput=`$$this.${reduceInput.startsWith('$')?reduceInput.substring(1):reduceInput}`;
                }

                let reduce={
                    $reduce:{
                        input: reduceInput,
                        initialValue: 0,
                        in:{
                            $sum:['$$value']
                        }
                    }
                }
                let curReduce=reduce;
                for (let i=1;i<parameters.length;i++){
                    let fieldName=parameters[i]&&parameters[i].$literal?parameters[i].$literal:parameters[i].startsWith&&parameters[i].startsWith('$')?parameters[i].substring(1):'';
                    if(!fieldName)throw new Error("Invalid parameter for field names");

                    if(i===parameters.length-1){
                        curReduce.$reduce.in.$sum.push(`$$this.${fieldName}`)
                    }else{
                        let reduce={
                            $reduce: {
                                input: `$$this.${fieldName}`,
                                initialValue: 0,
                                in:{$sum: ['$$value']}
                            }
                        };
                        curReduce.$reduce.in.$sum.push(reduce);
                        curReduce=reduce;
                    }
                }
                return reduce;
            },
        },
        {
            name: "arrayZip",
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

        //object helpers
        {
            name: "mergeObjects",
            parsedName: "$mergeObjects",
            allowQuery: true,
            parse: (parameters) => {
                return { $mergeObjects: parameters }
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
        //separate action
        // {
        //     name: "unwind",
        //     parsedName: "$unwind",
        //     parse: (items) => {
        //         return { $unwind: items[0] }
        //     },
        // },
        {
            name: "avg",
            parsedName: "$avg",
            allowQuery: true,
            type: "function",
            parse: (items) => {
                return { $avg: items }
            },
        },
        {
            name: "avg",
            parsedName: "$avg",
            type: "aggr_expr",
            parse: (items) => {
                return { $avg: items[0] }
            },
        },
        {
            name: "subtract",
            allowQuery: true,
            type: "function",
            parse: (items) => {
                return { $subtract: items }
            },
        },
        {
            //todo check count
            name: "count",
            parse: (parameter) => {
                return { $sum: 1 }
            },
        },
        {
            name: "convert",
            allowQuery: true,
            type: "function",
            parse: (parameters) => {
                const toSQLType=parameters[1]?(parameters[1].$literal||parameters[1]):null;
                if(!$check.string(toSQLType))throw new Error('Type not specified for convert');
                const toType=AllowableFunctions._sqlTypeMapping[toSQLType.toLowerCase()]||toSQLType;
                if(!['double','string','bool','date','int','objectId','long','decimal'].includes(toType))throw new Error(`Invalid type for convert:${toType}`);
                return { $convert: { input: parameters[0], to: toType } }
            },
        }
    ];

    static get functionList(){
        return AllowableFunctions._list;
    }

    static checkElementBasicType(element) {
        let result;

        if (typeof element === 'string') {
            result = `$${element}`;
        } else {
            result = element;
        }

        return result;
    }

    static parse(func, parameters) {
        let result;

        if (func.parse) return func.parse(parameters);

        //rewrite this
        //rather use $check with explicit returns
        //.length may be a proeprty on the object resultin in incorrect code


        if (typeof parameters === 'object') {
            if (parameters.length) {
                result = func.parse(parameters)
            } else {
                result = {
                    [func.parsedName || func.name]: parameters
                }
            }
        } else {
            result = {
                [func.parsedName]: AllowableFunctions.checkElementBasicType(parameters)
            };
        }

        return result;
    }
};

module.exports = AllowableFunctions;
