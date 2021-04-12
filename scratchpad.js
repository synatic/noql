const SQLParser=require('./lib/SQLParser');

let parsedVal=SQLParser.parseSQLtoAST("select  cast(a as Int) as s from collection")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
let x;

