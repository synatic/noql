const SQLParser=require('./lib/SQLParser');

let parsedVal=SQLParser.parseSQL("select * from `films`")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
let x;

