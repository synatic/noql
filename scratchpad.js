const SQLParser=require('./lib/SQLParser');

let parsedVal=SQLParser.makeMongoQuery("select log10(3) * floor(a) as s from collection")
let x;

