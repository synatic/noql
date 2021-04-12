const SQLParser=require('./lib/SQLParser');

let parsedVal=SQLParser.makeMongoQuery("SELECT ceiling(id + arraySize(Rentals) + 1) as testComplex FROM customers")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
let x;

