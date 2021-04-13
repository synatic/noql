const SQLParser=require('./lib/SQLParser');

let parsedVal=SQLParser.makeMongoAggregate("select  `Address.Country` as Country,avg(arraySize(`Rentals`)) as AvgRentals from `customers` where `First Name` like 'm%' group by `Country`")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
let x;

