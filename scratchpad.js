const SQLParser=require('./lib/SQLParser');


//let parsedVal=SQLParser.parseSQL("select * from (select id,`First Name`,`Last Name`,arrayLength(`Rentals`,'id') as rentalCount from customers) as t")
let parsedVal=SQLParser.parseSQL("select id,`First Name`,`Last Name`,arraySum(`Rentals`,`id`,'test') as totalIdRentals from customers")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
let x;

