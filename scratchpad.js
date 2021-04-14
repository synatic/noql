const SQLParser=require('./lib/SQLParser');


let parsedVal=SQLParser.makeMongoQuery("select cast(1+`id` as varchar) as `id` from `customers`")
//let parsedVal=SQLParser.makeMongoAggregate("select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers )")
//let parsedVal=SQLParser.makeMongoAggregate("select `Address.Country` as Country,sum(id) as totalId from customers group by `Address.Country`")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
//select id,sum(sumArray((select sumArray(`Payments`,'Amount') as total from `Rentals`),'total')) as t from customers
console.log(JSON.stringify(parsedVal))

//select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers ) as t inner join (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers ) as t2 on t2.id=t1.id

let x;

