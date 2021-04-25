const SQLParser=require('./lib/SQLParser');


//select id,(select count(*) as count from Rentals) as totalRentals from customers

let parsedVal=SQLParser.makeMongoAggregate("select mergeObjects((select t.CustomerID,t.Name),t.Rental) as `$$ROOT` from (select id as CustomerID,`First Name` as Name,unwind(Rentals) as Rental from customers) as t")


//let parsedVal=SQLParser.makeMongoAggregate("select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1")
//let parsedVal=SQLParser.makeMongoAggregate("select `Address.City` as City,abs(-1) as absId,avg(lengthOfArray(`Rentals`)) as AvgRentals from `customers` where `First Name` like 'm%' and absId >1 group by `Address.City`,absId")
//let parsedVal=SQLParser.makeMongoQuery("select makeObject(id,firstInArray(Rentals)) as `$$ROOT` from `customers`")
//let parsedVal=SQLParser.makeMongoAggregate("select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers )")
//let parsedVal=SQLParser.makeMongoAggregate("select `Address.Country` as Country,sum(id) as totalId from customers group by `Address.Country`")
//let parsedVal=SQLParser.makeMongoQuery("select (log10(3) * floor(a) +1) as s from collection")
//select id,sum(sumArray((select sumArray(`Payments`,'Amount') as total from `Rentals`),'total')) as t from customers
console.log(JSON.stringify(parsedVal.pipeline))

//select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers ) as t inner join (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers ) as t2 on t2.id=t1.id

let x;

