
const SQLParser=require('./lib/SQLParser');


// select id,(select count(*) as count from Rentals) as totalRentals from customers

// let parsedVal=SQLParser.makeMongoAggregate("select c.*,`customer-notes`.*,cn2.*  from customers c inner join `customer-notes` on `customer-notes`.id=c.id left outer join (select * from `customer-notes2` where id <3) cn2 on cn2.id=firstInArray(`customer-notes`.id)" )
// let parsedVal=SQLParser.makeMongoAggregate("select c.*,cn.* from customers c inner join `customer-notes` cn on cn.id=c.id and (cn.id>2 or cn.id<5)" )


// let parsedVal=SQLParser.makeMongoAggregate("select `Address.Country` as Country, sum(case when `Address.City`='Ueda' then 1 else 0 end) as Ueda,sum(case when `Address.City`='Tete' then 1 else 0 end) as Tete from `customers` group by `Country`" )
// select * from `customers` where `Address.City` not in ('Japan','Pakistan')
// select sum(case when id <10 then 1 when id > 10 then -1 else 0 end) as sumCase from `customers`
// select  AVG((select staffId as '$$ROOT' from Rentals)) as exprVal from `customer`")
// const parsedVal=SQLParser.makeMongoAggregate("select  `customer-notes`.*,YEAR(DATE_FROM_STRING(`date`)) as year from `customer-notes`")

// let parsedVal=SQLParser.makeMongoAggregate("select `Address.City` as City,abs(-1) as absId,avg(lengthOfArray(`Rentals`)) as AvgRentals from `customers` where `First Name` like 'm%' and absId >1 group by `Address.City`,absId")
const parsedVal=SQLParser.makeMongoAggregate("select `First Name`,`Address.City` as City,abs(-1) as absId from `customers` where `First Name` like 'm%' and abs(-1) >=1")

// let parsedVal=SQLParser.makeMongoAggregate("select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers )")
// let parsedVal=SQLParser.makeMongoAggregate("select `Address.Country` as Country,sum(id) as totalId from customers group by `Address.Country`")

return console.log(JSON.stringify(parsedVal.pipeline?parsedVal.pipeline:parsedVal.query))

// select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers ) as t inner join (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id') from customers ) as t2 on t2.id=t1.id


// const _arithmetic=require('./test/expressionOperators/ArithmeticExpressionOperators.json')
//
// const newTests=[];
// for (const expr of _arithmetic){
//     for (const k in expr){
//         newTests.push({
//             name:`Arithmetic:${k}`,
//             output:{
//                 limit:100,
//                 projection:expr[k].aggregateOutput.pipeline[0].$project,
//                 collection:expr[k].aggregateOutput.collections[0]
//             },
//
//             query:expr[k].query,
//
//         })
//     }
// }
//
// console.log(JSON.stringify(newTests,null,4))
//
//
// let x;

