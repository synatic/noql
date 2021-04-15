// const MongoClient = require('mongodb').MongoClient;

// const uri = "mongodb+srv://admin:123@cluster0.v4cc9.mongodb.net/myFirstDatabase?retryWrites=true&w=majority";
// const client = new MongoClient(uri, { useNewUrlParser: true, useUnifiedTopology: true });


// client.connect(err => {
//   const collection = client.db("test").collection("devices");
//   // perform actions on the collection object

//   console.log(collection)

//   client.close();
// });


const SQLParser = require('./lib/SQLParser.js');

// const r = SQLParser.parseSQL("select * from customers INNER JOIN films ON `customers.id` = `films.id` INNER JOIN films ON `films.id` = `stores.id`", 'aggregate');

// const r = SQLParser.parseSQL("select `Address.City` as City,avg(size(`Rentals`)) as AvgRentals from `customers` where `First Name` like 'm%' group by `Address.City`", 'aggregate');

// const r = SQLParser.parseSQL("select `collA.field3`, `collB.field3`, count(*) from collA inner join collB on `collA.field1` = `collB.field2` where `collA.date` > 12 group by `collA.field3`, `collB.field3` having count(*) > 250 order by `collA.field3`, `collB.field3`", 'aggregate');


// const r = SQLParser.parseSQL("select a.* from coll as a from table2 as t2 inner join table1 as t1", "aggregate");

// const r = SQLParser.parseSQL("select a,b,unwind(c) as c,c.a,c.b from `table` as t1  inner join `table2` as t2  on a=b", 'aggregate');

// let r
// //sql-ex example 
// //1
// r = SQLParser.parseSQL("select model, speed, hd from pc where price < 500");
// //2
// r = SQLParser.parseSQL("select distinct maker from product where type = 'Printer'", 'aggregate');
// //3
// r = SQLParser.parseSQL("select model, ram, screen from laptop where price > 1000");
// //4
// r = SQLParser.parseSQL("select * from printer where color = 'y'");
// //5
// // r = SQLParser.parseSQL("select model, speed, hd from pc where (cd = '12x' or cd = '24x') and price < 600");

// //6
// r = SQLParser.parseSQL("select distinct product.maker, laptop.speed from laptop inner join product on laptop.model = product.model where laptop.hd >= 10");

// //7
// r = SQLParser.parseSQL(`select distinct product.model, pc.price from product  
// inner join pc on product.model = pc.model
// where maker = 'B'
// union
// select distinct product.model, laptop.price from product 
// inner join laptop on product.model = laptop.model
// where maker = 'B'
// union
// select distinct product.model, printer.price from product 
// inner join printer on product.model = printer.model
// where maker = 'B'`);

// //8
// r = SQLParser.parseSQL(`select distinct maker from product
// where type = 'pc'
// except 
// select distinct maker from product
// where type = 'laptop'
// `);

// //9
// r = SQLParser.parseSQL(`select distinct maker from product 
// inner join pc on product.model = pc.model
// where pc.speed >= 450
// `);

// // //10
// r = SQLParser.parseSQL(`select distinct model, price from printer 
// where price = 
// (select max(price) from printer)
// `, 'aggregate');

// //11
// r = SQLParser.parseSQL(`select avg(speed) as speed from pc`);

// //12
// r = SQLParser.parseSQL(`select avg(speed) as speed from laptop 
// where price > 1000`);

// //13
// r = SQLParser.parseSQL(`select avg(speed) as speed from pc 
// inner join product on pc.model = product.model
// where product.maker = 'A'
// `);

// //14
// r = SQLParser.parseSQL(`select ships.class, ships.name, classes.country from ships
// inner join classes on classes.class = ships.class
// where classes.numGuns >= 10
// `);

// //15
// r = SQLParser.parseSQL(`select hd from pc group by hd having count(model) > 1`);




// r = SQLParser.parseSQL("select log(`Replacement Cost`, 2) as aggr from `films`", 'aggregate');

// r = SQLParser.parseSQL("select round(`Repl`, 2) as aggr from `films`", 'aggregate')

// r = SQLParser.parseSQL("select `First Name`, `Last Name`, unwind(Rentals) as Rental, Film.* as Film from customers inner join films as Film on `Rental.filmId`=`Film.id`", 'aggregate')
// r = SQLParser.parseSQL("select `First Name`, `Last Name`, unwind(Rentals) as Rental from customers inner join films on `Rental.filmId`=`films.id`", 'aggregate')


// r = SQLParser.parseSQL("select `First Name`, `Last Name` from c inner join f on `c.filmId`=`f.id`", 'aggregate')

// r = SQLParser.parseSQL("select a,b,unwind(c) as c,c.a,c.b from `table` as t1  inner join `table2` as t2  on a=b", c)

// r = SQLParser.parseSQL(`SELECT *, inventory_docs
//     FROM orders
//     WHERE inventory_docs IN (SELECT *
//     FROM inventory
//     WHERE sku= orders.item)`, 'aggregate')

// r = SQLParser.parseSQL(`SELECT *, holidays
// FROM absences
// WHERE holidays in (SELECT name, date
// FROM holidays
// WHERE year = 2018)`, 'aggregate')


// r = SQLParser.parseSQL("select (log10(3) * floor(a) + 1) as s from collection")
// r = SQLParser.parseSQL("select cast(123 as Int) as s from collection")
// r = SQLParser.parseSQL("SELECT *, convert(id, int) as idConv FROM customers")
// r = SQLParser.parseSQL("select count(*) as theCount from collection")
// r = SQLParser.parseSQL("select id, `First Name`,`Last Name`,sumArray(Rentals,filmId) as totalIdRentals from customers")
// r = SQLParser.makeMongoQuery("select cast(1+`id` as varchar) as `id` from `customers`")

// r = SQLParser.parseSQL("select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals) as rentalCount from customers) as t")
r = SQLParser.parseSQL("select * from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals) from customers ) as t1 inner join (select id,`First Name`,lengthOfArray(Rentals) from films ) as t2 on `t2.id`=`t1.id`")

console.log(JSON.stringify(r, null, 2))
