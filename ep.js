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

const r = SQLParser.parseSQL("select `collA.field3`, `collB.field3`, count(*) from collA inner join collB on `collA.field1` = `collB.field2` where `collA.date` > 12 group by `collA.field3`, `collB.field3` having count(*) > 250 order by `collA.field3`, `collB.field3`", 'aggregate');


console.log(JSON.stringify(r, null, 2))
