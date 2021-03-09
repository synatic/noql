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

const r = SQLParser.makeMongoQuery("select `a.b` as Id ,Name from `global-test` where `a.b`>1 limit 10 offset 5");

console.log(r)
