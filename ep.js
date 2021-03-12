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

const r = SQLParser.parseSQL("select age / 10 / 20 as aggr from `person` where `state`='a'", 'aggregate');

console.log(JSON.stringify(r, null, 2))
