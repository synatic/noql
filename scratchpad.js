const {MongoClient} = require('mongodb');
const assert = require('assert');


const _connectionString="mongodb://127.0.0.1:27017";
const _dbName="sql-to-mongo-test";

async function run (){
    try{
        const client = new MongoClient(_connectionString);
        await client.connect();
        const dbs=await client.listDatabases().toArray();
        let xc;
    }catch(exp){
        console.error(exp)
    }



}
run();
