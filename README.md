# MoQL

![build status](https://github.com/synatic/sql-to-mongo/actions/workflows/ci-build.yml/badge.svg)

MoQL Converts SQL statements to Mongo find statements or aggregation pipelines. MoQL supports mySQL and Postgres Syntax, and generates Mongo 3.6 or greater compatible queries.

For full docs and a playground to try MoQL out, vist [https://moql.synatic.dev/](https://moql.synatic.dev/)

## Installation

Install MoQL using the [npm install command](https://docs.npmjs.com/downloading-and-installing-packages-locally):

```bash
npm i @synatic/sql-to-mongo
```

## Usage

MQL outputs an object with the type, either `query` or `aggregate`, along with the components of the Mongo query. To use the output object, construct a query with `MongoClient` from the [MongoDB NodeJS Driver](https://www.npmjs.com/package/mongodb): 

```js
const SQLParser = require('@synatic/sql-to-mongo');
const {MongoClient} = require('mongodb');

(async () => {
    try {
        client = new MongoClient('mongodb://127.0.0.1:27017');
        await client.connect();
        const db = client.db('sql-to-mongo-test');

        const parsedSQL = SQLParser.parseSQL('select id from `films` limit 10');
        if (parsedSQL.type === 'query') {
            console.log(
                await db
                    .collection(parsedSQL.collection)
                    .find(parsedSQL.query || {}, parsedSQL.projection || {})
                    .limit(parsedSQL.limit || 50)
                    .toArray()
            );
        } else if (parsedSQL.type === 'aggregate') {
            console.log(
                await db
                    .collection(parsedSQL.collections[0])
                    .aggregate(parsedSQL.pipeline)
                    .toArray()
            );
        }
    } catch (exp) {
        console.error(exp);
    }
})();
```

## Currently Unsupported SQL Statements

- Over
- CTE's
- Pivot
- Union


See more in the full docs at [https://moql.synatic.dev/](https://moql.synatic.dev/)