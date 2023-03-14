# NoQL - Not Only SQL

![build status](https://github.com/synatic/noql/actions/workflows/ci-build.yml/badge.svg)

NoQL Converts SQL statements to Mongo find statements or aggregation pipelines. NoQL supports mySQL and Postgres Syntax, and generates Mongo 3.6 or greater compatible queries.

For full docs and a playground to try NoQL out, vist [https://noql.synatic.dev/](https://noql.synatic.dev/)

## Installation

Install NoQL using the [npm install command](https://docs.npmjs.com/downloading-and-installing-packages-locally):

```bash
npm i @synatic/noql
```

## Usage

NoQL outputs an object with the type, either `query` or `aggregate`, along with the components of the Mongo query. To use the output object, construct a query with `MongoClient` from the [MongoDB NodeJS Driver](https://www.npmjs.com/package/mongodb): 

```js
const SQLParser = require('@synatic/noql');
const {MongoClient} = require('mongodb');

(async () => {
    try {
        client = new MongoClient('mongodb://127.0.0.1:27017');
        await client.connect();
        const db = client.db('noql-test');

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

## NoQL Output Examples

NoQL outputs an object with the type, either `query` or `aggregate`, along with the components of the Mongo query. Here are some examples of the output:

For a straight query: 

```js
SQLMongoParser.parseSQL("select id from `films` where `id` > 10 limit 10")
```

NoQL will output:
```json
{
    "limit": 10,
    "collection": "films",
    "projection": {
        "id": "$id"
    },
    "query": {
        "id": {
            "$gt": 10
        }
    },
    "type": "query"
}
```

For an aggregate query:

```js
SQLMongoParser.makeMongoAggregate("select id from `films` where `id` > 10 group by id")
```

NoQL will output:

```json
{
    "pipeline": [
        {
            "$match": {
                "id": {
                    "$gt": 10
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "id": "$id"
                }
            }
        },
        {
            "$project": {
                "id": "$_id.id",
                "_id": 0
            }
        }
    ],
    "collections": [
        "films"
    ]
}
```

## Currently Unsupported SQL Statements

- Over
- CTE's
- Pivot
- Union

See more in the full docs at [https://noql.synatic.dev/](https://noql.synatic.dev/)