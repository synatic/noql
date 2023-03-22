## Synopsis

`#!js parseSQL(sqlStatement, options)`

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
    * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)
    * `type` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The type of query. Can be `query` or `aggregate`.
    * `collection` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The collection to query for a `query` type.
    * `query` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The query object to use for a `query` type.
    * `projection` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The projection object to use for a `query` type.
    * `limit` [`#!js <number>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#number_type) - The limit to use for a `query` type.
    * `pipeline` [`#!js <array>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - The aggregate array to use for an `aggregate` type.
    * `collections` [`#!js <array>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - The collections to use for an `aggregate` type.

## Description

Parses the given SQL statement to a JSON output object containing Mongo query components. It automatically determines if the output should be an aggregate pipeline or query depending on if a straight query is possible. The output JSON object properties can be used to construct a MongoDB query.

## Examples

???+ example "Example `parseSQL` usage for a query"

    Node.js:
    ```js
    const SQLMongoParser=require('@synatic/noql');

    SQLMongoParser.parseSQL(
                "select id from `films` where `id` > 10 limit 10", 
                { database: 'postgresql' /* or 'mysql' */ } );
    ```

    Output:

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


???+ example "Example `parseSQL` usage for an aggregate"

    Node.js:

    ```js
    const SQLMongoParser=require('@synatic/noql');

    SQLMongoParser.parseSQL(
            "select id from `films` where `id` > 10 group by id"),
            { database: 'postgresql' /* or 'mysql' */ });
    ```
    Output:

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

## Using the output with MongoClient

To use the output object,  construct a query with `MongoClient` from the [MongoDB NodeJS Driver](https://www.npmjs.com/package/mongodb)

???+ example "Example usage with `MongoClient`"

    Node.js:
    ```js
    const SQLParser = require('@synatic/noql');
    const {MongoClient} = require('mongodb');

    (async () => {
        try {
            client = new MongoClient('mongodb://127.0.0.1:27017');
            await client.connect();
            const db = client.db('noql-test');

            const parsedSQL = SQLParser.parseSQL(
                    "select id from `films` limit 10",
                     { database: 'postgresql' /* or 'mysql' */ });
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