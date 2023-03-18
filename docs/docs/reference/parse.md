# Parsing SQL 

## `#!js parseSQL(sqlStatement)`

Parses the given SQL statement to a JSON output object. It automatically determines if the output should be an aggregate or query depending on if a straight query is possible. The output JSON object properties can be used to construct a MongoDB query.

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

### Using the output with MongoClient

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


## `#!js canQuery(sqlStatement)`

Returns `true` if a statement can be queried or `false` if an aggregate must be used.

???+ example "Example `canQuery` usage"
    
    Node.js:

    ```js
    const SQLMongoParser = require('@synatic/noql');

    SQLMongoParser.canQuery('select id from `films`');
    // Returns true

    SQLMongoParser.canQuery('select id from `films` group by id');
    //Returns false
    ```


## `#!js makeMongoQuery(sqlStatement)`

Generates a mongo query if possible. Throws an exception if not possible.

Use [`canQuery`](`#canQuery(sqlStatement)`) to test if a query can be created, or if an aggregate must be made instead. 

If an aggregate must be made, use [`makeMongoAggregate`](`#makeMongoAggregate(sqlStatement)`) instead.

???+ example "Example `makeMongoQuery` usage"

    Node.js
    ```js
    const SQLMongoParser=require('@synatic/noql');

    SQLMongoParser.makeMongoQuery(
        "select id from `films` where id > 10 limit 10", 
        { database: 'postgresql' /* or 'mysql' */ });
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
        }
    }
    ```

## `#!js makeMongoAggregate(sqlStatement)`

Generates a mongo aggregate.

???+ example "Example `makeMongoAggregate` usage"

    Node.js

    ```js
    const SQLMongoParser = require('@synatic/noql');

    SQLMongoParser.makeMongoAggregate(
        "select id from `films` group by id", 
        { database: 'postgresql' /* or 'mysql' */ });
    ```
    Output:

    ```json
    {
        "pipeline": [
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


##  `#!js parseSQLtoAST(sqlStatement)`

Parses a SQL statement to an AST (abstract syntax tree)

???+ example "Example `parseSQLtoAST` usage"

    Node.js
    ```js
    const SQLMongoParser = require('@synatic/noql');

    SQLMongoParser.parseSQLtoAST(
        "select id from `films`", 
        { database: 'postgresql' /* or 'mysql' */ });
    ```

    Output:
    ```json
    {
        "tableList": [
            "select::null::films"
        ],
        "columnList": [
            "select::null::id"
        ],
        "ast": {
            "with": null,
            "type": "select",
            "options": null,
            "distinct": null,
            "columns": [
                {
                    "expr": {
                        "type": "column_ref",
                        "table": null,
                        "column": "id"
                    },
                    "as": null
                }
            ],
            "from": [
                {
                    "db": null,
                    "table": "films",
                    "as": null
                }
            ],
            "where": null,
            "groupby": null,
            "having": null,
            "orderby": null,
            "limit": null,
            "for_update": null
        }
    }
    ```
