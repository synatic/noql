# API Reference

## `#!js parseSQL(sqlStatement, options)`

Parses the given SQL statement to a JSON output object containing Mongo query components. It automatically determines if the output should be an aggregate pipeline or query depending on if a straight query is possible. The output JSON object properties can be used to construct a MongoDB query.

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

## `#!js parseSQLtoAST(sqlStatement, options)`

Parses a SQL statement to an AST (abstract syntax tree)

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
  * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)
  * `tableList` [`#!js <string[]>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - A list of tables used in the query
  * `columnList` [`#!js <string[]>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - A list of columns used in the query
  * `ast` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The AST (abstract syntax tree) of the query

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


## `#!js makeMongoQuery(sqlStatement, options)`

Generates Mongo query components from a SQL statement if possible. Throws an exception if not possible.

Use [`canQuery`](`/noql-api-reference/canQuery(sqlStatement)`) to test if a query can be created, or if an aggregate must be made instead.

If an aggregate must be made, use [`makeMongoAggregate`](`/noql-api-reference/makeMongoAggregate/`) instead. If you want NoQL to automatically choose, use [`parseSQL`](/noql-api-reference/parseSQL.md) instead.

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
  * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)
  * `type` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The type of query. It will be `query`.
  * `collection` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The collection to query for a `query` type.
  * `query` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The query object to use for a `query` type.
  * `projection` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The projection object to use for a `query` type.
  * `limit` [`#!js <number>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#number_type) - The limit to use for a `query` type.

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
        "type": "query",
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


## `#!js makeMongoAggregate(sqlStatement, options)`

Generates a mongo aggregate pipeline components from a SQL statement.

If your SQL statement can be represented by a straight query, use [`makeMongoQuery`](/noql-api-reference/makeMongoQuery.md) instead. If you want NoQL to automatically choose, use [`parseSQL`](/noql-api-reference/parseSQL.md) instead.


* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
  * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)
  * `type` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The type of query. It will be `aggregate`.
  * `pipeline` [`#!js <array>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - The aggregate pipeline array to use for an `aggregate` type.
  * `collections` [`#!js <array>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - The collections to use for an `aggregate` type.

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



## `#!js canQuery(sqlStatement, options)`

Checks if a SQL statement can be represented by a straight Mongo query, or if an aggregate query must be used.

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to check
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
  * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <boolean>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)


???+ example "Example `canQuery` usage"

    Node.js:

    ```js
    const SQLMongoParser = require('@synatic/noql');

    SQLMongoParser.canQuery('select id from `films`');
    // Returns true

    SQLMongoParser.canQuery('select id from `films` group by id');
    //Returns false
    ```

