## Synopsis

 `#!js makeMongoQuery(sqlStatement, options)`

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
    * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)
    * `type` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The type of query. It will be `query`. 
    * `collection` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The collection to query for a `query` type.
    * `query` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The query object to use for a `query` type.
    * `projection` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The projection object to use for a `query` type.
    * `limit` [`#!js <number>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#number_type) - The limit to use for a `query` type.

## Description

Generates Mongo query components from a SQL statement if possible. Throws an exception if not possible.

Use [`canQuery`](`/noql-api-reference/canQuery(sqlStatement)`) to test if a query can be created, or if an aggregate must be made instead.

If an aggregate must be made, use [`makeMongoAggregate`](`/noql-api-reference/makeMongoAggregate/`) instead. If you want NoQL to automatically choose, use [`parseSQL`](/noql-api-reference/parseSQL.md) instead.

## Examples

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
