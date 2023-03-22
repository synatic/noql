
## Synopsis

`#!js makeMongoAggregate(sqlStatement, options)`

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
    * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)
    * `type` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The type of query. It will be `aggregate`.
    * `pipeline` [`#!js <array>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - The aggregate pipeline array to use for an `aggregate` type.
    * `collections` [`#!js <array>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - The collections to use for an `aggregate` type.

## Description

Generates a mongo aggregate pipeline components from a SQL statement.

If your SQL statement can be represented by a straight query, use [`makeMongoQuery`](/noql-api-reference/makeMongoQuery.md) instead. If you want NoQL to automatically choose, use [`parseSQL`](/noql-api-reference/parseSQL.md) instead.

## Examples

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

