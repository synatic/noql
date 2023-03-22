# `#!js canQuery`

## Synopsis

 `#!js canQuery(sqlStatement, options)`

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to check
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
    * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <boolean>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#boolean_type)

## Description

Checks if a SQL statement can be represented by a straight Mongo query, or if an aggregate query must be used.

## Examples

???+ example "Example `canQuery` usage"
    
    Node.js:

    ```js
    const SQLMongoParser = require('@synatic/noql');

    SQLMongoParser.canQuery('select id from `films`');
    // Returns true

    SQLMongoParser.canQuery('select id from `films` group by id');
    //Returns false
    ```

