## Synopsis

`#!js parseSQLtoAST(sqlStatement, options)`

* `sqlStatement` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The SQL statement to parse
* `options` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - Options for the parser
    * `database` [`#!js <string>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Data_structures#string_type) - The database type. Can be `mysql` or `postgresql`. Defaults to `mysql`.
* Returns: [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object)
    * `tableList` [`#!js <string[]>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - A list of tables used in the query
    * `columnList` [`#!js <string[]>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Array) - A list of columns used in the query
    * `ast` [`#!js <object>`](https://developer.mozilla.org/en-US/docs/Web/JavaScript/Reference/Global_Objects/Object) - The AST (abstract syntax tree) of the query


## Description

Parses a SQL statement to an AST (abstract syntax tree)

## Examples

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
