# `#!js M-SQL`

NoQL requires `AS` for functions and sub queries

???+ example "Example `AS` usage"

    ```sql
    SELECT 
        ABS(-1) AS `absId` 
    FROM 
        `customers`
    ```

Using `AS` on a table requires prefixing

???+ example "Example `AS` usage with a table with prefixing"

    ```sql
    SELECT 
        c.* 
    FROM 
        customers AS c
    ```

Always prefix on `JOIN` statements
