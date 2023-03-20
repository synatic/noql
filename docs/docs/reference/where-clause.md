# `WHERE` Clauses

`WHERE` clauses are used to filter the results of a query. They are used in the same way as in SQL.

Functions in `WHERE` clauses require explicit definition and can't use a computed field

???+ success "Correct function and `WHERE` clause usage"

    ```sql
    --Correct
    SELECT 
        `Address.City` AS City
        ,ABS(`id`) AS absId 
    FROM 
        `customers` 
    WHERE 
        `First Name` LIKE 'm%' AND ABS(`id`) > 1 
    ORDER BY 
        absId
    ```

???+ failure "Incorrect function and `WHERE` clause usage"

    ```sql
    --Won't work
    SELECT 
        `Address.City` AS City
        ,ABS(`id`) AS absId 
    FROM 
        `customers` 
    WHERE 
        `First Name` LIKE 'm%' AND absId > 1
    ```

## Selecting on a calculated column by name

Calculated columns in `WHERE` clauses can only be used with aggregates

???+ example "Calculated columns in `WHERE` clauses with aggregates"

    ```sql
    --have to repeat select statement as with sql rules
    SELECT 
        id
        ,Title
        ,Rating
        ,ABS(id) AS absId 
    FROM 
        `films` 
    WHERE ABS(id)=1
    ```
