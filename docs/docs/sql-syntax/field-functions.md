# Columns

## Supported Column Functions

### UNSET

`UNSET(fieldName)`

Unsets a field. Useful to remove _id from a result set. Does not require an alias

???+ example "Example `UNSET` FIELD_EXISTS"

    ```sql 
    SELECT 
        UNSET(_id),
        name 
    FROM 
        `films` 
    ```


### FIELD_EXISTS

`FIELD_EXISTS(expr,true/false)`

Check if a field exists: FIELD_EXISTS(expr,true) or doesn't exist: FIELD_EXISTS(expr,false). Can only be used in where clauses and not as an expression.

???+ example "Example `SUM` FIELD_EXISTS"

    ```sql 
    SELECT 
        * 
    FROM 
        `films` 
    WHERE FIELD_EXISTS(`id`,true)
    ```
