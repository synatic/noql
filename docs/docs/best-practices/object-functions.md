# Objects

Supported methods that perform operations on objects

## PARSE_JSON

`PARSE_JSON(expr)`

Parses the JSON string. Use in conjunction with `ARRAY_TO_OBJECT` to convert an array to an object.

???+ example "Example `PARSE_JSON` usage"

    ```sql
    SELECT 
        id,
        ARRAY_TO_OBJECT(PARSE_JSON('[{"k":"val","v":1}]')) AS test
    FROM `customers`;
    ```
## MERGE_OBJECTS

`MERGE_OBJECTS(expr)`

???+ example "Example `MERGE_OBJECTS` usage"

    ```sql
    SELECT 
        id,
        MERGE_OBJECTS(`Address`,PARSE_JSON('{"val":1}')) AS test
    FROM `customers`;
    ```

???+ example "Example `MERGE_OBJECTS` usage with sub select"

    ```sql
    SELECT 
        id,
        MERGE_OBJECTS(`Address`,(SELECT 1 AS val)) AS test
    FROM `customers`;
    ```

## $$ROOT

Specifying `$$ROOT` as a column alias sets the value to root object but only works with aggregates (unless contained in array sub select). This is useful when you want to return the root object as a column.

???+ example "Example `$$ROOT` usage"

    ```sql
    SELECT 
        t AS `$$ROOT` 
    FROM 
        (
        SELECT 
            id
            ,`First Name`
            ,`Last Name`
            ,LENGTHOFARRAY(Rentals,'id')  AS numRentals 
        FROM customers) 
    AS t
    ```
