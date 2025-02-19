# Objects

### $$ROOT

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

### Merge Fields into Object

Only available in aggregates. Use a `SELECT` without specifying a table to create a new object.

???+ example "Creating a new object"

    ```sql
    SELECT
        (SELECT id,`First Name` AS Name) AS t
    FROM
        customers
    ```

Create a new Object and assign to root

???+ example "Creating a new object and assigning to root"

    ```sql
    SELECT
        (SELECT id,`First Name` AS Name) AS t1
        ,(SELECT id,`Last Name` AS LastName) AS t2
        ,MERGE_OBJECTS(t1,t2) AS `$$ROOT`
    FROM
        customers
    ```

## Supported Object Functions

### PARSE_JSON

`PARSE_JSON(expr)`

Parses the JSON string. Use in conjunction with `ARRAY_TO_OBJECT` to convert an array to an object.

???+ example "Example `PARSE_JSON` usage"

    ```sql
    SELECT
        id,
        ARRAY_TO_OBJECT(PARSE_JSON('[{"k":"val","v":1}]')) AS test
    FROM `customers`;
    ```

### MERGE_OBJECTS

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

### EMPTY_OBJECT

`EMPTY_OBJECT()`

Creates an empty object.

???+ example "Example `EMPTY_OBJECT` usage"

    ```sql
    SELECT
        id,
        EMPTY_OBJECT() AS emptyObj,
        MERGE_OBJECTS(EMPTY_OBJECT(), `Address`) AS filledObj
    FROM `customers`;
    ```

???+ example "Example `EMPTY_OBJECT` usage in a condition"

    ```sql
    SELECT
        id,
        `Address`,
        CASE
            WHEN `Address` = EMPTY_OBJECT() THEN 'No Address'
            ELSE 'Has Address'
        END AS addressStatus
    FROM `customers`;
    ```

### FLATTEN

`FLATTEN(field, prefix)`

Flattens an object into a set of fields. You can optionally add a

???+ example "Example `FLATTEN` usage"

```sql'
   SELECT
       id,
       FLATTEN(`address`,'addr_')
   FROM `customers`;
```

???+ example "Example `FLATTEN` usage with unset"

```sql'
   SELECT
       id,
       FLATTEN(`address`,'addr_',true)
   FROM `customers`;
```

> Will remove the `address` field from the output and will only have the `addr_` prefixed fields.
