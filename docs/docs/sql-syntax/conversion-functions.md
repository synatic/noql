# Conversion Functions

!!! tip

    Literal types in `WHERE` clauses using conversion functions are converted     automatically e.g.:
    
    ```sql
    SELECT 
        TO_DATE(date) AS d 
        FROM customers 
    WHERE 
        TO_DATE(date) > TO_DATE('2012-01-01')
    ```
    
    NoQL will automatically convert the date value and result in the following:
    
    ```
    {"$expr":{"$gt":[{"$toDate":"$date"},new Date("2012-01-01T00:00:00.000Z")]}}
    ```

## Supported Conversion Functions

### CONVERT

`CONVERT(expr,to)`

Converts the expression to the specified mongo type: `double`, `string`, `bool`, `date`, `int`, `objectId`, `long`, `decimal`s.

???+ example "Example `CONVERT` usage"

    ```sql 
    SELECT 
        SUBTRACT(CONVERT(‘1’,‘int’),ABS(`Replacement Cost`)) AS d,Title 
    FROM 
        `films`
    ```

### IFNULL

`IFNULL(expr,expr)`

Return the value if the expression is null.

???+ example "Example `IFNULL` usage"

    ```sql
    SELECT 
        IFNULL(id,1) AS `conv` 
    FROM 
        `customers`
    ```

???+ example "Example using select without `FROM` clause for object generation"

    ```sql
    SELECT 
        IFNULL(NULL,(select ‘a’ as val,1 as num)) AS `conv` 
    FROM 
        `customers` 
    ```

### TO_BOOL

`TO_BOOL(expr)`

Convert the expression to a boolean.

???+ example "Example `TO_BOOL` usage"

    ```sql
    SELECT 
        TO_BOOL(‘true’) AS `conv` 
    FROM 
        `customers`
    ```

### TO_DATE

`TO_DATE(expr)`

Convert the expression to a date.

???+ example "Example `TO_DATE` usage"

    ```sql  
    SELECT 
        TO_DATE(‘2021-12-15T00:00:00Z’) AS `conv` 
    FROM 
        `customers`
    ```

### TO_DECIMAL

`TO_DECIMAL(expr)`

Convert the expression to a decimal.

???+ example "Example `TO_DECIMAL` usage"

    ```sql 
    SELECT 
        TO_DECIMAL(‘123.35’) AS `conv` 
    FROM 
        `customers`
    ```

### TO_DOUBLE

`TO_DOUBLE(expr)`

Convert the expression to a double.

???+ example "Example `TO_DOUBLE` usage"

    ```sql  
    SELECT
         TO_DOUBLE(‘123.35’) AS `conv` 
    FROM 
        `customers`
    ```
 
### TO_INT

`TO_INT(expr)`

Convert the expression to an integer.

???+ example "Example `TO_INT` usage"

    ```sql 
    SELECT 
        TO_INT(‘12345’) AS `conv` 
    FROM 
        `customers`
    ```

### TO_LONG

`TO_LONG(expr)`

Convert the expression to a long.

???+ example "Example `TO_LONG` usage"

    ```sql  
    SELECT 
        TO_LONG(‘1234567891’) AS `conv` 
    FROM 
        `customers`
    ```

### TO_STRING

`TO_STRING(expr)`

Convert the expression to a string.

???+ example "Example `TO_STRING` usage"

    ```sql 
    SELECT 
        TO_STRING(`id`) AS `conv` 
    FROM 
        `customers`
    ```
### TO_OBJECTID

`TO_OBJECTID(expr)`

Convert the expression to a mongodb id.

???+ example "Example `TO_OBJECTID` usage"

    ```sql 
    SELECT 
        TO_OBJECTID(`id`) AS `objId` 
    FROM 
        `customers`
    ```

### TYPEOF

`TYPEOF(expr)`

Returns the mongo type of the expression.

???+ example "Example `TYPEOF` usage"

    ```sql 
    SELECT 
        TYPEOF(id) AS `conv` 
    FROM 
        `customers`
    ```


## Casting

NoQL supports cast operations with the following type mappings:

| MySQL Type | Mongo Type |
| ---------- | ---------- |
| VARCHAR    | string     |
| DECIMAL    | decimal    |
| INT        | int        |
| DATETIME   | date       |
| TIME       | date       |
| FLOAT      | number     |
| CHAR       | string     |
| NCHAR      | string     |
| TEXT       | string     |

???+ example "Example casting usage"

    ```sql
    SELECT CAST(1+`id` AS VARCHAR) AS `id` FROM `customers`
    SELECT CAST(abs(-1) AS VARCHAR) AS `id` FROM `customers`
    SELECT CAST('2021-01-01T00:00:00Z' AS DATE) as `id` FROM `customers`
    ```
