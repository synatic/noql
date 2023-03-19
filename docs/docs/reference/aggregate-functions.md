# Aggregate Functions

## Supported Aggregate Functions

### SUM

`SUM(field)`

Returns the sum of all values in the given field.

???+ example "Example `SUM` usage"

    ```sql 
    SELECT 
        SUM(`id`) AS aggrVal
        ,`Address.City` AS City 
    FROM 
        `customers` 
    GROUP BY 
        `Address.City` 
    ORDER BY `Address.City`;
    ```

### AVG

`AVG(field)`

Returns the average of all values in the given field.

???+ example "Example `AVG` usage"

    ```sql
    SELECT
        AVG(`id`) AS aggrVal
        ,`Address.City` AS City
    FROM
        `customers`
    GROUP BY 
        `Address.City`
    ORDER BY 
        `Address.City`;
    ```

### MIN

`MIN(field)`

Returns the minimum value in the given field.

???+ example "Example `MIN` usage"

    ```sql
    SELECT
        MIN(`id`) AS aggrVal
        ,`Address.City` AS City
    FROM 
        `customers`
    GROUP BY
        `Address.City`
    ORDER BY
        `Address.City`;
    ```

### MAX

`MAX(field)`

Returns the maximum value in the given field.

???+ example "Example `MAX` usage"

    ```sql 
    SELECT 
        MAX(`id`) AS aggrVal
        ,`Address.City` AS City
    FROM
        `customers`
    GROUP BY
        `Address.City`
    ORDER BY
        `Address.City`;
    ```

### COUNT

`COUNT(field)`

Returns the count of rows in the given group.

???+ example "Example `COUNT` usage"

    ```sql
    SELECT 
        COUNT(*) AS countVal
        ,`Address.City` AS City
    FROM
        `customers`
    GROUP BY
        `Address.City`
    ORDER BY
        `Address.City`;
    ```

## `SUM` - `CASE` Logic

Sum Case logic is supported:

???+ example "Example `SUM` - `CASE` usage"

    ```sql
    SELECT 
        SUM(CASE WHEN `Address.City`='Ueda' THEN 1 ELSE 0 END) AS Ueda
        ,SUM(CASE WHEN `Address.City`='Tete' THEN 1 ELSE 0 END) AS Tete 
    FROM 
        `customers` 
    GROUP BY 
        `xxx`
    ```
