NoQL supports `INNER` and `OUTER` Joins, but does not unwind by default and the items are added as an array field.

## Join Hints

There are several join hints to support automatically unwinding of joins:

- first
- last
- unwind

Examples:

???+ example "Example `first` join hint"

    ```sql
    --return the first item in the array
    SELECT
         * 
    FROM 
        orders 
    INNER JOIN 
        `inventory|first` AS inventory_docs ON sku=item
    ```

???+ example "Example `last` join hint"

    ```sql
    --take the last item of the array
    SELECT
         * 
    FROM 
        orders 
    INNER JOIN 
        `inventory|last` AS inventory_docs ON sku=item
    ```

???+ example "Example `unwind` join hint"

    ```sql
    --unwind the array to multiple documents
    SELECT 
        * 
    FROM 
        orders 
    INNER JOIN 
        `inventory|unwind` AS inventory_docs ON sku=item
    ```

## `JOIN` Array Functions

Alternatively the explicit array functions `FIRST_IN_ARRAY`, `LAST_IN_ARRAY`, `OBJECT_TO_ARRAY` can be used instead of the join hints:

???+ example "Example `FIRST_IN_ARRAY` usage"

    ```sql
    --return the first item in the array
    SELECT 
        *
        ,FIRST_IN_ARRAY(inventory) AS inventory_docs 
    FROM 
        orders 
    INNER JOIN `inventory` ON sku=item
    ```

???+ example "Example `LAST_IN_ARRAY` usage"

    ```sql
    --take the last item of the array
    SELECT *
        ,LAST_IN_ARRAY(inventory) AS inventory_docs 
    FROM orders 
    INNER JOIN 
        `inventory` ON sku=item
    ```

???+ example "Example `OBJECT_TO_ARRAY` usage"

    ```sql
    --unwind the array to multiple documents
    SELECT 
        *
        ,UNWIND(inventory) AS inventory_docs 
    FROM
        orders
    INNER JOIN 
        `inventory` ON sku=item
    ```

## Caveats

An `IN` sub-select on a `WHERE` clause does not work as a join. Use a join instead.

???+ failure "Example `IN` sub-select. This won't work."

    ```sql
        --Won't Work
        SELECT 
            *, inventory_docs
        FROM 
            orders
        WHERE 
            inventory_docs IN 
            (SELECT 
                * FROM inventory
            WHERE 
                sku= orders.item
            )
    ```
    
???+ success "Using `JOIN` instead of `IN` sub-select"
    
    ```sql
    --use join instead
    SELECT
         *
    FROM
         orders
    INNER JOIN
        inventory inventory_docs ON sku=item
    ```
