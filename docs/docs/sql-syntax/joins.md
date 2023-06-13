NoQL supports `INNER` and `OUTER` Joins, but does not unwind by default and the items are added as an array field.

## Join Hints

There are several join hints to simplify document model joining:

- first
- last
- unwind
- optimize

Join hints are added using a pipe `|` character to the join table or alias.

???+ example "Example join hint"

    ```sql
    --return the first item in the array
    SELECT
         * 
    FROM 
        orders 
    INNER JOIN 
        `inventory` AS `inventory_docs|first` 
    ON sku=item
    ```

### first

Returns the first element of the join result as an object on the result

???+ example "Example `first` join hint"

    ```sql
    --return the first item in the array
    SELECT
         * 
    FROM 
        orders 
    INNER JOIN 
        `inventory` AS `inventory_docs|first` ON sku=item
    ```

### last

Returns the last element of the join result as an object on the result

???+ example "Example `last` join hint"

    ```sql
    --take the last item of the array
    SELECT
         * 
    FROM 
        orders 
    INNER JOIN 
        `inventory` AS `inventory_docs|last` ON sku=item
    ```

### unwind

Unwinds the result into multiple records following the result of the join

???+ example "Example `unwind` join hint"

    ```sql
    --unwind the array to multiple documents
    SELECT 
        * 
    FROM 
        orders 
    INNER JOIN 
        `inventory` AS `inventory_docs|unwind` ON sku=item
    ```

### optimize

The optimize hint is a current workaround to limit the result set for the $lookup when working with sub selects.

The $match on sub query joins is applied after the subquery pipeline which can cause performance issues since indexes may not be used. It may be better to put the match before the pipeline to limit the input set depending on the on conditions.

???+ example "Example `optimize` usage"

    ```sql
    SELECT 
        c.*
        ,cn.* 
    FROM 
        customers c 
    INNER JOIN
        (SELECT * FROM `customer-notes` WHERE id>2) `cn|optimize` 
    ON 
        cn.id=c.id
    ```

The on field must be part of the sub query select to be a valid optimization.

## `JOIN` Array Functions

Alternatively the explicit array functions `FIRST_IN_ARRAY`, `LAST_IN_ARRAY`, `UNWIND` can be used instead of the join hints:

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

???+ example "Example `UNWIND` usage"

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
