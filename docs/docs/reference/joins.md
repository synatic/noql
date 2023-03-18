## Joins

NoQL supports INNER and OUTER Joins but does not unwind by default and the items are added as an array field.

There are several join hints to support automatically unwinding of joins:

- first
- last
- unwind

Examples:

```sql
--return the first item in the array
select * 
from orders 
inner join 
    `inventory|first` as inventory_docs on sku=item

--take the last item of the array
select * 
from 
    orders 
inner join 
    `inventory|last` as inventory_docs on sku=item

--unwind the array to multiple documents
select * 
from orders 
inner join 
    `inventory|unwind` as inventory_docs on sku=item
```

Alternatively the explicit array functions can be used:

```sql
--return the first item in the array
select *
    ,FIRST_IN_ARRAY(inventory) as inventory_docs 
from orders 
inner join `inventory` on sku=item

--take the last item of the array
select *
    ,LAST_IN_ARRAY(inventory) as inventory_docs 
from orders 
inner join 
    `inventory` on sku=item

--unwind the array to multiple documents
select *
    ,UNWIND(inventory) as inventory_docs 
from orders
inner join 
    `inventory` on sku=item
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
