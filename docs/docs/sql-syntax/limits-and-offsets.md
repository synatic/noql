# Limit and Offset

NoQL supports MySQL and PostgresQL style limits and offsets that equates to `limit` and `skip` in MongoDB. For example:

???+ example "Using `LIMIT` and `OFFSET`"

    ```sql
    SELECT 
        (SELECT * FROM Rentals) AS t 
    FROM 
        `customers` 
    LIMIT 10 OFFSET 2
    ```
