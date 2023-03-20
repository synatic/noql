# Order By Clause


An `ORDER BY` clause requires the order field to be part of the `SELECT` statement


???+ success "Correct"

    ```sql
    SELECT 
        `Address.City` AS City, ABS(`id`) AS absId 
    FROM 
        `customers` 
    WHERE 
        `First Name` LIKE 'm%' AND ABS(`id`) > 1 
    ORDER BY absId
    ```

???+ failure "Incorrect"

    ```sql
    SELECT 
        `Address.City` AS City 
    FROM 
        `customers` 
    WHERE 
        `First Name` LIKE 'm%' AND ABS(`id`) > 1 
    ORDER BY ABS(`id`)    
    ```
