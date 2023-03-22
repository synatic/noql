# Merge Fields into Object

Only available in aggregates. Use a `SELECT` without specifying a table to create a new object.

## Examples

Create a new Object

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

Using with unwind with joins

???+ example "Using with unwind with joins"

    ```sql
    SELECT 
        MERGE_OBJECTS(
            (SELECT 
                t.CustomerID
                ,t.Name
            )
            ,t.Rental
            ) AS `$$ROOT` 
    FROM 
        (SELECT 
            id AS CustomerID
            ,`First Name` AS Name
            ,UNWIND(Rentals) AS Rental 
        FROM customers) AS t
    ```