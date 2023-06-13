NoQL aims to combine the ease of use and wide understanding of SQL Syntax with the power and flexibility of the MongoDB document model and aggregation pipeline.

To achieve this, NoQL is a SQL to MongoDB interpreter that provides for standard SQL statements with some caveats.

## Key Concepts

NoQL supports parsing of either MySQL or PostgreSQL syntax. 

NoQL follows the MongoDB requirements for fields and values including:

  * Case-sensitive field names
  * Case-sensitive and type exact field values
  * Fields name cannot contain . or start with $

### Documents

NoQL supports the standard MongoDB query and aggregation notation for document navigation including:

  * Dot notation for field traversal including arrays 
  * Operators for object and array field manipulation


### Objects

NoQL provides a shortcut syntax to selecting fields from objects using a sub-select without a where clause. See [`Objects`](/sql-syntax/objects)

### Arrays

NoQL provides many methods for using arrays and functions by using a sub select with a where statement. See [`Arrays`](/sql-syntax/arrays)

### Joins

NoQL currently supports complex inner and left outer joins with hints to simplify required output processing. See [`Joins`](/sql-syntax/joins)

### Aliasing

NoQL requires aliasing (`AS`) for functions and sub queries

???+ example "Example `AS` usage"

    ```sql
    SELECT 
        ABS(-1) AS `absId` 
    FROM 
        `customers`
    ```

Using `AS` on a table requires prefixing

???+ example "Example `AS` usage with a table with prefixing"

    ```sql
    SELECT 
        c.* 
    FROM 
        customers AS c
    ```

Always alias on `JOIN` statements

### Indexing

Complex aggregation pipelines on large sets of data requires an understanding of how MongoDB is utilising indexes, especially on joins. We are working on an index suggestion tool to simplify this process.
