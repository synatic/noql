# Arrays

## Array Support

NoQL supports many methods that perform operations on array fields. They can be used as part of select statements and queries.

NoQL uses sub-selects with a FROM array field to query array fields in collections. E.g.

???+ example "Using sub-selects to query array fields"

    ```sql
    SELECT
        (SELECT * FROM Rentals WHERE staffId=2) AS t
    FROM
        `customers`
    ```

Using '$$ROOT' in sub select promotes the field to the root value of the array

???+ example "Using '$$ROOT' in sub select"

    ```sql
    SELECT
        (SELECT filmId AS `$$ROOT` FROM Rentals WHERE staffId=2) AS t
    FROM
        `customers`
    ```

Slicing the array is supported by limit and offset in queries

???+ example "Slicing an array with limit and offset"

    ```sql
    SELECT
        (SELECT * FROM Rentals WHERE staffId=2 LIMIT 10 OFFSET 5) AS t
    FROM
        `customers`
    ```

Sorting Arrays is supported in MongoDB 5.2+ and NoQL

???+ example "Sorting Arrays is supported"

    ```sql
    SELECT id,
        (SELECT * FROM Rentals ORDER BY id DESC) AS totalRentals
    FROM customers
    ```

!!! warning "Aggregation functions are not supported in a sub select"
Aggregation functions are not supported in a sub select. For example, the following won't work
`sql
    --Wont'Work
    SELECT id,
        (SELECT count(*) AS count FROM Rentals) AS totalRentals
    FROM customers
    `

## UNWIND Function

`UNWIND(array_expr)`

NoQL has a high level unwind function that will unwind array fields. For Joins, the unwind join hint should be used.

???+ example "UNWIND in SELECT"

    ```sql
    SELECT
      field1,
      UNWIND(arrFld) as arrFld
    FROM
      test
    ```

???+ example "Complex UNWIND"

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

## Supported Array Functions

### ALL_ELEMENTS_TRUE

`ALL_ELEMENTS_TRUE(array expr)`

Returns true when all elements in the array are true.

???+ example "Example `ALL_ELEMENTS_TRUE` usage"

    ```sql
    SELECT id,
        (CASE WHEN ALL_ELEMENTS_TRUE(Rentals) THEN 'Yes' ELSE 'No' END) AS test
    FROM `customers`;
    ```

### JOIN

`JOIN(array expr, delimiter)`

Joins all values into a string.

???+ example "Example `JOIN` usage"

    ```sql
    SELECT id,
        JOIN((SELECT Name FROM Rentals),',') AS RentalNames
    FROM customers;
    ```

### ANY_ELEMENT_TRUE

`ANY_ELEMENT_TRUE(array expr)`

Returns true when any element in the array is true.

???+ example "Example `ANY_ELEMENT_TRUE` usage"

    ```sql
    SELECT id,
        (CASE WHEN ANY_ELEMENT_TRUE(Rentals) THEN 'Yes' ELSE 'No' END) AS test
    FROM `customers`;
    ```

### ARRAY_ELEM_AT

`ARRAY_ELEM_AT(array expr,position)`

Returns the element of an array at a position.

???+ example "Example `ARRAY_ELEM_AT` usage"

    ```sql
    SELECT id,
        ARRAY_ELEM_AT(Rentals,5) AS test
    FROM `customers`;
    ```

### ARRAY_RANGE

`ARRAY_RANGE(start,stop,step)`

Generates an array of numbers from to with the specified step.

???+ example "Example `ARRAY_RANGE` usage"

    ```sql
    SELECT id,
        ARRAY_RANGE(0,10,2) AS test
    FROM `customers`;
    ```

### ARRAY_TO_OBJECT

`ARRAY_TO_OBJECT(array expr)`

Converts the array to an object.

???+ example "Example `ARRAY_TO_OBJECT` usage"

    ```sql
    SELECT id,
        ARRAY_TO_OBJECT(OBJECT_TO_ARRAY(`Address`)) AS test
    FROM `customers`;
    ```

### CONCAT_ARRAYS

`CONCAT_ARRAYS(array expr,...)`

Concatenate the provided list of arrays.

???+ example "Example `CONCAT_ARRAYS` usage"

    ```sql
    SELECT id,
        CONCAT_ARRAYS((SELECT `Film Title` AS `$$ROOT` FROM `Rentals`), ARRAY_RANGE(0,10,2)) AS test
    FROM `customers`;
    ```

### FIRST_IN_ARRAY

`FIRST_IN_ARRAY(array expr)`

Returns the first element of an array.

???+ example "Example `FIRST_IN_ARRAY` usage"

    ```sql
    SELECT id,
        FIRST_IN_ARRAY(`Rentals`) AS test
    FROM `customers`;
    ```

### INDEXOF_ARRAY

`INDEXOF_ARRAY(array expr,value,[start],[end])`

Returns the index of the value in the array.

???+ example "Example `INDEXOF_ARRAY` usage"

    ```sql
    SELECT id,
        INDEXOF_ARRAY((SELECT `Film Title` AS `$$ROOT` FROM `Rentals`),5) AS test
    FROM `customers`;
    ```

### IS_ARRAY

`IS_ARRAY(array expr)`

Returns true when the field is an array.

???+ example "Example `IS_ARRAY` usage"

    ```sql
    SELECT id,
        (CASE WHEN IS_ARRAY(Rentals) THEN 'Yes' ELSE 'No' END) AS test
    FROM `customers`;
    ```

### LAST_IN_ARRAY

`LAST_IN_ARRAY(array expr)`

Returns the last element of an array.

???+ example "Example `LAST_IN_ARRAY` usage"

    ```sql
    SELECT id,
        LAST_IN_ARRAY(`Rentals`) AS test
    FROM `customers`;
    ```

### OBJECT_TO_ARRAY

`OBJECT_TO_ARRAY(expr)`

Converts the object to an array.

???+ example "Example `OBJECT_TO_ARRAY` usage"

    ```sql
    SELECT id,
        OBJECT_TO_ARRAY(`Address`) AS test
    FROM `customers`;
    ```

### REVERSE_ARRAY

`REVERSE_ARRAY(array expr)`

Reverses the order of an array field.

???+ example "Example `REVERSE_ARRAY` usage"

    ```sql
    SELECT id,
        REVERSE_ARRAY(`Rentals`) AS test
    FROM `customers`;
    ```

### SET_DIFFERENCE

`SET_DIFFERENCE(array expr,...)`

Returns an array as the difference of the provided arrays.

???+ example "Example `SET_DIFFERENCE` usage"

    ```sql
    SELECT id,
        SET_DIFFERENCE((SELECT `Film Title` AS `$$ROOT` FROM `Rentals`), PARSE_JSON('[ 1,2,3,4]')) AS test
    FROM `customers`;
    ```

### SET_EQUALS

`SET_EQUALS(array expr,...)`

Returns true or false if the arrays are equal.

???+ example "Example `SET_EQUALS` usage"

    ```sql
    SELECT id,
        SET_EQUALS((SELECT `Film Title` AS `$$ROOT` FROM `Rentals`), PARSE_JSON('[ 1,2,3,4]')) AS test
    FROM `customers`;
    ```

### SET_INTERSECTION

`SET_INTERSECTION(array expr,...)`

Returns an array as the difference of the provided arrays.

???+ example "Example `SET_INTERSECTION` usage"

    ```sql
    SELECT id,
        SET_INTERSECTION((SELECT filmId AS `$$ROOT` FROM `Rentals`), PARSE_JSON('[ 1,2,3,4]')) AS
    test FROM `customers`;
    ```

### SET_IS_SUBSET

`SET_IS_SUBSET(array expr,...)`

Returns whether an array is a subset of another.

???+ example "Example `SET_IS_SUBSET` usage"

    ```sql
    SELECT id,
        SET_IS_SUBSET((SELECT filmId AS `$$ROOT` FROM `Rentals`), PARSE_JSON(‘[ 1,2,3,4] ‘)) AS test
    FROM `customers`;
    ```

### SET_UNION

`SET_UNION(array expr,...)`

Returns an array as the union of the provided arrays.

???+ example "Example `SET_UNION` usage"

    ```sql
    SELECT id,
        SET_UNION((SELECT filmId AS `$$ROOT` FROM `Rentals`), PARSE_JSON(‘[ 1,2,3,4] ‘)) AS test
    FROM `customers`;
    ```

### SIZE_OF_ARRAY

`SIZE_OF_ARRAY(array expr)`

Returns the size of array.

???+ example "Example `SIZE_OF_ARRAY` usage"

    ```sql
    SELECT id,
        SIZE_OF_ARRAY(`Rentals`) AS test
    FROM `customers`;
    ```

### SUM_ARRAY

`SUM_ARRAY(array expr,[field])`

Sums the values in an array given an array field or sub-select and the field to sum.

???+ example "Example `SUM_ARRAY` usage"

    ```sql
    SELECT
        SUM_ARRAY(`Rentals`, 'staffId') AS totalStaffIds
    FROM `customers`;
    ```

???+ example "Example `SUM_ARRAY` usage with a sub select"

    ```sql
    SELECT id,
        `First Name`,
        `Last Name`,
        SUM_ARRAY(
            (SELECT
                SUM_ARRAY(`Payments`, ‘Amount’) AS total
            FROM `Rentals`), ‘total’) AS t
    FROM customers;
    ```

### AVG_ARRAY

`AVG_ARRAY(array expr,[field])`

Averages the values in an array given an array field or sub-select and the field to average.

???+ example "Example `AVG_ARRAY` usage"

    ```sql
    SELECT
        AVG_ARRAY(`Rentals`, 'staffId') AS avgStaffIds
    FROM `customers`;
    ```

### ZIP_ARRAY

`ZIP_ARRAY(array expr,...)`

Transposes an array of input arrays so that the first element of the output array would be an array containing, the first element of the first input array, the first element of the second input array, etc.

???+ example "Example `ZIP_ARRAY` usage"

    ```sql
    SELECT id,
        ZIP_ARRAY(
            (SELECT `Film Title` AS `$$ROOT` FROM `Rentals`),ARRAY_RANGE(0,10,2)) AS test
    FROM `customers`;
    ```
