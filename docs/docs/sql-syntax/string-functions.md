# Strings

!!! note

    `+` (str + str) does not work for string concatenation. Use `CONCAT` instead.

## LIKE

`LIKE '%%'`

Provides string match functionality with support for standard % notation. Case insensitive.

???+ example "Example `LIKE` usage"

    ```sql
    SELECT
        *
    FROM
        `customers`
    WHERE
      name LIKE 'john%';
    ```

## NOT LIKE

`NOT LIKE '%%'`

Provides string non match functionality with support for standard % notation. Case insensitive.

???+ example "Example `NOT LIKE` usage"

    ```sql
    SELECT
        *
    FROM
        `customers`
    WHERE
      name NOT LIKE 'john%';
    ```

## Supported String Functions

### CONCAT

`CONCAT(expr,expr,expr...)`

Concatenates strings.

???+ example "Example `CONCAT` usage"

    ```sql
    SELECT
        CONCAT(`First Name`,‘:’,`Last Name`) AS exprVal
    FROM
        `customers`;
    ```

### JOIN

`JOIN(expr,separator)`

Joins the field that is an array of strings using the separator string.

???+ example "Example `JOIN` usage"

    ```sql
    SELECT
        JOIN(`Addresses`,‘,’) AS fullAddress
    FROM
        `customers`;
    ```

### TRIM

`TRIM(expr,[chars])`

Trims the string

???+ example "Example `TRIM` usage"

    ```sql
    SELECT
        TRIM(`First Name`,‘_ -‘) AS exprVal
    FROM
        `customers`;
    ```

### LTRIM

`LTRIM(expr,[chars])`

Left trims the string

???+ example "Example `LTRIM` usage"

    ```sql
    SELECT
        LTRIM(`First Name`,‘_ -‘) AS exprVal
    FROM
        `customers`;
    ```

### RTRIM

`RTRIM(expr,[chars])`

Right trims the string

???+ example "Example `RTRIM` usage"

    ```sql
    SELECT
        RTRIM(`First Name`,‘_ -‘) AS exprVal
    FROM
        `customers`;
    ```

### SUBSTR

`SUBSTR(expr,start,length)`

Returns the substring of a string.

???+ example "Example `SUBSTR` usage"

    ```sql
    SELECT
        SUBSTR(`First Name`,1,10) AS exprVal
    FROM
        `customers`;
    ```

### SUBSTR_BYTES

`SUBSTR_BYTES(expr,start,length)`

Returns the substring of a string by bytes.

???+ example "Example `SUBSTR_BYTES` usage"

    ```sql
    SELECT
        SUBSTR_BYTES(`First Name`,1,10) AS exprVal
    FROM
        `customers`;
    ```

### REPLACE

`REPLACE(expr,find,replace)`

Replaces the first instance of a search string in an input string with a replacement string.

???+ example "Example `REPLACE` usage"

    ```sql
    SELECT
        REPLACE(`First Name`,‘a’,‘b’) AS exprVal
    FROM
        `customers`;
    ```

### REPLACE_ALL

`REPLACE_ALL(expr,find,replace)`

Replaces all instances of a search string in an input string with a replacement string.

???+ example "Example `REPLACE_ALL` usage"

    ```sql
    SELECT
        REPLACE_ALL(`First Name`,‘a’,‘b’) AS exprVal
    FROM
        `customers`;
    ```

### STRLEN

`STRLEN(expr)`

Returns the number of UTF-8 encoded bytes in the specified string.

???+ example "Example `STRLEN` usage"

    ```sql
    SELECT
        STRLEN(`First Name`)  AS exprVal
    FROM
        `customers`;
    ```

### STRLEN_CP

`STRLEN_CP(expr)`

Returns the number of UTF-8 code points in the specified string.

???+ example "Example `STRLEN_CP` usage"

    ```sql
    SELECT
        STRLEN_CP(`First Name`)  AS exprVal
    FROM
        `customers`;
    ```

### SPLIT

`SPLIT(expr,delimiter)`

Splits the string to an array.

???+ example "Example `SPLIT` usage"

    ```sql
    SELECT
        SPLIT(`First Name`,‘,’) AS exprVal
    FROM
        `customers`;
    ```

### STRPOS

`STRPOS(expr,substr)`

Finds the first index of the substring within the expression. Returns 0 if not found and 1 based index position as per PostgresQL.

???+ example "Example `STRPOS` usage"

    ```sql
    SELECT
        STRPOS(Title,'B') as pos,filmId
    FROM
        films
    WHERE
        STRPOS(Title,'B') > 0;
    ```

### LOCATE

`LOCATE(expr,substr)`

Finds the first index of the substring within the expression. Returns 0 if not found and 1 based index position as per MySQL.

???+ example "Example `LOCATE` usage"

    ```sql
    SELECT
        LOCATE(Title,'B') as pos,filmId
    FROM
        films
    WHERE
        LOCATE(Title,'B') > 0;
    ```

### LEFT

`LEFT(expr, length)`

Returns the leftmost characters from the expression.

???+ example "Example `LEFT` usage"

    ```sql
    SELECT
        LEFT(`First Name`, 3) AS firstThreeChars
    FROM `customers`;
    ```

### STARTS_WITH

`STARTS_WITH(expr, prefix)`

Returns true if the expression starts with the specified prefix.

???+ example "Example `STARTS_WITH` usage"

    ```sql
    SELECT
        id,
        `First Name`,
        STARTS_WITH(`First Name`, 'Jo') AS startsWithJo
    FROM `customers`;
    ```

### WRAP_PARAM

`WRAP_PARAM(expr, [forceString])`

Wraps a parameter, typically used for handling special characters or ensuring correct interpretation of a string.

???+ example "Example `WRAP_PARAM` usage"

    ```sql
    SELECT
        WRAP_PARAM('Hello, "World"!') as wrappedString,
        WRAP_PARAM(42, true) as wrappedNumber
    FROM
        `customers`
    LIMIT 1
    ```

In this example, `WRAP_PARAM` ensures that the string with quotes is properly handled, and when `forceString` is set to true, it forces the number to be treated as a string.
