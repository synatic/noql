---
icon: material/alphabet-greek
---

# Mathematical Functions

## Supported Mathematical Functions

### AVG

`#!sql AVG(expr,expr,...)`

Returns the average of a set of values.


???+ example "Example `#!sql AVG` usage"

    ```sql
    SELECT
        AVG(`Replacement Cost`,`id`) AS exprVal 
    FROM 
        `films` 
    ```

### ABS

`ABS(expr)`

Returns the absolute value of a number.

???+ example "Example `ABS` usage"

    ```sql
    SELECT 
        ABS(`Replacement Cost`) AS exprVal 
    FROM 
        `films` 
    ```

### ACOS

`ACOS(expr)`

Returns the inverse cosine (arc cosine) of a value.

???+ example "Example `ACOS` usage"

    ```sql
    SELECT 
        ACOS(`Replacement Cost`) AS exprVal 
    FROM 
        `films` 
    ```

### ACOSH

`ACOSH(expr)`

Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value.

???+ example "Example `ACOSH` usage"

    ```sql
    SELECT 
        ACOSH(`Replacement Cost`) AS exprVal
    FROM 
        `films`  
    ```

### ASIN

`ASIN(expr)`

Returns the inverse sine (arc sine) of a value.

???+ example "Example `ASIN` usage"

    ```sql
    SELECT 
        ASIN(`Replacement Cost`) AS exprVal 
    FROM 
        `films`  
    ```
### ASINH

`ASINH(expr)`

Returns the inverse hyperbolic sine (hyperbolic arc sine) of a value.

???+ example "Example `ASINH` usage"

    ```sql
    SELECT 
        ASINH(`Replacement Cost`) AS exprVal 
    FROM 
        `films`  
    ```

### ATAN

`ATAN(expr)`

Returns the inverse tangent (arc tangent) of a value.

???+ example "Example `ATAN` usage"

    ```sql
    SELECT 
        ATAN(`Replacement Cost`) AS exprVal 
    FROM 
        `films`
    ```

### ATAN2

`ATAN2(y,x)`

Returns the inverse tangent (arc tangent) of y / x, where y and x are the first and second values passed to the expression respectively.

???+ example "Example `ATAN2` usage"

    ```sql
    SELECT 
        ATAN2(3,4) AS exprVal
    FROM 
        `films`  
    ```

### ATANH

`ATANH(expr)`

Returns the inverse hyperbolic tangent (hyperbolic arc tangent) of a value.

???+ example "Example `ATANH` usage"

    ```sql
    SELECT 
        ATANH(`Replacement Cost`) AS exprVal 
    FROM 
        `films`  
    ```

### BINARY_SIZE

`BINARY_SIZE(expr)`

Returns the byte size of the expression.

???+ example "Example `BINARY_SIZE` usage"

    ```sql
    SELECT
        id
        ,BINARY_SIZE(`First Name`) as exprVal 
    FROM 
        `customers` 
    ```

### CEIL

`CEIL(expr)`

Returns the smallest integer greater than or equal to the specified number.

???+ example "Example `CEIL` usage"

    ```sql
    SELECT 
        CEIL(`Replacement Cost`, 1) AS exprVal 
    FROM 
        `films`  
    ```

### DEGREES_TO_RADIANS

`DEGREES_TO_RADIANS(expr)`

Converts an input value measured in degrees to radians.

???+ example "Example `DEGREES_TO_RADIANS` usage"

    ```sql
    SELECT 
        DEGREES_TO_RADIANS(300) AS exprVal 
    FROM 
        `films` 
    ``` 

### DIVIDE

`DIVIDE(expr,expr)`

Divides one number by another and returns the result.

???+ example "Example `DIVIDE` usage"

    ```sql
    SELECT 
        DIVIDE(`Replacement Cost`,10) AS exprVal 
    FROM 
        `films`  
    ```


### EXP

`EXP(expr)`

Raises Euler's number (i.e. e ) to the specified exponent and returns the result.

???+ example "Example `EXP` usage"

    ```sql
    SELECT 
        EXP(`Replacement Cost`, 1) As exprVal 
    FROM 
        `films` 
    ```

### FLOOR

`FLOOR(expr)`

Returns the largest integer less than or equal to the specified number.

???+ example "Example `FLOOR` usage"

    ```sql
    SELECT 
        FLOOR(`Replacement Cost`) AS exprVal 
    FROM 
        `films` 
    ```

### LN

`LN(expr)`

Calculates the natural logarithm ln (i.e log e) of a number and returns the result as a double.

???+ example "Example `LN` usage"

    ```sql
    SELECT 
        LN(`Replacement Cost`) AS exprVal 
    FROM 
        `films` 
    ```

### LOG

`LOG(number,base)`

Calculates the log of a number in the specified base and returns the result as a double.

???+ example "Example `LOG` usage"

    ```sql
    SELECT 
        LOG(`Replacement Cost`,10) AS exprVal 
    FROM 
        `films` 
    ```

### LOG10

`LOG10(expr)`

Calculates the log base 10 of a number and returns the result as a double.

???+ example "Example `LOG10` usage"

    ```sql
    SELECT 
        LOG10(`Replacement Cost`) AS exprVal 
    FROM 
        `films`  
    ```
### MAX

`MAX(expr,expr,...)`

Returns the max of a set of numbers.

???+ example "Example `MAX` usage"

    ```sql
    SELECT 
        MAX(`Replacement Cost`,10) AS exprVal 
    FROM 
        `films` 
    ```

### MIN

`MIN(xpr,expr,...)`

Returns the min of a set of numbers.

???+ example "Example `MIN` usage"

    ```sql
    SELECT 
        MIN(`Replacement Cost`,10) AS exprVal
    FROM 
        `films` 
    ``` 

### MOD

`MOD(expr,expr)`

Divides one number by another and returns the remainder.

???+ example "Example `MOD` usage"
    ```sql
    SELECT 
        MOD(`Replacement Cost`,10) AS exprVal 
    FROM 
        `films`
    ```

### MULTIPLY

`MULTIPLY(expr,expr,...)`

Multiplies numbers together and returns the result.

???+ example "Example `MULTIPLY` usage"

    ```sql
    SELECT 
        MULTIPLY(`Replacement Cost`,10) AS exprVal
    FROM
        `films`
    ```

### POW

`POW(expr,exponent)`

Raises a number to the specified exponent and returns the result.

???+ example "Example `POW` usage"
    ```sql
    SELECT 
        POW(`Replacement Cost`,10) AS exprVal 
    FROM 
        `films` 
    ```

### RADIANS_TO_DEGREES

`RADIANS_TO_DEGREES(expr)`

Converts an input value measured in radians to degrees.

???+ example "Example `RADIANS_TO_DEGREES` usage"

    ```sql
    SELECT 
        RADIANS_TO_DEGREES(0.5) AS exprVal 
    FROM 
        `films` 
    ``` 
### RAND

`RAND()`

Returns a random float between 0 and 1 each time it is called.

???+ example "Example `RAND` usage"

    ```sql
    SELECT 
        RAND() AS exprVal 
    FROM 
        `films`  
    ```

### ROUND

`ROUND(expr,[places])`

Rounds a number to a whole integer or to a specified decimal place.

???+ example "Example `ROUND` usage"

    ```sql
    SELECT 
        ROUND(`Replacement Cost`,1) AS exprVal
    FROM 
        `films`  
    ```

### SIN

`SIN(expr)`

Returns the sine of a value that is measured in radians.

???+ example "Example `SIN` usage"

    ```sql
    SELECT 
        SIN(90) AS exprVal 
    FROM 
        `films`  
    ```

### SINH

`SINH(expr)`

Returns the hyperbolic sine of a value that is measured in radians.

???+ example "Example `SINH` usage"

    ```sql
    SELECT 
        SINH(90) AS exprVal 
    FROM 
        `films`  
    ```

### SQRT

`SQRT(expr)`

Calculates the square root of a positive number and returns the result as a double.

???+ example "Example `SQRT` usage"

    ```sql
    SELECT 
        SQRT(`id`) AS exprVal 
    FROM 
        `films`  
    ```

### SUBTRACT

`SUBTRACT(expr,expr)`

Subtracts two numbers to return the difference, or two dates to return the difference in milliseconds, or a date and a number in milliseconds to return the resulting date.

???+ example "Example `SUBTRACT` usage"

    ```sql
    SELECT 
        SUBTRACT(10,`id`) AS exprVal 
    FROM 
        `films` 
    ```

### SUM

`SUM(expr,expr,...)`

Sums the values provided in the expression.

???+ example "Example `SUM` usage"

    ```sql
    SELECT 
        SUM(`Replacement Cost`,2,id) AS s 
    FROM 
        `films`  
    ```

### TAN

`TAN(expr)`

Returns the tangent of a value that is measured in radians.

???+ example "Example `TAN` usage"

    ```sql
    SELECT 
        TAN(90) AS exprVal 
    FROM 
        `films`  
    ```

### TANH

`TANH(expr)`

Returns the hyperbolic tangent of a value that is measured in radians.

???+ example "Example `TANH` usage"

    ```sql
    SELECT 
        TANH(90) AS exprVal 
    FROM 
        `films`  
    ```

### TRUNC

`TRUNC(expr,[places])`

Truncates a number to a whole integer or to a specified decimal place.

???+ example "Example `TRUNC` usage"

    ```sql
    SELECT 
        TRUNC(`Replacement Cost`, 1) AS exprVal 
    FROM 
        `films`
    ```
