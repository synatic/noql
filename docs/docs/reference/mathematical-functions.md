---
icon: material/alphabet-greek
---

# Mathematical Functions

| M-SQL Function           | Description                                                                                                                                                                 |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| AVG(expr,expr,...)       | Returns the average of a set of values. <br/><br/> ``#!sql select AVG(`Replacement Cost`,`id`) as exprVal from `films`  ``|
| ABS(expr)                | Returns the absolute value of a number.<br/><br/> ``#!sql select ABS(`Replacement Cost`) as exprVal from `films`  ``  |
| ACOS(expr)               | Returns the inverse cosine (arc cosine) of a value.<br/><br/> ``#!sql select ACOS(`Replacement Cost`) as exprVal from `films`  `` |
| ACOSH(expr)              | Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value.<br/><br/> ``#!sql select ACOSH(`Replacement Cost`) as exprVal from `films`  `` |
| ASIN(expr)               | Returns the inverse sine (arc sine) of a value.<br/><br/> ``#!sql select ASIN(`Replacement Cost`) as exprVal from `films`  `` |
| ASINH(expr)              | Returns the inverse hyperbolic sine (hyperbolic arc sine) of a value.<br/><br/> ``#!sql select ASINH(`Replacement Cost`) as exprVal from `films`  `` |
| ATAN(expr)               | Returns the inverse tangent (arc tangent) of a value.<br/><br/> ``#!sql select ATAN(`Replacement Cost`) as exprVal from `films`  `` |
| ATAN2(y,x)               | Returns the inverse tangent (arc tangent) of y / x, where y and x are the first and second values passed to the expression respectively. <br/><br/> ``#!sql select ATAN2(3,4) as exprVal from `films`  ``                       |
| ATANH(expr)              | Returns the inverse hyperbolic tangent (hyperbolic arc tangent) of a value. <br/><br/> ``#!sql select ATANH(`Replacement Cost`) as exprVal from `films`  ``  |
| BINARY_SIZE(expr)        | Returns the byte size of the expression. <br/><br/> ``#!sql select id,BINARY_SIZE(`First Name`) as exprVal from `customers`  `` |
| CEIL(expr)               | Returns the smallest integer greater than or equal to the specified number. <br/><br/> ``#!sql select CEIL(`Replacement Cost`, 1) as exprVal from `films`  `` |
| DEGREES_TO_RADIANS(expr) | Converts an input value measured in degrees to radians. <br/><br/> ``#!sql select DEGREES_TO_RADIANS(300) as exprVal from `films`  `` |
| DIVIDE(expr,expr)        | Divides one number by another and returns the result. <br/><br/> ``#!sql select DIVIDE(`Replacement Cost`,10) as exprVal from `films`  ``|
| EXP(expr)                | Raises Euler's number (i.e. e ) to the specified exponent and returns the result.<br/><br/> ``#!sql select EXP(`Replacement Cost`, 1) as exprVal from `films`  `` |
| FLOOR(expr)              | Returns the largest integer less than or equal to the specified number.<br/><br/> ``#!sql select FLOOR(`Replacement Cost`) as exprVal from `films`  ``        |
| LN(expr)                 | Calculates the natural logarithm ln (i.e log e) of a number and returns the result as a double.<br/><br/> ``#!sql select LN(`id`) as exprVal from `films`  `` |
| LOG(number,base)         | Calculates the log of a number in the specified base and returns the result as a double.<br/><br/> ``#!sql select LOG(`id`,10) as exprVal from `films`  ``|
| LOG10(expr)              | Calculates the log base 10 of a number and returns the result as a double. <br/><br/> ``#!sql select LOG10(`id`) as exprVal from `films`  `` |
| MAX(expr,expr,...)       | Returns the max of a set of numbers.<br/><br/> ``#!sql select MAX(`id`,10) as exprVal from `films`  ``|
| MIN(xpr,expr,...)        | Returns the min of a set of numbers.<br/><br/> ``#!sql select MIN(`id`,10) as exprVal from `films`  ``|
| MOD(expr,expr)           | Divides one number by another and returns the remainder.<br/><br/> ``#!sql select MOD(`id`,10) as exprVal from `films`  `` |
| MULTIPLY(expr,expr,...)  | Multiplies numbers together and returns the result. <br/><br/> ``#!sql select MULTIPLY(`id`,10) as exprVal from `films`  ``|
| POW(expr,exponent)       | Raises a number to the specified exponent and returns the result.<br/><br/> ``#!sql select POW(`id`,10) as exprVal from `films`  ``|
| RADIANS_TO_DEGREES(expr) | Converts an input value measured in radians to degrees.<br/><br/> ``#!sql select RADIANS_TO_DEGREES(0.5) as exprVal from `films`  ``|
| RAND(expr)               | Returns a random float between 0 and 1 each time it is called.<br/><br/> ``#!sql select RAND() as exprVal from `films`  `` |
| ROUND(expr,[places])     | rounds a number to a whole integer or to a specified decimal place.<br/><br/> ``#!sql select ROUND(`Replacement Cost`,1) as exprVal from `films`  `` |
| SIN(expr)                | Returns the sine of a value that is measured in radians.<br/><br/> ``#!sql select SIN(90) as exprVal from `films`  ``|
| SINH(expr)               | Returns the hyperbolic sine of a value that is measured in radians.<br/><br/> ``#!sql select SINH(90) as exprVal from `films`  `` |
| SQRT(expr)               | Calculates the square root of a positive number and returns the result as a double. <br/><br/> ``#!sql select SQRT(`id`) as exprVal from `films`  `` |
| SUBTRACT(expr,expr)      | Subtracts two numbers to return the difference, or two dates to return the difference in milliseconds, or a date and a number in milliseconds to return the resulting date. <br/><br/> ``#!sql select SUBTRACT(10,`id`) as exprVal from `films`  ``                |
| SUM(expr,expr,...)       | Sums the values provided in the expression. <br/><br/> ``#!sql select SUM(`Replacement Cost`,2,id) as s from `films`  `` |
| TAN(expr)                | Returns the tangent of a value that is measured in radians. <br/><br/> ``#!sql select TAN(90) as exprVal from `films`  ``|
| TANH(expr)               | Returns the hyperbolic tangent of a value that is measured in radians. <br/><br/> ``#!sqlselect TANH(90) as exprVal from `films`  ``|
| TRUNC(expr,[places])     | Truncates a number to a whole integer or to a specified decimal place.<br/><br/> ``#!sql select TRUNC(`Replacement Cost`, 1) as exprVal from `films`  `` |
