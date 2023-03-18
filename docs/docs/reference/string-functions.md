# String Functions

| M-SQL Function                  | Description                                                                                   | Example                                                                        |
| ------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| CONCAT(expr,expr,expr...)       | Concatenates strings.                                                                         | `` select CONCAT(`First Name`,':',`Last Name`) as exprVal from `customers`  `` |
| TRIM(expr,[chars])              | Trims string.                                                                                 | `` select TRIM(`First Name`,'_ -') as exprVal from `customers`  ``             |
| LTRIM(expr,[chars])             | Left trim string.                                                                             | `` select LTRIM(`First Name`,'_ -') as exprVal from `customers`  ``            |
| RTRIM(expr,[chars])             | Right trim string.                                                                            | `` select RTRIM(`First Name`,'_ -') as exprVal from `customers`  ``            |
| SUBSTR(expr,start,length)       | Returns the substring of a string.                                                            | `` select SUBSTR(`First Name`,1,10) as exprVal from `customers`  ``            |
| SUBSTR_BYTES(expr,start,length) | Returns the substring of a string by bytes.                                                   | `` select SUBSTR(`First Name`,1,10) as exprVal from `customers`  ``            |
| REPLACE(expr,find,replace)      | Replaces the first instance of a search string in an input string with a replacement string.. | `` select REPLACE(`First Name`,'a','b') as exprVal from `customers`  ``        |
| REPLACE_ALL(expr,find,replace)  | Replaces all instances of a search string in an input string with a replacement string.       | `` select REPLACE_ALL(`First Name`,'a','b') as exprVal from `customers`  ``    |
| STRLEN(expr)                    | Returns the number of UTF-8 encoded bytes in the specified string.                            | `` select STRLEN(`First Name`) as exprVal from `customers`  ``                 |
| STRLEN_CP(expr)                 | Returns the number of UTF-8 code points in the specified string.                              | `` select STRLEN_CP(`First Name`) as exprVal from `customers`  ``              |
| SPLIT(expr,delimiter)           | Splits the string to an array                                                                 | `` select SPLIT(`First Name`,',') as exprVal from `customers`  ``              |

!!! note

     `+` (str + str) does not work for string concatenation. Use `CONCAT` instead.
