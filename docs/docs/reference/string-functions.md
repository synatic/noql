# String Functions

## `#!sql CONCAT(expr,expr,expr...)`

Concatenates strings.
Example:

```sql
select CONCAT(`First Name`,‘:’,`Last Name`) as exprVal from `customers`
```

!!! note

    `+` (str + str) does not work for string concatenation. Use `CONCAT` instead.


## `#!sql TRIM(expr,[chars])`

Trims string.
Example:

```sql
select TRIM(`First Name`,‘_ -‘) as exprVal from `customers`
```

## `#!sql LTRIM(expr,[chars])`

Left trim string.
Example:

```sql
select LTRIM(`First Name`,‘_ -‘) as exprVal from `customers`
```

## `#!sql RTRIM(expr,[chars])`

Right trim string.
Example:

```sql
select RTRIM(`First Name`,‘_ -‘) as exprVal from `customers`
```

## `#!sql SUBSTR(expr,start,length)`

Returns the substring of a string.
Example:

```sql
select SUBSTR(`First Name`,1,10) as exprVal from `customers`
```

## `#!sql SUBSTR_BYTES(expr,start,length)`

Returns the substring of a string by bytes.
Example:

```sql
select SUBSTR(`First Name`,1,10) as exprVal from `customers`
```

## `#!sql REPLACE(expr,find,replace)`

Replaces the first instance of a search string in an input string with a replacement string.
Example:

```sql
select REPLACE(`First Name`,‘a’,‘b’) as exprVal from `customers`
```

## `#!sql REPLACE_ALL(expr,find,replace)`

Replaces all instances of a search string in an input string with a replacement string.
Example:

```sql
select REPLACE_ALL(`First Name`,‘a’,‘b’) as exprVal from `customers`
```

## `#!sql STRLEN(expr)`

Returns the number of UTF-8 encoded bytes in the specified string.
Example:

```sql
select STRLEN(`First Name`) as exprVal from `customers`
```

## `#!sql STRLEN_CP(expr)`

Returns the number of UTF-8 code points in the specified string.
Example:

```sql
select STRLEN_CP(`First Name`) as exprVal from `customers`
```

## `#!sql SPLIT(expr,delimiter)`

Splits the string to an array.
Example:

```sql
select SPLIT(`First Name`,‘,’) as exprVal from `customers`
```