# Conversion Functions

!!! tip

    Literal types in `WHERE` clauses using conversion functions are converted     automatically e.g.:
    
    ```sql
    select to_date(date) as d from customers where to_date(date) > to_date('2012-01-01')
    ```
    
    Will automatically convert the date value and result in the following:
    
    ```
    {"$expr":{"$gt":[{"$toDate":"$date"},new Date("2012-01-01T00:00:00.000Z")]}}
    ```

## CONVERT(expr,to)

Converts the expression to a mongo type: ‘double’, ‘string’, ‘bool’, ‘date’, ‘int’, ‘objectId’, ‘long’, ‘decimal’.

```sql 
select SUBTRACT(CONVERT(‘1’,‘int’),ABS(`Replacement Cost`)) as d,Title from `films`
```

## IFNULL(expr,expr)

Return the value if the expression is null.

```sql
select IFNULL(id,1) as `conv` from `customers`
```

Example using select without from for object generation:

```sql
select IFNULL(NULL,(select ‘a’ as val,1 as num)) as `conv` from `customers` 
```

## TO_BOOL(expr)

Convert the expression to a boolean.

```sql
select TO_BOOL(‘true’) as `conv` from `customers`
```

## TO_DATE(expr)

Convert the expression to a date.

```sql  
select TO_DATE(‘2021-12-15T00:00:00Z’) as `conv` from `customers`
```

## TO_DECIMAL(expr)

Convert the expression to a decimal.

```sql 
select TO_DECIMAL(‘123.35’) as `conv` from `customers`
```

## TO_DOUBLE(expr)

Convert the expression to a double.

```sql  
select TO_DOUBLE(‘123.35’) as `conv` from `customers`
```

## TO_INT(expr)

Convert the expression to an integer.

```sql 
select TO_INT(‘12345’) as `conv` from `customers`
```

## TO_LONG(expr)

Convert the expression to a long.

```sql  
select TO_LONG(‘1234567891’) as `conv` from `customers`
```

## TO_STRING(expr)

Convert the expression to a string.

```sql 
select TO_STRING(`id`) as `conv` from `customers`
```

## TYPEOF(expr)

Returns the mongo type of the expression.

```sql 
select TYPEOF(id) as `conv` from `customers`
```


## Casting

NoQL supports cast operations with the following type mappings:

| MySQL Type | Mongo Type |
| ---------- | ---------- |
| VARCHAR    | string     |
| DECIMAL    | decimal    |
| INT        | int        |
| DATETIME   | date       |
| TIME       | date       |
| FLOAT      | number     |
| CHAR       | string     |
| NCHAR      | string     |
| TEXT       | string     |

Example:

```sql
select cast(1+`id` as varchar) as `id` from `customers`
select cast(abs(-1) as varchar) as `id` from `customers`
select cast('2021-01-01T00:00:00Z' as date) as `id` from `customers`
```
