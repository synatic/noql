# Conversion Functions

| M-SQL Function                                | Description &<br/> Example                                                                                        |
| --------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| `#!sql CONVERT(expr,to)`                      | Converts the expression to a mongo type: 'double', 'string', 'bool', 'date', 'int', 'objectId', 'long', 'decimal' <br/><br/> ```#!sql select SUBTRACT(CONVERT('1','int'),ABS(`Replacement Cost`)) as d,Title from `films` ```                        |
| `#!sql TYPEOF(expr)`                          | returns the mongo type of the expression <br/><br/> ```#!sql  select TYPEOF(id) as `conv` from `customers` ``` |
| `#!sql TO_DATE(expr)`                         | Convert the expression to a date <br/><br/> ```#!sql  select TO_DATE('2021-12-15T00:00:00Z') as `conv` from `customers` ``` |
| `#!sql TO_STRING(expr)`                       | Convert the expression to a date <br/><br/> ```#!sql  select TO_STRING(`id`) as `conv` from `customers` ``` |
| `#!sql TO_INT(expr)`                          | Convert the expression to a date <br/><br/> ```#!sql  select TO_INT('12345') as `conv` from `customers` ```        |
| `#!sql TO_LONG(expr)`                         | Convert the expression to a date <br/><br/> ```#!sql  select TO_LONG('1234567891') as `conv` from `customers` ```  |
| `#!sql TO_BOOL(expr)`                         | Convert the expression to a date <br/><br/> ```#!sql  select TO_BOOLE('true') as `conv` from `customers` ```       |
| `#!sql TO_DECIMAL(expr)`                      | Convert the expression to a date <br/><br/> ```#!sql  select TO_DECIMAL('123.35') as `conv` from `customers` ```   |
| `#!sql TO_DOUBLE(expr)`                       | Convert the expression to a date <br/><br/> ```#!sql  select TO_DOUBLE('123.35') as `conv` from `customers` ```    |
| `#!sql IFNULL(expr,expr)`                     | Return the value if the expression is null <br/><br/> ```#!sql  select IFNULL(id,1) as `conv` from `customers` ``` |
|                                               | Example using select without from for object generation <br/><br/> ```#!sql select IFNULL(NULL,(select 'a' as val,1 as num)) as `conv` from `customers` <br> {"val":"a","num":1}``` |

Literal types in where statements using conversion functions are converted automatically e.g.:

```sql
select to_date(date) as d from customers where to_date(date) > to_date('2012-01-01')
```

Will automatically convert the date value and result in the following:

```
{"$expr":{"$gt":[{"$toDate":"$date"},new Date("2012-01-01T00:00:00.000Z")]}}
```

## Casting

Supports cast operations with the following type mappings:

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

```sql
select cast(1+`id` as varchar) as `id` from `customers`
select cast(abs(-1) as varchar) as `id` from `customers`
select cast('2021-01-01T00:00:00Z' as date) as `id` from `customers`
```
