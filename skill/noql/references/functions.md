# Functions

## Conversion

```sql
CONVERT(expr, 'int')        -- types: double, string, bool, date, int, objectId, long, decimal
TO_INT(expr)
TO_LONG(expr)
TO_DOUBLE(expr)
TO_DECIMAL(expr)
TO_STRING(expr)
TO_BOOL(expr)
TO_DATE(expr)               -- literal strings in WHERE are auto-converted
TO_OBJECTID(expr)
IFNULL(expr, fallback)
TYPEOF(expr)                -- returns MongoDB type name
CAST(expr AS INT)           -- MySQL-style: VARCHAR, DECIMAL, INT, DATETIME, FLOAT, CHAR, TEXT
```

Literal conversion in `WHERE` is automatic:

```sql
SELECT * FROM orders WHERE TO_DATE(orderDate) > TO_DATE('2021-01-01')
```

## String

```sql
CONCAT(expr, ...)                   -- concatenation (do not use +)
TRIM(expr, [chars])
LTRIM(expr, [chars])
RTRIM(expr, [chars])
SUBSTR(expr, start, length)         -- 1-indexed
SUBSTR_BYTES(expr, start, length)
REPLACE(expr, find, replacement)    -- first occurrence
REPLACE_ALL(expr, find, replacement)
STRLEN(expr)                        -- byte length
STRLEN_CP(expr)                     -- code-point length
SPLIT(expr, delimiter)              -- string → array
STRPOS(expr, substr)                -- 1-based; 0 if not found (PostgreSQL)
LOCATE(expr, substr)                -- 1-based; 0 if not found (MySQL)
LEFT(expr, length)
STARTS_WITH(expr, prefix)           -- boolean
WRAP_PARAM(expr, [forceString])
```

Pattern matching in WHERE:

```sql
WHERE name LIKE 'john%'      -- case-insensitive, % is wildcard
WHERE name NOT LIKE 'john%'
```

## Date

```sql
CURRENT_DATE()
DATE_FROM_PARTS(year, month, day, hour, second, minute, ms, tz)
DATE_FROM_ISO_PARTS(isoWeekYear, isoWeek, isoDayOfWeek, hour, second, minute, ms, tz)
DATE_FROM_STRING(expr, format, tz, onError, onNull)
DATE_TO_STRING(expr, format, tz, onNull)
DATE_TO_PARTS(expr, tz, iso8601)
DATE_ADD(date, unit, amount, [tz])       -- alias: DATEADD
DATE_SUBTRACT(date, unit, amount, [tz])  -- alias: DATESUBTRACT
DATE_DIFF(start, end, unit, [tz], [startOfWeek]) -- alias: DATEDIFF
DATE_TRUNC(date, unit)
  -- units: year, quarter, month, week, day, hour, minute, second, millisecond

YEAR(expr) | MONTH(expr) | DAY_OF_MONTH(expr) | DAY_OF_WEEK(expr) | DAY_OF_YEAR(expr)
HOUR(expr) | MINUTE(expr) | SECOND(expr) | MILLISECOND(expr)
WEEK(expr) | ISO_WEEK(expr) | ISO_WEEK_YEAR(expr) | ISO_DAY_OF_WEEK(expr)
EXTRACT(year|month|day|hour|minute|second|milliseconds|week|dow FROM expr)
```

## Mathematical functions and operators

Operators: `+`, `-`, `*`, `/`, `%`

```sql
ABS(expr) | CEIL(expr) | FLOOR(expr) | ROUND(expr, [places]) | TRUNC(expr, [places])
SQRT(expr) | POW(base, exp) | MOD(dividend, divisor)
LN(expr) | LOG(expr, base) | LOG10(expr) | EXP(expr)
SIN(expr) | COS(expr) | TAN(expr)
ASIN(expr) | ACOS(expr) | ATAN(expr) | ATAN2(y, x)
SINH(expr) | COSH(expr) | TANH(expr)
DEGREES_TO_RADIANS(expr) | RADIANS_TO_DEGREES(expr)
RAND()
```

## Window functions

```sql
-- RANK (gaps on ties)
SELECT value, RANK() OVER (ORDER BY value) AS rnk, UNSET(_id)
FROM `data`

-- DENSE_RANK (consecutive ranks, no gaps)
SELECT value, DENSE_RANK() OVER (ORDER BY value) AS dense_rnk, UNSET(_id)
FROM `data`

-- ROW_NUMBER (sequential, no ties)
SELECT value, ROW_NUMBER() OVER (ORDER BY value) AS row_num, UNSET(_id)
FROM `data`
```

## UNION

```sql
SELECT id FROM customers
UNION ALL
SELECT id FROM orders
```

## Pivot and unpivot

```sql
-- PIVOT: rows → columns
SELECT * FROM sales
PIVOT (SUM(amount) FOR category IN ('Electronics', 'Clothing', 'Food'))

-- UNPIVOT: columns → rows
SELECT * FROM quarterly_sales
UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4))
```
