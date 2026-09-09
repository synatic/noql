# NoQL Skill Reference

NoQL converts SQL statements into MongoDB queries and aggregation pipelines. Use this reference to write correct NoQL statements.

<div style="margin: 1.5rem 0;">
  <button id="noql-download-btn" onclick="downloadSkillFile()" style="background:#3e8db9;color:#fff;border:none;padding:0.6rem 1.4rem;border-radius:4px;font-size:1rem;cursor:pointer;font-weight:600;letter-spacing:0.02em;">
    ⬇ Download skill.md
  </button>
</div>

<script>
function downloadSkillFile() {
    fetch('assets/noql-skill.txt')
        .then(function(r) { return r.blob(); })
        .then(function(blob) {
            var url = URL.createObjectURL(blob);
            var a = document.createElement('a');
            a.href = url;
            a.download = 'noql-skill.md';
            document.body.appendChild(a);
            a.click();
            document.body.removeChild(a);
            URL.revokeObjectURL(url);
        });
}
</script>

---

## Installation

```bash
npm i @synatic/noql --save
```

## API Usage

```js
const SQLParser = require('@synatic/noql');
const { MongoClient } = require('mongodb');

const parsedSQL = SQLParser.parseSQL(
    "SELECT id FROM `films` LIMIT 10",
    { database: 'postgresql' } // or 'mysql'
);

if (parsedSQL.type === 'query') {
    await db.collection(parsedSQL.collection)
        .find(parsedSQL.query || {}, parsedSQL.projection || {})
        .limit(parsedSQL.limit || 50)
        .toArray();
} else if (parsedSQL.type === 'aggregate') {
    await db.collection(parsedSQL.collections[0])
        .aggregate(parsedSQL.pipeline)
        .toArray();
}
```

---

## Quoting Conventions

| Situation | Quote character | Example |
|---|---|---|
| Field names with spaces or special chars | backtick or single quote | `` `First Name` `` or `'First Name'` |
| `$` Mongo paths in array sub-selects | backtick | `` `$favouriteFilms` ``, `` `$$ROOT.filmId` `` |
| Collection names with hyphens | backtick or single/double quote | `` `customer-notes` `` |
| String literals | single quote | `'hello'` |
| Aliased table in FROM | backtick or no quotes | `` FROM customers AS c `` |

MongoDB field names are **case-sensitive**. Use dot notation to traverse nested documents: `` `Address.City` ``.

---

## Aliasing Rules

- Functions **must** be aliased: `ABS(id) AS absId`
- Subqueries **must** be aliased: `(SELECT ...) AS t`
- Table aliases enable prefixed field access: `FROM customers AS c` → `c.id`

```sql
-- Required alias on function
SELECT ABS(-1) AS absId FROM `customers`

-- Required alias on subquery
SELECT t.id FROM (SELECT id FROM `films`) AS t
```

---

## Core Syntax

### SELECT / FROM

```sql
SELECT id, `First Name`, `Address.City` FROM customers

-- Wildcard
SELECT * FROM customers

-- Remove _id from results
SELECT *, UNSET(_id) FROM customers

-- Multiple UNSET fields
SELECT *, UNSET(_id, `Address`) FROM customers
```

### WHERE

```sql
SELECT * FROM customers WHERE id = 1
SELECT * FROM customers WHERE `First Name` LIKE 'm%'
SELECT * FROM customers WHERE id > 1 AND id < 10
SELECT * FROM customers WHERE FIELD_EXISTS(id, true)
SELECT * FROM customers WHERE FIELD_EXISTS(phone, false)

-- Functions in WHERE must be repeated (cannot reference alias)
SELECT ABS(id) AS absId FROM customers WHERE ABS(id) > 1
```

### GROUP BY / Aggregates

```sql
SELECT SUM(id) AS total, `Address.City` AS City
FROM customers GROUP BY `Address.City` ORDER BY City

SELECT COUNT(*) AS cnt, `Address.City` AS City
FROM customers GROUP BY `Address.City`

SELECT COUNT(DISTINCT `Address.Town`) AS uniq, `Address.City` AS City
FROM customers GROUP BY `Address.City`

SELECT AVG(id) AS avg, MIN(id) AS mn, MAX(id) AS mx,
       `Address.City` AS City
FROM customers GROUP BY `Address.City`

-- Return first/last N records per group as array
SELECT `Address.City` AS City, FIRSTN(10) AS sample
FROM customers GROUP BY `Address.City`

-- SUM with CASE
SELECT SUM(CASE WHEN `Address.City`='Ueda' THEN 1 ELSE 0 END) AS Ueda
FROM customers
```

### ORDER BY / LIMIT / OFFSET

```sql
SELECT * FROM customers ORDER BY `Last Name` ASC, id DESC
SELECT * FROM customers LIMIT 10
SELECT * FROM customers LIMIT 10 OFFSET 5
```

---

## Joins

By default, joins produce an **array field** on each result document. Use **join hints** to control output shape.

### Join Types

```sql
-- LEFT JOIN: all left records, matched right records (or empty array)
SELECT *, UNSET(_id) FROM orders AS o
LEFT JOIN `inventory` AS i ON o.item = i.sku

-- INNER JOIN: only records with a match on both sides
SELECT *, UNSET(_id) FROM orders AS o
INNER JOIN `inventory` AS i ON o.item = i.sku
```

### Join Hints

Append hints to the **table name or alias** using `|`. Multiple hints can be chained.

```sql
-- |first  → collapse array to first match (object, not array)
SELECT * FROM customers c
INNER JOIN `customer-notes` AS `cn|first` ON cn.id = c.id

-- Also valid on the alias side:
LEFT OUTER JOIN 'customer-notes' 'cn|first' ON cn.id = to_int(c.id)

-- |last   → collapse array to last match
SELECT * FROM orders o
INNER JOIN `inventory` AS `inv|last` ON inv.sku = o.item

-- |unwind → expand matches into multiple result rows
SELECT * FROM orders o
INNER JOIN `inventory` AS `inv|unwind` ON inv.sku = o.item

-- |optimize → push $match to beginning of sub-pipeline (better index use)
-- Use with sub-select joins
SELECT c.*, cn.* FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|optimize`
ON cn.id = c.id

-- Chain hints: |optimize|first
SELECT c.*, cn.* FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|optimize|first`
ON cn.id = c.id
```

### JOIN Array Functions (alternative syntax in SELECT)

```sql
SELECT *, FIRST_IN_ARRAY(inventory) AS inv FROM orders
INNER JOIN `inventory` ON sku = item

SELECT *, LAST_IN_ARRAY(inventory) AS inv FROM orders
INNER JOIN `inventory` ON sku = item

SELECT *, UNWIND(inventory) AS inv FROM orders
INNER JOIN `inventory` ON sku = item
```

### Multi-table (N-level) Joins

```sql
SELECT o.id AS orderId, i.id AS inventoryId, c.id AS customerId,
       UNSET(_id)
FROM orders AS o
INNER JOIN `inventory|unwind` AS i ON o.item = i.sku
INNER JOIN `customers|unwind` AS c ON o.customerId = c.id
```

### Subquery Joins

```sql
SELECT c.*, cn.*
FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|first`
ON cn.id = c.id
```

### Join Caveats

```sql
-- IN sub-select does NOT work as a join substitute — use JOIN instead
-- WRONG:
SELECT * FROM orders WHERE inventory IN (SELECT * FROM inventory WHERE sku=orders.item)
-- CORRECT:
SELECT * FROM orders INNER JOIN inventory inv ON inv.sku = orders.item
```

---

## Arrays

Use sub-selects referencing an array field name as the FROM target.

```sql
-- Filter array elements
SELECT (SELECT * FROM Rentals WHERE staffId = 2) AS filtered FROM customers

-- Project specific fields from array elements
SELECT (SELECT filmId, staffId FROM Rentals WHERE staffId = 2) AS t FROM customers

-- Promote single field to root array values with $$ROOT as an alias
SELECT (SELECT filmId AS `$$ROOT` FROM Rentals WHERE staffId = 2) AS filmIds FROM customers

-- Slice array
SELECT (SELECT * FROM Rentals LIMIT 10 OFFSET 5) AS page FROM customers

-- Sort array (MongoDB 5.2+)
SELECT id, (SELECT * FROM Rentals ORDER BY id DESC) AS sorted FROM customers

-- Aggregate functions NOT supported inside array sub-selects
```

### Field references in array sub-selects

Array sub-selects compile to `$map` / `$filter`. Bare names bind to the current element (`$$this`). A backtick-quoted `$path` is a parent/root document field.

| NoQL | Meaning | MongoDB |
|---|---|---|
| `filmId` | Current array element | `$$this.filmId` |
| `` `$favouriteFilms` `` | Parent document | `$favouriteFilms` |
| `` `$$ROOT.favouriteFilms` `` | Explicit root document | `$$ROOT.favouriteFilms` |
| `` `$this.filmId` `` / `` `$$this.filmId` `` | Legacy element workaround | `$$this.filmId` |

```sql
-- Parent array + element field: $favouriteFilms stays on the customer
SELECT (
    SELECT *
    FROM Rentals
    WHERE INDEXOF_ARRAY(`$favouriteFilms`, filmId) >= 0
) AS favourites
FROM customers

-- Equivalent explicit root path
SELECT (
    SELECT *
    FROM Rentals
    WHERE INDEXOF_ARRAY(`$$ROOT.favouriteFilms`, filmId) >= 0
) AS favourites
FROM customers
```

Prefer bare names for element fields. `$this` / `$$this` still work for backwards compatibility but should not be used in new queries. `$field` / `$$ROOT.field` always mean the query's root document, not an intermediate nested `$map` parent. Quote `$` paths with backticks. Use `INDEXOF_ARRAY(...) >= 0` (it returns `0` for the first match).

### Array Functions

| Function | Signature | Description |
|---|---|---|
| `ARRAY_ELEM_AT` | `(array, pos)` | Element at position |
| `ARRAY_RANGE` | `(start, stop, step)` | Generate number array |
| `ARRAY_TO_OBJECT` | `(array)` | Array → object |
| `AVG_ARRAY` | `(array, field)` | Average field in array |
| `CONCAT_ARRAYS` | `(arr, ...)` | Concatenate arrays |
| `FIRST_IN_ARRAY` | `(array)` | First element |
| `INDEXOF_ARRAY` | `(array, val, [start], [end])` | Index of value |
| `IS_ARRAY` | `(expr)` | Boolean: is array? |
| `JOIN` | `(array, delimiter)` | Array → delimited string |
| `LAST_IN_ARRAY` | `(array)` | Last element |
| `OBJECT_TO_ARRAY` | `(expr)` | Object → array |
| `REVERSE_ARRAY` | `(array)` | Reverse order |
| `SET_DIFFERENCE` | `(arr, ...)` | Set difference |
| `SET_EQUALS` | `(arr, ...)` | Arrays equal? |
| `SET_INTERSECTION` | `(arr, ...)` | Set intersection |
| `SET_IS_SUBSET` | `(arr, ...)` | Is subset? |
| `SET_UNION` | `(arr, ...)` | Set union |
| `SIZE_OF_ARRAY` | `(array)` | Array length |
| `SUM_ARRAY` | `(array, field)` | Sum field in array |
| `UNWIND` | `(array)` | Unwind to multiple rows |
| `ZIP_ARRAY` | `(arr, ...)` | Transpose arrays |
| `ALL_ELEMENTS_TRUE` | `(array)` | All elements truthy? |
| `ANY_ELEMENT_TRUE` | `(array)` | Any element truthy? |

---

## Objects

```sql
-- $$ROOT: promote a sub-expression to the document root
SELECT t AS `$$ROOT`
FROM (SELECT id, `First Name`, `Last Name` FROM customers) AS t

-- Create an inline object (SELECT without FROM)
SELECT (SELECT id, `First Name` AS Name) AS obj FROM customers

-- MERGE_OBJECTS: merge two objects
SELECT id, MERGE_OBJECTS(`Address`, PARSE_JSON('{"val":1}')) AS merged FROM customers

-- FLATTEN: spread object fields to root level with optional prefix
SELECT id, FLATTEN(`Address`, 'addr_') FROM customers
-- With third arg true, removes the original field:
SELECT id, FLATTEN(`Address`, 'addr_', true) FROM customers

-- PARSE_JSON: parse a JSON string
SELECT ARRAY_TO_OBJECT(PARSE_JSON('[{"k":"key","v":1}]')) AS obj FROM customers

-- EMPTY_OBJECT
SELECT EMPTY_OBJECT() AS empty FROM customers
```

---

## Conversion Functions

```sql
CONVERT(expr, 'int')       -- double, string, bool, date, int, objectId, long, decimal
TO_INT(expr)
TO_LONG(expr)
TO_DOUBLE(expr)
TO_DECIMAL(expr)
TO_STRING(expr)
TO_BOOL(expr)
TO_DATE(expr)              -- auto-converts literal strings in WHERE
TO_OBJECTID(expr)
IFNULL(expr, fallback)
TYPEOF(expr)               -- returns MongoDB type name
CAST(expr AS INT)          -- MySQL-style; VARCHAR, DECIMAL, INT, DATETIME, FLOAT, CHAR, TEXT
```

Literal type conversion in `WHERE` is automatic:
```sql
-- TO_DATE on a literal is auto-resolved
SELECT * FROM orders WHERE TO_DATE(orderDate) > TO_DATE('2021-01-01')
```

---

## String Functions

```sql
CONCAT(expr, ...)                  -- string concatenation (use instead of +)
TRIM(expr, [chars])                -- strip chars from both ends
LTRIM(expr, [chars])               -- strip from left
RTRIM(expr, [chars])               -- strip from right
SUBSTR(expr, start, length)        -- substring (1-indexed)
SUBSTR_BYTES(expr, start, length)  -- substring by byte position
REPLACE(expr, find, replacement)   -- replace first occurrence
REPLACE_ALL(expr, find, replacement)
STRLEN(expr)                       -- byte length
STRLEN_CP(expr)                    -- code-point length
SPLIT(expr, delimiter)             -- string → array
STRPOS(expr, substr)               -- 1-based index; 0 if not found (PostgreSQL style)
LOCATE(expr, substr)               -- 1-based index; 0 if not found (MySQL style)
LEFT(expr, length)                 -- leftmost N chars
STARTS_WITH(expr, prefix)          -- boolean
WRAP_PARAM(expr, [forceString])    -- wrap/escape a parameter
```

Pattern matching:
```sql
SELECT * FROM customers WHERE name LIKE 'john%'     -- case-insensitive
SELECT * FROM customers WHERE name NOT LIKE 'john%'
```

---

## Date Functions

```sql
CURRENT_DATE()
DATE_FROM_PARTS(year, month, day, hour, second, minute, ms, tz)
DATE_FROM_ISO_PARTS(isoWeekYear, isoWeek, isoDayOfWeek, hour, second, minute, ms, tz)
DATE_FROM_STRING(expr, format, tz, onError, onNull)
DATE_TO_STRING(expr, format, tz, onNull)
DATE_TO_PARTS(expr, tz, iso8601)
DATE_ADD(date, unit, amount, [tz])      -- alias: DATEADD
DATE_SUBTRACT(date, unit, amount, [tz]) -- alias: DATESUBTRACT
DATE_DIFF(start, end, unit, [tz], [startOfWeek]) -- alias: DATEDIFF
DATE_TRUNC(date, unit)                  -- units: year, quarter, month, week, day, hour, minute, second, millisecond

YEAR(expr) | MONTH(expr) | DAY_OF_MONTH(expr) | DAY_OF_WEEK(expr) | DAY_OF_YEAR(expr)
HOUR(expr) | MINUTE(expr) | SECOND(expr) | MILLISECOND(expr)
WEEK(expr) | ISO_WEEK(expr) | ISO_WEEK_YEAR(expr) | ISO_DAY_OF_WEEK(expr)
EXTRACT(year|month|day|hour|minute|second|milliseconds|week|dow FROM expr)
```

---

## Mathematical Functions and Operators

Operators: `+`, `-`, `*`, `/`, `%` (modulo)

```sql
ABS(expr)
CEIL(expr)
FLOOR(expr)
ROUND(expr, [places])
SQRT(expr)
POW(base, exp)
MOD(dividend, divisor)
LN(expr)
LOG(expr, base)
LOG10(expr)
EXP(expr)
TRUNC(expr, [places])
SIN(expr) | COS(expr) | TAN(expr)
ASIN(expr) | ACOS(expr) | ATAN(expr) | ATAN2(y, x)
SINH(expr) | COSH(expr) | TANH(expr)
DEGREES_TO_RADIANS(expr) | RADIANS_TO_DEGREES(expr)
RAND()
```

---

## Window Functions

```sql
-- RANK (gaps on ties)
SELECT value,
       RANK() OVER (ORDER BY value) AS rank_number,
       UNSET(_id)
FROM `function-test-data`

-- DENSE_RANK (no gaps on ties)
SELECT value,
       DENSE_RANK() OVER (ORDER BY value) AS dense_rank,
       UNSET(_id)
FROM `function-test-data`

-- ROW_NUMBER (sequential, no ties)
SELECT value,
       ROW_NUMBER() OVER (ORDER BY value) AS row_num,
       UNSET(_id)
FROM `function-test-data`
```

---

## UNION

```sql
SELECT id FROM customers
UNION ALL
SELECT id FROM orders
```

---

## Pivot and Unpivot

```sql
-- PIVOT: rows → columns
SELECT * FROM sales
PIVOT (SUM(amount) FOR category IN ('Electronics', 'Clothing', 'Food'))

-- UNPIVOT: columns → rows
SELECT * FROM quarterly_sales
UNPIVOT (amount FOR quarter IN (Q1, Q2, Q3, Q4))
```

---

## Subqueries

```sql
-- FROM subquery (must alias)
SELECT t.id, t.total FROM (
    SELECT id, SUM(price) AS total FROM orders GROUP BY id
) AS t

-- Correlated subquery in SELECT (for array operations)
SELECT id, (SELECT * FROM Rentals WHERE staffId = 2) AS filtered
FROM customers

-- FULL OUTER JOIN
SELECT c.customerName, o.orderId, UNSET(_id)
FROM "foj-customers" c
FULL OUTER JOIN "foj-orders" o ON c.customerId = o.customerId
ORDER BY c.customerName ASC
```

---

## Key Caveats

1. **Case-sensitive fields and values** — `First Name` ≠ `first name`
2. **Stored field names cannot start with `$` or contain `.`**. Dot notation is for traversal. In array sub-selects, a backtick-quoted `$path` is a parent-document Mongo path, not a stored field name.
3. **Functions in WHERE must be repeated** — cannot reference a computed alias
4. **`+` is not string concatenation** — use `CONCAT(a, b)`
5. **IN subselect does not work** — use JOIN instead
6. **Aggregate functions not allowed inside array sub-selects**
7. **Always alias functions and subqueries** — `ABS(id) AS absId`, `(SELECT ...) AS t`
8. **`|optimize` hint**: only effective when the ON field is part of the sub-select
9. **`|first` and `|last` hints**: for complex ON conditions (functions, AND) or subquery joins, a `$limit: 1` / scan-all is applied inside the lookup pipeline; simple `col = col` joins use MongoDB's native `localField`/`foreignField`
10. **Sorting arrays** requires MongoDB 5.2+

---

## Quick Reference

```sql
-- Simple query
SELECT id, name, UNSET(_id) FROM customers WHERE id > 5 LIMIT 10

-- Nested field
SELECT `Address.City`, `Address.Country` FROM customers

-- Aggregate
SELECT COUNT(*) AS cnt, `Address.City` FROM customers GROUP BY `Address.City`

-- Left join with first hint
SELECT c.id, cn.notes AS note, UNSET(_id)
FROM customers c
LEFT OUTER JOIN 'customer-notes' 'cn|first' ON cn.id = TO_INT(c.id)

-- Inner join with unwind
SELECT o.id, i.sku, i.instock, UNSET(_id)
FROM orders o
INNER JOIN `inventory|unwind` i ON i.sku = o.item

-- Array sub-select (bare names = element; `$field` = parent document)
SELECT id, (SELECT filmId AS `$$ROOT` FROM Rentals WHERE staffId = 2) AS films
FROM customers
SELECT (
    SELECT * FROM Rentals WHERE INDEXOF_ARRAY(`$favouriteFilms`, filmId) >= 0
) AS favourites FROM customers

-- SUM with CASE
SELECT SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS activeCount
FROM customers

-- Date filter with auto-conversion
SELECT * FROM orders WHERE TO_DATE(orderDate) > TO_DATE('2021-01-01')

-- Window function
SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk, UNSET(_id) FROM results
```
