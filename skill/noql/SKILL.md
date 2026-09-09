---
name: noql
description: Write NoQL SQL and convert it to MongoDB find queries or aggregation pipelines with @synatic/noql. Use when writing NoQL, translating SQL to MongoDB, building $lookup joins, array sub-selects, or when the user mentions NoQL, SQL-to-Mongo, or @synatic/noql.
license: GPL-3.0-or-later
metadata:
  author: Synatic Inc
  homepage: https://noql.synatic.dev
---

# NoQL

NoQL converts SQL into MongoDB queries (`type: 'query'`) or aggregation pipelines (`type: 'aggregate'`).

Write **NoQL**, not generic SQL. Load a reference file when the task needs joins, arrays, objects, or function catalogs.

- Docs: https://noql.synatic.dev
- NPM: `@synatic/noql`

## Instructions

1. Produce valid NoQL that `@synatic/noql` can parse. Do not invent SQL features NoQL does not support.
2. Follow quoting, aliasing, and caveats in this file on every query.
3. Read the matching reference before writing joins, array sub-selects, object helpers, or less-common functions:
   - Joins and `|first` / `|last` / `|unwind` / `|optimize` hints — [references/joins.md](references/joins.md)
   - Array sub-selects and array functions — [references/arrays.md](references/arrays.md)
   - Objects (`$$ROOT`, `FLATTEN`, `MERGE_OBJECTS`) — [references/objects.md](references/objects.md)
   - Conversion, string, date, math, window, UNION, pivot — [references/functions.md](references/functions.md)
   - Copy-paste examples — [references/examples.md](references/examples.md)
4. If generating Node.js, call `parseSQL` and branch on `query` vs `aggregate` as shown below.

## Installation

```bash
npm i @synatic/noql --save
```

## API usage

```js
const SQLParser = require('@synatic/noql');

const parsedSQL = SQLParser.parseSQL(
    'SELECT id FROM `films` LIMIT 10',
    {database: 'postgresql'} // or 'mysql'
);

if (parsedSQL.type === 'query') {
    await db
        .collection(parsedSQL.collection)
        .find(parsedSQL.query || {}, parsedSQL.projection || {})
        .limit(parsedSQL.limit || 50)
        .toArray();
} else if (parsedSQL.type === 'aggregate') {
    await db.collection(parsedSQL.collections[0]).aggregate(parsedSQL.pipeline).toArray();
}
```

## Quoting conventions

| Situation | Quote character | Example |
|---|---|---|
| Field names with spaces or special chars | backtick or single quote | `` `First Name` `` or `'First Name'` |
| `$` Mongo paths in array sub-selects | backtick | `` `$favouriteFilms` ``, `` `$$ROOT.filmId` `` |
| Collection names with hyphens | backtick or single/double quote | `` `customer-notes` `` |
| String literals | single quote | `'hello'` |
| Aliased table in FROM | backtick or no quotes | `FROM customers AS c` |

MongoDB field names are **case-sensitive**. Use dot notation to traverse nested documents: `` `Address.City` ``.

## Aliasing rules

- Functions **must** be aliased: `ABS(id) AS absId`
- Subqueries **must** be aliased: `(SELECT ...) AS t`
- Table aliases enable prefixed field access: `FROM customers AS c` → `c.id`

```sql
SELECT ABS(-1) AS absId FROM `customers`
SELECT t.id FROM (SELECT id FROM `films`) AS t
```

## Core syntax

### SELECT / FROM

```sql
SELECT id, `First Name`, `Address.City` FROM customers
SELECT * FROM customers
SELECT *, UNSET(_id) FROM customers
SELECT *, UNSET(_id, `Address`) FROM customers
```

### WHERE

```sql
SELECT * FROM customers WHERE id = 1
SELECT * FROM customers WHERE `First Name` LIKE 'm%'
SELECT * FROM customers WHERE id > 1 AND id < 10
SELECT * FROM customers WHERE FIELD_EXISTS(id, true)
SELECT * FROM customers WHERE FIELD_EXISTS(phone, false)

-- Functions in WHERE must be repeated; cannot reference a computed alias
SELECT ABS(id) AS absId FROM customers WHERE ABS(id) > 1
```

### GROUP BY / aggregates

```sql
SELECT SUM(id) AS total, `Address.City` AS City
FROM customers GROUP BY `Address.City` ORDER BY City

SELECT COUNT(*) AS cnt, `Address.City` AS City
FROM customers GROUP BY `Address.City`

SELECT COUNT(DISTINCT `Address.Town`) AS uniq, `Address.City` AS City
FROM customers GROUP BY `Address.City`

SELECT AVG(id) AS avg, MIN(id) AS mn, MAX(id) AS mx, `Address.City` AS City
FROM customers GROUP BY `Address.City`

SELECT `Address.City` AS City, FIRSTN(10) AS sample
FROM customers GROUP BY `Address.City`

SELECT SUM(CASE WHEN `Address.City`='Ueda' THEN 1 ELSE 0 END) AS Ueda
FROM customers
```

### ORDER BY / LIMIT / OFFSET

```sql
SELECT * FROM customers ORDER BY `Last Name` ASC, id DESC
SELECT * FROM customers LIMIT 10
SELECT * FROM customers LIMIT 10 OFFSET 5
```

## Key caveats

1. **Case-sensitive fields and values** — `First Name` ≠ `first name`
2. **Stored field names cannot start with `$` or contain `.`**. Dot notation is for traversal. In array sub-selects, a backtick-quoted `$path` is a parent-document Mongo path, not a stored field name.
3. **Functions in WHERE must be repeated** — cannot reference a computed alias
4. **`+` is not string concatenation** — use `CONCAT(a, b)`
5. **IN sub-select does not work** — use JOIN instead
6. **Aggregate functions are not allowed inside array sub-selects**
7. **Always alias functions and subqueries** — `ABS(id) AS absId`, `(SELECT ...) AS t`
8. **`|optimize` hint** only helps when the ON field is part of the sub-select
9. **Array sorting** requires MongoDB 5.2+

## Additional resources

- [references/joins.md](references/joins.md)
- [references/arrays.md](references/arrays.md)
- [references/objects.md](references/objects.md)
- [references/functions.md](references/functions.md)
- [references/examples.md](references/examples.md)
