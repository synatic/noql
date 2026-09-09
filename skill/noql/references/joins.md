# Joins

By default, joins produce an **array field** on each result document. Use **join hints** to control output shape.

## Join types

```sql
-- LEFT JOIN: all left records; matched right records (or empty array)
SELECT *, UNSET(_id) FROM orders AS o
LEFT JOIN `inventory` AS i ON o.item = i.sku

-- INNER JOIN: only records with a match on both sides
SELECT *, UNSET(_id) FROM orders AS o
INNER JOIN `inventory` AS i ON o.item = i.sku

-- FULL OUTER JOIN
SELECT c.customerName, o.orderId, UNSET(_id)
FROM "foj-customers" c
FULL OUTER JOIN "foj-orders" o ON c.customerId = o.customerId
ORDER BY c.customerName ASC
```

## Join hints

Append hints to the table name or alias using `|`. Multiple hints can be chained.

```sql
-- |first  → collapse result array to first match (object, not array)
SELECT * FROM customers c
INNER JOIN `customer-notes` AS `cn|first` ON cn.id = c.id

-- Also valid on the alias side of AS:
LEFT OUTER JOIN 'customer-notes' 'cn|first' ON cn.id = to_int(c.id)

-- |last   → collapse result array to last match
SELECT * FROM orders o
INNER JOIN `inventory` AS `inv|last` ON inv.sku = o.item

-- |unwind → expand each match into a separate result row
SELECT * FROM orders o
INNER JOIN `inventory` AS `inv|unwind` ON inv.sku = o.item

-- |optimize → push $match to the front of the sub-pipeline for better index use
-- Use with sub-select joins; the ON field must be part of the sub-select
SELECT c.*, cn.* FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|optimize`
ON cn.id = c.id

-- Chain hints: |optimize|first
SELECT c.*, cn.* FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|optimize|first`
ON cn.id = c.id
```

`|first` and `|last`: for complex ON conditions (functions, AND) or subquery joins, a `$limit: 1` / scan-all is applied inside the lookup pipeline. Simple `col = col` joins use MongoDB's native `localField` / `foreignField`.

## JOIN array functions (alternative SELECT syntax)

```sql
SELECT *, FIRST_IN_ARRAY(inventory) AS inv FROM orders
INNER JOIN `inventory` ON sku = item

SELECT *, LAST_IN_ARRAY(inventory) AS inv FROM orders
INNER JOIN `inventory` ON sku = item

SELECT *, UNWIND(inventory) AS inv FROM orders
INNER JOIN `inventory` ON sku = item
```

## Multi-table (N-level) joins

```sql
SELECT o.id AS orderId, i.id AS inventoryId, c.id AS customerId, UNSET(_id)
FROM orders AS o
INNER JOIN `inventory|unwind` AS i ON o.item = i.sku
INNER JOIN `customers|unwind` AS c ON o.customerId = c.id
```

## Subquery joins

```sql
SELECT c.*, cn.*
FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|first`
ON cn.id = c.id
```

## Join caveats

```sql
-- IN sub-select does NOT work as a join — use JOIN instead
-- WRONG:
SELECT * FROM orders WHERE inventory IN (SELECT * FROM inventory WHERE sku=orders.item)
-- CORRECT:
SELECT * FROM orders INNER JOIN inventory inv ON inv.sku = orders.item
```
