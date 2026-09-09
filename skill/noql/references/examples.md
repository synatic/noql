# Examples

```sql
-- Simple query
SELECT id, name, UNSET(_id) FROM customers WHERE id > 5 LIMIT 10

-- Nested field access
SELECT `Address.City`, `Address.Country` FROM customers

-- Aggregate
SELECT COUNT(*) AS cnt, `Address.City` FROM customers GROUP BY `Address.City`

-- Left join with |first hint
SELECT c.id, cn.notes AS note, UNSET(_id)
FROM customers c
LEFT OUTER JOIN 'customer-notes' 'cn|first' ON cn.id = TO_INT(c.id)

-- Inner join with |unwind hint
SELECT o.id, i.sku, i.instock, UNSET(_id)
FROM orders o
INNER JOIN `inventory|unwind` i ON i.sku = o.item

-- Subquery join with |optimize|first
SELECT c.*, cn.* FROM customers c
INNER JOIN (SELECT * FROM `customer-notes` WHERE id > 2) `cn|optimize|first`
ON cn.id = c.id

-- Array sub-select: filter and project (bare names = element; `$field` = parent)
SELECT id, (SELECT filmId AS `$$ROOT` FROM Rentals WHERE staffId = 2) AS films
FROM customers
SELECT (
    SELECT * FROM Rentals WHERE INDEXOF_ARRAY(`$favouriteFilms`, filmId) >= 0
) AS favourites FROM customers

-- SUM with CASE
SELECT SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS activeCount
FROM customers

-- Date comparison with auto-conversion
SELECT * FROM orders WHERE TO_DATE(orderDate) > TO_DATE('2021-01-01')

-- Window function
SELECT id, RANK() OVER (ORDER BY score DESC) AS rnk, UNSET(_id) FROM results

-- GROUP BY date bucket
SELECT DATE_TRUNC(orderDate, 'month') AS month, COUNT(*) AS cnt
FROM orders GROUP BY DATE_TRUNC(orderDate, 'month') ORDER BY month

-- FROM subquery (must alias)
SELECT t.id, t.total FROM (
    SELECT id, SUM(price) AS total FROM orders GROUP BY id
) AS t
```
