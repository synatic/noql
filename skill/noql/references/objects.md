# Objects

```sql
-- $$ROOT: promote a sub-expression to the document root
SELECT t AS `$$ROOT`
FROM (SELECT id, `First Name`, `Last Name` FROM customers) AS t

-- Create an inline object (SELECT without FROM)
SELECT (SELECT id, `First Name` AS Name) AS obj FROM customers

-- MERGE_OBJECTS
SELECT id, MERGE_OBJECTS(`Address`, PARSE_JSON('{"val":1}')) AS merged FROM customers

-- FLATTEN: spread object fields to root with optional prefix
SELECT id, FLATTEN(`Address`, 'addr_') FROM customers
-- Third arg true removes the original field:
SELECT id, FLATTEN(`Address`, 'addr_', true) FROM customers

-- PARSE_JSON
SELECT ARRAY_TO_OBJECT(PARSE_JSON('[{"k":"key","v":1}]')) AS obj FROM customers

-- EMPTY_OBJECT
SELECT EMPTY_OBJECT() AS empty FROM customers
```
