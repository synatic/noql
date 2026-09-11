# Arrays

Use sub-selects with an array field name as the FROM target.

```sql
-- Filter array elements
SELECT (SELECT * FROM Rentals WHERE staffId = 2) AS filtered FROM customers

-- Project specific fields
SELECT (SELECT filmId, staffId FROM Rentals WHERE staffId = 2) AS t FROM customers

-- $$ROOT: promote a single field to root values of the result array
SELECT (SELECT filmId AS `$$ROOT` FROM Rentals WHERE staffId = 2) AS filmIds FROM customers

-- Slice array
SELECT (SELECT * FROM Rentals LIMIT 10 OFFSET 5) AS page FROM customers

-- Sort array (MongoDB 5.2+)
SELECT id, (SELECT * FROM Rentals ORDER BY id DESC) AS sorted FROM customers

-- Aggregate functions are NOT supported inside array sub-selects
```

## Field references in array sub-selects

Array sub-selects compile to `$map` / `$filter`. Bare names bind to the current element (`$$this`). A backtick-quoted `$path` is a parent/root document field.

| NoQL | Meaning | MongoDB |
|------|---------|---------|
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

## Array functions

| Function | Signature | Description |
|---|---|---|
| `ALL_ELEMENTS_TRUE` | `(array)` | All elements truthy? |
| `ANY_ELEMENT_TRUE` | `(array)` | Any element truthy? |
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
