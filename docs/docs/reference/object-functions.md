# Objects

Supported methods that perform operations on objects

| M-SQL Function      | Description            | Example                                                                                      |
| ------------------- | ---------------------- | -------------------------------------------------------------------------------------------- |
| PARSE_JSON(expr)    | Parses the JSON string | `` select id,ARRAY_TO_OBJECT(PARSE_JSON('[{"k":"val","v":1}]')) as test from `customers`  `` |
| MERGE_OBJECTS(expr) | Merges objects         | `` select id,MERGE_OBJECTS(`Address`,PARSE_JSON('{"val":1}')) as test from `customers`  ``   |
|                     | With Sub Select        | `` select id,MERGE_OBJECTS(`Address`,(select 1 as val)) as test from `customers`  ``         |

## $$ROOT

Specifying '$$ROOT' as a column alias sets the value to root object but only works with aggregates (unless contained in array sub select)

```sql
select t as `$$ROOT` from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id')  as numRentals from customers) as t
```
