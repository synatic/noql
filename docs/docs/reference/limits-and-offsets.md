# Limit and Offset

Supports MySQL style limits and offset that equates to limit and skip

```sql
select (select * from Rentals) as t from `customers` limit 10 offset 2
```
