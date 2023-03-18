# Where Clauses

Where clauses are used to filter the results of a query. They are used in the same way as in SQL.

Functions in WHERE clauses require explicit definition and can't use a computed field

```sql
--Correct
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by absId

--Won't work
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and absId > 1
```

## Selecting on a calculated column by name

Calculated columns in where clauses can only be used with aggregates

```sql
--have to repeat select statement as with sql rules
select id,Title,Rating,abs(id) as absId from `films` where abs(id)=1
```
