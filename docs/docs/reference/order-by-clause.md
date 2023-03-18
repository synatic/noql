# Order By Clause


ORDER BY requires the field to be part of the `SELECT`

```sql
--Correct
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by absId

--Won't work
select `Address.City` as City from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by abs(`id`)
```
