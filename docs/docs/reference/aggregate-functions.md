# Aggregate Functions

| Aggregate Function | Example                                                                                                                    |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| SUM                | `` select sum(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| AVG                | `` select avg(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| MIN                | `` select min(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| MAX                | `` select max(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| COUNT              | `` select count(*) as countVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |

## Sum / Case Logic

Sum Case logic is supported:

```sql
select sum(case when `Address.City`='Ueda' then 1 else 0 end) as Ueda,sum(case when `Address.City`='Tete' then 1 else 0 end) as Tete from `customers` group by `xxx`
```
