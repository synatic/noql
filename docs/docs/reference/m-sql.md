# `#!js M-SQL`

Requires as for functions and sub queries

```sql
select abs(-1) as `absId` from `customers`
```

as on table requires prefixing

```sql
select c.* from customers as c
```

Always prefix on joins
