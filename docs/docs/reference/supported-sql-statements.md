# Supported SQL Statements

Functions in WHERE statements require explicit definition and can't use a computed field

```sql
--Correct
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by absId

--Won't work
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and absId > 1
```

ORDER BY requires field to be part of select

```sql
--Correct
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by absId

--Won't work
select `Address.City` as City from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by abs(`id`)
```

## Limit and Offset

Supports MySQL style limits and offset that equates to limit and skip

```sql
select (select * from Rentals) as t from `customers` limit 10 offset 2
```

## Merge Fields into Object

Only available in aggregate
Select without table

Create a new Object

```sql
select (select id,`First Name` as Name) as t  from customers
```

Create a new Object and assign to root

```sql
select (select id,`First Name` as Name) as t1, (select id,`Last Name` as LastName) as t2,MERGE_OBJECTS(t1,t2) as `$$ROOT`  from customers
```

Using with unwind with joins

```sql
select MERGE_OBJECTS((select t.CustomerID,t.Name),t.Rental) as `$$ROOT` from (select id as CustomerID,`First Name` as Name,unwind(Rentals) as Rental from customers) as t
```

PARSE_JSON(json string)


## Selecting on a calculated column by name

Calculated columns in where statements can only be used with aggregates

```sql
--have to repeat select statemnt as with sql rules
select id,Title,Rating,abs(id) as absId from `films` where abs(id)=1
```
