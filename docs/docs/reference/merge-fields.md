# Merge Fields into Object

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

