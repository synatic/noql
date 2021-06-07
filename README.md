# sql-to-mongo
Converts M-SQL Queries to Mongo find statements or aggregation pipelines

**What is M-SQL**

M-SQL is a specific way to use MySQL style queries tailored to MongoDB functions and a multilevel document paradigm. 

## Notes
* Supports Mongo 3.6 or greater
* Follows mySQL Syntax
* As with MongoDB column names and strings are case sensitive.

## Installation
```
npm i @synatic/sql-to-mongo --save
```

## Usage
```
const SQLMongoParser=require('@synatic/sql-to-mongo');
```

### parseSQL

### makeMongoQuery

### makeMongoAggregate

### canQuery

### parseSQLtoAST

## M-SQL

Requires as for functions and sub queries
```
select abs(-1) as `absId` from `customers`
```


as on table requires prefixing
```
select c.* from customers  as c 
```

Always prefix on joins

## Supported SQL Statements

### $$ROOT
```
select t as `$$ROOT` from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id')  as numRentals from customers) as t
```

### Limit and Offset
Supports MySQL style limits and offset that equates to limit and skip
```
select (select * from Rentals) as t from `customers` limit 10 offset 2
```

### Merge Fields into Object
Only available in aggregate
Select without table

Create a new Object
```
select (select id,`First Name` as Name) as t  from customers
```
Create a new Object and assign to root 
```
select (select id,`First Name` as Name) as t1, (select id,`Last Name` as LastName) as t2,mergeObjects(t1,t2) as `$$ROOT`  from customers
```

Using with unwind with joins
```
select mergeObjects((select t.CustomerID,t.Name),t.Rental) as `$$ROOT` from (select id as CustomerID,`First Name` as Name,unwind(Rentals) as Rental from customers) as t
```

### Group By and Having



### Joins

hints
first
last
unwind

### Sub Queries

### Case Statements


### Aggregate Functions
| Aggregate Function | Example |
| ------------- | ------------- |
| SUM | ``` select sum(`id`) as aggrVal,`Address.City` as City  from `customers` group by `Address.City` order by `Address.City` ``` |
| AVG | ``` select avg(`id`) as aggrVal,`Address.City` as City  from `customers` group by `Address.City` order by `Address.City` ``` |
| MIN | ``` select min(`id`) as aggrVal,`Address.City` as City  from `customers` group by `Address.City` order by `Address.City` ``` |
| MAX | ``` select max(`id`) as aggrVal,`Address.City` as City  from `customers` group by `Address.City` order by `Address.City` ``` |
| COUNT | ``` select count(*) as countVal,`Address.City` as City  from `customers` group by `Address.City` order by `Address.City` ``` |
| ADDTOSET - Not supported | Returns an array of all unique values that results from applying an expression to each document in a group of documents that share the same group by key |
Requires group by for aggregate functions. If grouping by top level, use fake field for group by: 
```
select sum(`id`) as sumId  from `customers` group by `xxxx`
 ```
#### SUM CASE
Sum Case logic is supported:
```
select sum(case when `Address.City`='Ueda' then 1 else 0 end) as Ueda,sum(case when `Address.City`='Tete' then 1 else 0 end) as Tete from `customers` group by `xxx`
```
### Cast
Supports cast operations to the MySQL types: VARCHAR, INT, DECIMAL, DATETIME, DECIMAL
```
select cast(1+`id` as varchar) as `id` from `customers`
select cast(abs(-1) as varchar) as `id` from `customers`
select cast(`id` as varchar) as `id` from `customers`
```
Alternatively, the convert function supports the mongodb convert types: double, string, bool, date, int, objectId ,long , decimal
```
select convert('1','int') as d `films`
select convert(`id`,'string') as d `films`
```

### Mathematical Functions

### Arrays
Use sub-select to query array fields in collections
```
select (select * from Rentals where staffId=2) as t from `customers`
```
| M-SQL Function | Description | Example |
| ------------- | ------------- | ------------- |
| IS_ARRAY(expr) | Returns true when the field is an array | ```select id,(case when IS_ARRAY(Rentals) then 'Yes' else 'No' end) as test from `customers` ``` | 
| SUM_ARRAY(array expr,[field]) |  Sums the values in an array given an array field or sub-select and the field to sum | ```select SUM_ARRAY(`Rentals`,'staffId') as totalStaffIds from `customers` ```  |
|  | With sub select | ```select id,`First Name`,`Last Name`,SUM_ARRAY((select SUM_ARRAY(`Payments`,'Amount') as total from `Rentals`),'total') as t from customers``` |
| AVG_ARRAY(array expr,[field]) | Average the elements in an array by field | ```select id,`First Name`,`Last Name`,avg_ARRAY(`Rentals`,'filmId') as avgIdRentals from customers``` |

#### firstInArray ($first)
#### lastInArray ($last)

#### allElementsTrue
anyElementTrue
#### Unwind


Returns true if the field or expression is an Array
```
select id,(case when isArray(Rentals) then 'Yes' else 'No' end) as test from `customers`
```
#### Map ($map) - Not fully supported
Use sub select
```
select id,(select filmId from `Rentals` limit 10 offset 5) as Rentals from customers
```
#### Slice ($slice)
Use sub select
```
select id,(select * from `Rentals` limit 10 offset 5) as Rentals from customers
```

### Unsupported SQL Statements
* Over
* CTE's
* IN fixed value list
* Pivot



Sorting array in sub select, user unwind

### Selecting on a calculated column by name
Calculated columns in where statements can only be used with aggregates  
```
--have to repeat select statemnt as with sql rules
select id,Title,Rating,abs(id) as absId from `films` where abs(id)=1
```

## Function Mapping

| Mongo Function | M-SQL Function | Description | Example |
| ------------- | ------------- | ------------- | ------------- |
| $abs | ABS(expr) |  Returns the absolute value of a number. | ```select ABS(`Replacement Cost`) as exprVal from `films` ```  |
| $acos | ACOS(expr) |  Returns the inverse cosine (arc cosine) of a value. | ```select ACOS(`Replacement Cost`) as exprVal from `films` ```  |
| $acosh | ACOSH(expr) |  Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value. | ```select ACOSH(`Replacement Cost`) as exprVal from `films` ```  |
| $add | SUM(expr,expr,...) | Sums the values provided in the expression | ```select SUM(`Replacement Cost`,2,id) as s from `films` ```  |
 
| ACOSH(expr) |  Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value. | ```select ACOSH(`Replacement Cost`) as exprVal from `films` ```  |
| $ceil | CEIL(expr) | Returns the smallest integer greater than or equal to the specified number. | ```select CEIL(`Replacement Cost`, 1) as exprVal from `films` ```  |
| $exp | EXP(expr) | Raises Euler's number (i.e. e ) to the specified exponent and returns the result. | ```select EXP(`Replacement Cost`, 1) as exprVal from `films` ```  |
| $trunc | TRUNC(expr,[places]) |  Truncates a number to a whole integer or to a specified decimal place | ```select TRUNC(`Replacement Cost`, 1) as exprVal from `films` ```  |



### Operators
