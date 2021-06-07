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

| M-SQL Function | Description | Example |
| ------------- | ------------- | ------------- |
| ABS(expr) |  Returns the absolute value of a number. | ```select ABS(`Replacement Cost`) as exprVal from `films` ```  |
| ACOS(expr) |  Returns the inverse cosine (arc cosine) of a value. | ```select ACOS(`Replacement Cost`) as exprVal from `films` ```  |
| ACOSH(expr) |  Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value. | ```select ACOSH(`Replacement Cost`) as exprVal from `films` ```  |
| SUM(expr,expr,...) | Sums the values provided in the expression | ```select SUM(`Replacement Cost`,2,id) as s from `films` ```  |

| ACOSH(expr) |  Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value. | ```select ACOSH(`Replacement Cost`) as exprVal from `films` ```  |
| CEIL(expr) | Returns the smallest integer greater than or equal to the specified number. | ```select CEIL(`Replacement Cost`, 1) as exprVal from `films` ```  |
| EXP(expr) | Raises Euler's number (i.e. e ) to the specified exponent and returns the result. | ```select EXP(`Replacement Cost`, 1) as exprVal from `films` ```  |
| TRUNC(expr,[places]) |  Truncates a number to a whole integer or to a specified decimal place | ```select TRUNC(`Replacement Cost`, 1) as exprVal from `films` ```  |



### Arrays
Use sub-select to query array fields in collections
```
select (select * from Rentals where staffId=2) as t from `customers`
```
Sorting array in sub select, user unwind
'$$root' in sub select promotes the field to the value

| M-SQL Function | Description | Example |
| ------------- | ------------- | ------------- |
| IS_ARRAY(array expr) | Returns true when the field is an array | ```select id,(case when IS_ARRAY(Rentals) then 'Yes' else 'No' end) as test from `customers` ``` |
| ALL_ELEMENTS_TRUE(array expr) | Returns true when all elements in the array are true | ```select id,(case when ALL_ELEMENTS_TRUE(Rentals) then 'Yes' else 'No' end) as test from `customers` ``` | 
| ANY_ELEMENT_TRUE(array expr) | Returns true when any element in the array is true | ```select id,(case when ANY_ELEMENT_TRUE(Rentals) then 'Yes' else 'No' end) as test from `customers` ``` |
| SIZE_OF_ARRAY(array expr) | Returns the size of array | ```select id,SIZE_OF_ARRAY(`Rentals`) as test from `customers` ``` |
| FIRST_IN_ARRAY(array expr) | Returns the first element of an array | ```select id,FIRST_IN_ARRAY(`Rentals`) as test from `customers` ``` |
| LAST_IN_ARRAY(array expr) | Returns the last element of an array | ```select id,LAST_IN_ARRAY(`Rentals`) as test from `customers` ``` |
| REVERSE_ARRAY(array expr) | Reverses the order of an array field | ```select id,REVERSE_ARRAY(`Rentals`) as test from `customers` ``` |
| ARRAY_ELEM_AT(array expr,position) | Returns the element of an array at a position | ```select id,ARRAY_ELEM_AT(`Rentals`,5) as test from `customers` ``` |
| INDEXOF_ARRAY(array expr,value,[start],[end]) | Returns the index of the value in the array | ```select id,INDEXOF_ARRAY((select filmId as '$$ROOT' from `Rentals`),5) as test from `customers` ``` |
| ARRAY_RANGE(from,to,step) | Generates an array of numbers from to with the specified step | ```select id,ARRAY_RANGE(0,10,2) as test from `customers` ``` |
| ZIP_ARRAY(array expr,...) | Transposes an array of input arrays so that the first element of the output array would be an array containing, the first element of the first input array, the first element of the second input array, etc. | ```select id,ZIP_ARRAY((select `Film Title` as '$$ROOT' from `Rentals`),ARRAY_RANGE(0,10,2)) as test from `customers` ``` |
| SUM_ARRAY(array expr,[field]) |  Sums the values in an array given an array field or sub-select and the field to sum | ```select SUM_ARRAY(`Rentals`,'staffId') as totalStaffIds from `customers` ```  |
|  | With sub select | ```select id,`First Name`,`Last Name`,SUM_ARRAY((select SUM_ARRAY(`Payments`,'Amount') as total from `Rentals`),'total') as t from customers``` |
| AVG_ARRAY(array expr,[field]) | Average the elements in an array by field | ```select id,`First Name`,`Last Name`,avg_ARRAY(`Rentals`,'filmId') as avgIdRentals from customers``` |

#### firstInArray ($first)
#### lastInArray ($last)


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





### Selecting on a calculated column by name
Calculated columns in where statements can only be used with aggregates  
```
--have to repeat select statemnt as with sql rules
select id,Title,Rating,abs(id) as absId from `films` where abs(id)=1
```

## Function Mapping


### Operators
