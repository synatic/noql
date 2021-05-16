# sql-to-mongo
Converts SQL Queries to Mongo find statements or aggregation pipelines


##Installation
```
npm i @synatic/sql-to-mongo --save
```

##Usage
```
const SQLMongoParser=require('@synatic/sql-to-mongo');
```

###parseSQL

###makeMongoQuery

###makeMongoAggregate

###canQuery

##parseSQLtoAST


##Notes
As with MongoDB column names and strings are case sensitive.

Follows mySQL Syntax

Requires as for functions and sub queries
```
select abs(-1) as `absId` from `customers`
```
Supports Mongo 3.6 or greater

as on table requires prefixing
```
select c.* from customers  as c 
```

Always prefix on joins

##Supported SQL Statements

###$$ROOT
```
select t as `$$ROOT` from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id')  as numRentals from customers) as t
```

###Limit and Offset
Supports MySQL style limits and offset that equates to limit and skip
```
select (select * from Rentals) as t from `customers` limit 10 offset 2
```

###Merge Fields into Object
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

###Group By and Having

###Joins

###Sub Queries

###Case Statements




###Cast
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

###Mathematical Functions

###Arrays
Use sub-select to query array fields in collections
```
select (select * from Rentals where staffId=2) as t from `customers`
```
####sumArray
Sums the values in an array given an array field or sub-select and the field to sum
```
select sumArray(`Rentals`,'fileId') as totalFileIds from `customers`
```

e.g. with sub select
```
select id,`First Name`,`Last Name`,sumArray((select sumArray(`Payments`,'Amount') as total from `Rentals`),'total') as t from customers
```
####avgArray
####firstInArray ($first)
####lastInArray ($last)
####isArray ($isArray)
Returns true if the field or expression is an Array
```
select id,(case when isArray(Rentals) then 'Yes' else 'No' end) as test from `customers`
```
####Map ($map) - Not fully supported
Use sub select
```
select id,(select filmId from `Rentals` limit 10 offset 5) as Rentals from customers
```
####Slice ($slice)
Use sub select
```
select id,(select * from `Rentals` limit 10 offset 5) as Rentals from customers
```

###Group Methods
sum
avg
max
min


##Unsupported SQL Statements
Over

CTE's

IN fixed value list (not supported by AST parser)

From SubQuery as first statement: (select * from (select * from customers)). First item must be a named collection

Sorting array in sub select, user unwind

###Selecting on a calculated column by name
Calculated columns in where statements can only be used with aggregates  
```
--have to repeat select statemnt as with sql rules
select id,Title,Rating,abs(id) as absId from `films` where abs(id)=1
```

##Function Mapping

|Mongo Function |M-SQL Function |Description  |Example  |
