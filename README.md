# sql-to-mongo
Converts SQL Queries to Mongo find statements or aggregation pipelines


##Installation
```
npm i @synatic/sql-to-mongo --save
```

##Usage
```
const SQLMongoParser=require('@synatic/sql-to-mongo'');
```

###parseSQL

###makeMongoQuery

###makeMongoAggregate

###canQuery

##parseSQLtoAST


##Notes
As with monogo, case sensitive
Follows mySQL Syntax
Requires as for functions and sub queries
Mongo 3.6
```
select 
```

##Supported SQL Statements

###Limit and Offset
Supports MySQL style limits and offset that equates to limit and skip
```
select (select * from Rentals) as t from `customers` limit 10 offset 2
```

###Group By and Having

###Joins

###Sub Queries

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

###Array Methods
use sub-select to query array fields in collections
```
 select (select * from Rentals) as t from `customers`
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

####firstInArray ($first)
####lastInArray ($last)
####isArray

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
##Unsupported SQL Statements
over

case statements

CTE's

IN fixed value list

from SubQuery (select * from (select * from customers)). First item must be a named collection

Sorting array in sub select, user unwind
