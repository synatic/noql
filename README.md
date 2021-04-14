# sql-to-mongo
Converts SQL Queries to Mongo find statements or aggregation pipelines


##Installation
``
npm i @synatic/sql-to-mongo --save
``

##Usage
``
const SQLParser=require('@synatic/sql-to-mongo'');



``

##Notes
As with monogo, case sensitive
Follows mySQL Syntax
Requires as for functions and sub queries

``
select 
``

##Supported SQL Statements

###Limit and Offset
Supports MySQL style limits and offset that equates to limit and skip
``
select (select * from Rentals) as t from `customers` limit 10 offset 2
``

###Group By and Having

###Joins

###Sub Queries



###Mathematical Functions

###Array Methods
use sub-select to query array fields in collections
``
 select (select * from Rentals) as t from `customers`
``
####sumArray
Sums the values in an array given an array field or sub-select and the field to sum

``
select sumArray(`Rentals`,'fileId') as totalFileIds from `customers`
``

e.g. with sub select

``
select id,`First Name`,`Last Name`,sumArray((select sumArray(`Payments`,'Amount') as total from `Rentals`),'total') as t from customers
``

####firstInArray ($first)
####lastInArray ($last)
####isArray

####Map ($map) - Not fully supported
Use sub select
``
select id,(select filmId from `Rentals` limit 10 offset 5) as Rentals from customers
``
####Slice ($slice)
Use sub select
``
select id,(select * from `Rentals` limit 10 offset 5) as Rentals from customers
``
##Unsupported SQL Statements
over

case statements

CTE's

##Mongo Example Usage
