# sql-to-mongo
Converts SQL Queries to Mongo find statements or aggregation pipelines




Supported methods



###Notes
As with monogo, case sensitive

Requires as for functions and sub queries

``
select 
``

###Array Methods
use sub-select to query array fields in collections
``
 select (select * from ArrayField) as t from `customers`
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

####firstInArray
####lastInArray


## Unsupported
over

case statements

CTE's

##Mongo Example Usage
