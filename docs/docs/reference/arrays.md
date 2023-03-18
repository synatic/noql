# Arrays

Methods that perform operations on array fields. Can be used as part of select statements and queries.

M-SQL uses sub-selects to query array fields in collections. E.g.

```sql
select (select * from Rentals where staffId=2) as t from `customers`
```

Using '$$ROOT' in sub select promotes the field to the root value of the array

```sql
select (select filmId as '$$ROOT' from Rentals where staffId=2) as t from `customers`
```

Slicing the array is supported by limit and offset in queries

```sql
select (select * from Rentals where staffId=2 limit 10 offset 5) as t from `customers`
```

**NOTE:** _Sorting array in sub select is not supported and will need to use unwind_

```sql
--Wont'Work
select id,(select * from Rentals order by id desc) as totalRentals from customers
```

**NOTE:** _Aggregation functions not supported in sub select_

```sql
--Wont'Work
select id,(select count(*) as count from Rentals) as totalRentals from customers
```

| M-SQL Function                                | Description                                                                                                                                                                                                   | Example                                                                                                                                         |
| --------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------------------------------------- |
| IS_ARRAY(array expr)                          | Returns true when the field is an array                                                                                                                                                                       | `` select id,(case when IS_ARRAY(Rentals) then 'Yes' else 'No' end) as test from `customers`  ``                                                |
| ALL_ELEMENTS_TRUE(array expr)                 | Returns true when all elements in the array are true                                                                                                                                                          | `` select id,(case when ALL_ELEMENTS_TRUE(Rentals) then 'Yes' else 'No' end) as test from `customers`  ``                                       |
| ANY_ELEMENT_TRUE(array expr)                  | Returns true when any element in the array is true                                                                                                                                                            | `` select id,(case when ANY_ELEMENT_TRUE(Rentals) then 'Yes' else 'No' end) as test from `customers`  ``                                        |
| SIZE_OF_ARRAY(array expr)                     | Returns the size of array                                                                                                                                                                                     | `` select id,SIZE_OF_ARRAY(`Rentals`) as test from `customers`  ``                                                                              |
| FIRST_IN_ARRAY(array expr)                    | Returns the first element of an array                                                                                                                                                                         | `` select id,FIRST_IN_ARRAY(`Rentals`) as test from `customers`  ``                                                                             |
| LAST_IN_ARRAY(array expr)                     | Returns the last element of an array                                                                                                                                                                          | `` select id,LAST_IN_ARRAY(`Rentals`) as test from `customers`  ``                                                                              |
| REVERSE_ARRAY(array expr)                     | Reverses the order of an array field                                                                                                                                                                          | `` select id,REVERSE_ARRAY(`Rentals`) as test from `customers`  ``                                                                              |
| ARRAY_ELEM_AT(array expr,position)            | Returns the element of an array at a position                                                                                                                                                                 | `` select id,ARRAY_ELEM_AT(`Rentals`,5) as test from `customers`  ``                                                                            |
| INDEXOF_ARRAY(array expr,value,[start],[end]) | Returns the index of the value in the array                                                                                                                                                                   | `` select id,INDEXOF_ARRAY((select filmId as '$$ROOT' from `Rentals`),5) as test from `customers`  ``                                           |
| ARRAY_RANGE(from,to,step)                     | Generates an array of numbers from to with the specified step                                                                                                                                                 | `` select id,ARRAY_RANGE(0,10,2) as test from `customers`  ``                                                                                   |
| ZIP_ARRAY(array expr,...)                     | Transposes an array of input arrays so that the first element of the output array would be an array containing, the first element of the first input array, the first element of the second input array, etc. | `` select id,ZIP_ARRAY((select `Film Title` as '$$ROOT' from `Rentals`),ARRAY_RANGE(0,10,2)) as test from `customers`  ``                       |
| CONCAT_ARRAYS(array expr,...)                 | Concatenate the provided list of arrays                                                                                                                                                                       | `` select id,CONCAT_ARRAYS((select `Film Title` as '$$ROOT' from `Rentals`),ARRAY_RANGE(0,10,2)) as test from `customers`  ``                   |
| OBJECT_TO_ARRAY(expr)                         | Converts the object to an array                                                                                                                                                                               | `` select id,OBJECT_TO_ARRAY(`Address`) as test from `customers`  ``                                                                            |
| ARRAY_TO_OBJECT(expr)                         | Converts the array to an object                                                                                                                                                                               | `` select id,ARRAY_TO_OBJECT(OBJECT_TO_ARRAY(`Address`)) as test from `customers`  ``                                                           |
| SET_UNION(array expr,...)                     | Returns an array as the union of the provided arrays                                                                                                                                                          | `` select id,SET_UNION((select filmId as '$$ROOT' from `Rentals`),PARSE_JSON('[ 1,2,3,4] ')) as test from `customers`  ``                       |
| SET_DIFFERENCE(array expr,...)                | Returns an array as the difference of the provided arrays                                                                                                                                                     | `` select id,SET_DIFFERENCE((select filmId as '$$ROOT' from `Rentals`),PARSE_JSON('[ 1,2,3,4] ')) as test from `customers`  ``                  |
| SET_INTERSECTION(array expr,...)              | Returns an array as the difference of the provided arrays                                                                                                                                                     | `` select id,SET_INTERSECTION((select filmId as '$$ROOT' from `Rentals`),PARSE_JSON('[ 1,2,3,4] ')) as test from `customers`  ``                |
| SET_EQUALS(array expr,...)                    | Returns true or false if the arrays are equal                                                                                                                                                                 | `` select id,SET_EQUALS((select filmId as '$$ROOT' from `Rentals`),PARSE_JSON('[ 1,2,3,4] ')) as test from `customers`  ``                      |
| SET_IS_SUBSET(array expr,...)                 | Returns whether an array is a subset of another                                                                                                                                                               | `` select id,SET_IS_SUBSET((select filmId as '$$ROOT' from `Rentals`),PARSE_JSON('[ 1,2,3,4] ')) as test from `customers`  ``                   |
| SUM_ARRAY(array expr,[field])                 | Sums the values in an array given an array field or sub-select and the field to sum                                                                                                                           | `` select SUM_ARRAY(`Rentals`,'staffId') as totalStaffIds from `customers`  ``                                                                  |
|                                               | With sub select                                                                                                                                                                                               | `` select id,`First Name`,`Last Name`,SUM_ARRAY((select SUM_ARRAY(`Payments`,'Amount') as total from `Rentals`),'total') as t from customers `` |
| AVG_ARRAY(array expr,[field])                 | Average the elements in an array by field                                                                                                                                                                     | `` select id,`First Name`,`Last Name`,avg_ARRAY(`Rentals`,'filmId') as avgIdRentals from customers ``                                           |

[//]: # '| MIN_ARRAY(array expr,[field])                                                           | returns the minimum of a value array e.g. [1,2,3]                                                                                                                                                             | `` select MIN_ARRAY((select filmId as `$$ROOT` from Rentals)) as s from customers  ``                                                           |'
[//]: # '| MAX_ARRAY(array expr,[field])                                                           | returns the maximum of a value array e.g. [1,2,3]                                                                                                                                                             | `` select MAX_ARRAY((select filmId as `$$ROOT` from Rentals)) as s from customers  ``                                                           |'
