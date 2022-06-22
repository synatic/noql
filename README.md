# sql-to-mongo

Converts M-SQL Queries to Mongo find statements or aggregation pipelines.

What is M-SQL?

M-SQL is a specific way to use MySQL style queries tailored to MongoDB functions and a multilevel document paradigm.

## Notes

-   Supports Mongo 3.6 or greater
-   Follows mySQL Syntax
-   As with MongoDB column names and strings are case-sensitive.

### Currently Unsupported SQL Statements

-   Over
-   CTE's
-   Pivot
-   Union

## Installation

```bash
npm i @synatic/sql-to-mongo --save
```

## Usage

```js
const SQLMongoParser = require('@synatic/sql-to-mongo');
```

### parseSQL(sqlStatement)

Parses the given SQL statement to an aggregate or query depending on if a straight query is possible.

```js
const SQLMongoParser=require('@synatic/sql-to-mongo');
console.log(JSON.stringify(SQLMongoParser.parseSQL("select id from `films` where `id` > 10 limit 10"),null,4));

{
    "limit": 10,
    "collection": "films",
    "projection": {
        "id": "$id"
    },
    "query": {
        "id": {
            "$gt": 10
        }
    },
    "type": "query"
}
```

```js
const SQLMongoParser=require('@synatic/sql-to-mongo');
console.log(JSON.stringify(SQLMongoParser.makeMongoAggregate("select id from `films` where `id` > 10 group by id"),null,4));
{
    "pipeline": [
        {
            "$match": {
                "id": {
                    "$gt": 10
                }
            }
        },
        {
            "$group": {
                "_id": {
                    "id": "$id"
                }
            }
        },
        {
            "$project": {
                "id": "$_id.id",
                "_id": 0
            }
        }
    ],
    "collections": [
        "films"
    ]
}
```

```js
const SQLParser = require('./lib/SQLParser');
const {MongoClient} = require('mongodb');

(async () => {
    try {
        client = new MongoClient('mongodb://127.0.0.1:27017');
        await client.connect();
        const db = client.db('sql-to-mongo-test');

        const parsedSQL = SQLParser.parseSQL('select id from `films` limit 10');
        if (parsedSQL.type === 'query') {
            console.log(
                await db
                    .collection(parsedSQL.collection)
                    .find(parsedSQL.query || {}, parsedSQL.projection || {})
                    .limit(parsedSQL.limit || 50)
                    .toArray()
            );
        } else if (parsedSQL.type === 'aggregate') {
            console.log(
                await db
                    .collection(parsedSQL.collections[0])
                    .aggregate(parsedSQL.pipeline)
                    .toArray()
            );
        }
    } catch (exp) {
        console.error(exp);
    }
})();
```

### makeMongoQuery(sqlStatement)

Generates a mongo query if possible. Will throw an exception if not possible.

```js
const SQLMongoParser=require('@synatic/sql-to-mongo');
console.log(SQLMongoParser.makeMongoQuery("select id from `films` where id > 10 limit 10"));

{
    "limit": 10,
    "collection": "films",
    "projection": {
        "id": "$id"
    },
    "query": {
        "id": {
            "$gt": 10
        }
    }
}
```

### makeMongoAggregate(sqlStatement)

Generates a mongo aggregate.

```js
const SQLMongoParser=require('@synatic/sql-to-mongo');
console.log(JSON.stringify(SQLMongoParser.makeMongoAggregate("select id from `films` group by id"), null, 4));

{
    "pipeline": [
        {
            "$group": {
                "_id": {
                    "id": "$id"
                }
            }
        },
        {
            "$project": {
                "id": "$_id.id",
                "_id": 0
            }
        }
    ],
    "collections": [
        "films"
    ]
}
```

### canQuery

Returns whether a statement can be queried or an aggregate must be used.

```js
const SQLMongoParser = require('@synatic/sql-to-mongo');

console.log(SQLMongoParser.canQuery('select id from `films`'));
//true

console.log(SQLMongoParser.canQuery('select id from `films` group by id'));
//false
```

### parseSQLtoAST(sqlStatement)

Parses a SQL statement to an AST (abstract syntax tree)

```js
const SQLMongoParser=require('@synatic/sql-to-mongo');

const ast=SQLMongoParser.parseSQLtoAST("select id from `films`");
console.log(JSON.stringify(ast, null, 4));

{
    "tableList": [
        "select::null::films"
    ],
    "columnList": [
        "select::null::id"
    ],
    "ast": {
        "with": null,
        "type": "select",
        "options": null,
        "distinct": null,
        "columns": [
            {
                "expr": {
                    "type": "column_ref",
                    "table": null,
                    "column": "id"
                },
                "as": null
            }
        ],
        "from": [
            {
                "db": null,
                "table": "films",
                "as": null
            }
        ],
        "where": null,
        "groupby": null,
        "having": null,
        "orderby": null,
        "limit": null,
        "for_update": null
    }
}
```

## M-SQL

Requires as for functions and sub queries

```sql
select abs(-1) as `absId` from `customers`
```

as on table requires prefixing

```sql
select c.* from customers as c
```

Always prefix on joins

## Supported SQL Statements

Functions in WHERE statements require explicit definition and can't use a computed field

```sql
--Correct
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by absId

--Won't work
select `Address.City` as City from `customers` where `First Name` like 'm%' and absId > 1
```

ORDER BY requires field to be part of select

```sql
--Correct
select `Address.City` as City,abs(`id`) as absId from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by absId

--Won't work
select `Address.City` as City from `customers` where `First Name` like 'm%' and abs(`id`) > 1 order by abs(`id`)
```

### Limit and Offset

Supports MySQL style limits and offset that equates to limit and skip

```sql
select (select * from Rentals) as t from `customers` limit 10 offset 2
```

### Merge Fields into Object

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

### Group By and Having

Coming soon

### Joins

Support INNER and OUTER Joins but does not unwind by default and the items are added as an array field.

There are several join hints to support automatically unwinding of joins:

-   first
-   last
-   unwind

```sql
--return the first item in the array
select * from orders inner join `inventory|first` as inventory_docs on sku=item

--take the last item of the array
select * from orders inner join `inventory|last` as inventory_docs on sku=item

--unwind the array to multiple documents
select * from orders inner join `inventory|unwind` as inventory_docs on sku=item
```

Alternatively the explicit array functions can be used:

```sql
--return the first item in the array
select *,FIRST_IN_ARRAY(inventory) as inventory_docs from orders inner join `inventory` on sku=item

--take the last item of the array
select *,LAST_IN_ARRAY(inventory) as inventory_docs from orders inner join `inventory` on sku=item

--unwind the array to multiple documents
select *,UNWIND(inventory) as inventory_docs from orders inner join `inventory` on sku=item
```

**NOTE**: _In sub select on where statement does not work as a join_

```sql
--Won't Work (from MongoDB docs)
SELECT *, inventory_docs
FROM orders
WHERE inventory_docs IN (SELECT *
FROM inventory
WHERE sku= orders.item)

--use join instead
select *
from orders
inner join inventory inventory_docs
on sku=item
```

### Sub Queries

### Column Functions

Methods that perform operations on columns/fields

| M-SQL Function                | Description                                                                         | Example                                                   |
| ----------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------- |
| FIELD_EXISTS(expr,true/false) | Check if a field exists. Can only be used in where clauses and not as an expression | `` select * from `films` where FIELD_EXISTS(`id`,true) `` |

### Comparison Operators

| M-SQL Operator | Description                                       | Example                                                                                               |
| -------------- | ------------------------------------------------- | ----------------------------------------------------------------------------------------------------- |
| &gt;           | Greater than                                      | <code>select \* from \`films\` where id > 10<br>select (id>10) as exprVal from \`films\` </code>      |
| &lt;           | Less than                                         | <code>select \* from \`films\` where id < 10<br>select (id<10) as exprVal from \`films\` </code>      |
| =              | Equal to                                          | <code>select \* from \`films\` where id = 10 <br>select (id=10) as exprVal from \`films\`</code>      |
| >=             | Greater than or equal to                          | <code>select \* from \`films\` where id >= 10<br>select (id>=10) as exprVal from \`films\` </code>    |
| <=             | Less than or equal to                             | <code>select \* from \`films\` where id <= 10<br>select (id<=10) as exprVal from \`films\` </code>    |
| !=             | Not equal to                                      | <code>select \* from \`films\` where id != 10 <br>select (id!=10) as exprVal from \`films\`</code>    |
| IS NOT NULL    | Is not null                                       | <code>select \* from \`films\` where id IS NOT NULL </code>                                           |
| LIKE           | String like, support standard %, case insensitive | <code>select \`First Name\` as FName from \`customers\` where \`First Name\` Like 'M%' </code>        |
| GT             | Greater than                                      | <code>select GT(id,10) as exprVal from \`films\` </code>                                              |
| LT             | Less than                                         | <code>select LT(id,10) as exprVal from \`films\` </code>                                              |
| EQ             | Equal to                                          | <code>select EQ(id,10) as exprVal from \`films\`</code>                                               |
| GTE            | Greater than or equal to                          | <code>select GTE(id,10) as exprVal from \`films\` </code>                                             |
| LTE            | Less than or equal to                             | <code>select LTE(id,10) as exprVal from \`films\` </code>                                             |
| NE             | Not equal to                                      | <code>select NE(id,10) as exprVal from \`films\`</code>                                               |
| IN             | In a list of values                               | <code>select \* from \`customers\` where \`Address.City\` in ('Japan','Pakistan')</code>              |
| CASE           | Case statement                                    | <code>select id,(case when id=3 then '1' when id=2 then '1' else 0 end) as test from customers</code> |

### Aggregate Functions

| Aggregate Function | Example                                                                                                                    |
| ------------------ | -------------------------------------------------------------------------------------------------------------------------- |
| SUM                | `` select sum(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| AVG                | `` select avg(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| MIN                | `` select min(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| MAX                | `` select max(`id`) as aggrVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |
| COUNT              | `` select count(*) as countVal,`Address.City` as City from `customers` group by `Address.City` order by `Address.City`  `` |

#### SUM CASE

Sum Case logic is supported:

```sql
select sum(case when `Address.City`='Ueda' then 1 else 0 end) as Ueda,sum(case when `Address.City`='Tete' then 1 else 0 end) as Tete from `customers` group by `xxx`
```

### Arrays

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
| MIN(array expr)                               | returns the minimum of a value array e.g. [1,2,3]                                                                                                                                                             | `` select MIN((select filmId as `$$ROOT` from Rentals)) as s from customers  ``                                                                 |
| MAX(array expr)                               | returns the maximum of a value array e.g. [1,2,3]                                                                                                                                                             | `` select MAX((select filmId as `$$ROOT` from Rentals)) as s from customers  ``                                                                 |
| SUM(array expr)                               | returns the sum of a value array e.g. [1,2,3]                                                                                                                                                                 | `` select SUM((select filmId as `$$ROOT` from Rentals)) as s from customers  ``                                                                 |
| AVG(array expr)                               | returns the average of a value array e.g. [1,2,3]                                                                                                                                                             | `` select AVG((select filmId as `$$ROOT` from Rentals)) as s from customers  ``                                                                 |

#### Unwind

Coming soon

### Objects

Methods that perform operations on objects

| M-SQL Function      | Description            | Example                                                                                      |
| ------------------- | ---------------------- | -------------------------------------------------------------------------------------------- |
| PARSE_JSON(expr)    | Parses the JSON string | `` select id,ARRAY_TO_OBJECT(PARSE_JSON('[{"k":"val","v":1}]')) as test from `customers`  `` |
| MERGE_OBJECTS(expr) | Merges objects         | `` select id,MERGE_OBJECTS(`Address`,PARSE_JSON('{"val":1}')) as test from `customers`  ``   |
|                     | With Sub Select        | `` select id,MERGE_OBJECTS(`Address`,(select 1 as val)) as test from `customers`  ``         |

#### $$ROOT

Specifying '$$ROOT' as a column alias sets the value to root object but only works with aggregates (unless contained in array sub select)

```sql
select t as `$$ROOT` from (select id,`First Name`,`Last Name`,lengthOfArray(Rentals,'id')  as numRentals from customers) as t
```

### Mathematical Functions

| M-SQL Function           | Description                                                                                                                                                                 | Example                                                                |
| ------------------------ | --------------------------------------------------------------------------------------------------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| AVG(expr,expr,...)       | Returns the average of a set of values.                                                                                                                                     | `` select AVG(`Replacement Cost`,`id`) as exprVal from `films`  ``     |
| ABS(expr)                | Returns the absolute value of a number.                                                                                                                                     | `` select ABS(`Replacement Cost`) as exprVal from `films`  ``          |
| ACOS(expr)               | Returns the inverse cosine (arc cosine) of a value.                                                                                                                         | `` select ACOS(`Replacement Cost`) as exprVal from `films`  ``         |
| ACOSH(expr)              | Returns the inverse hyperbolic cosine (hyperbolic arc cosine) of a value.                                                                                                   | `` select ACOSH(`Replacement Cost`) as exprVal from `films`  ``        |
| ASIN(expr)               | Returns the inverse sine (arc sine) of a value.                                                                                                                             | `` select ASIN(`Replacement Cost`) as exprVal from `films`  ``         |
| ASINH(expr)              | Returns the inverse hyperbolic sine (hyperbolic arc sine) of a value.                                                                                                       | `` select ASINH(`Replacement Cost`) as exprVal from `films`  ``        |
| ATAN(expr)               | Returns the inverse tangent (arc tangent) of a value.                                                                                                                       | `` select ATAN(`Replacement Cost`) as exprVal from `films`  ``         |
| ATAN2(y,x)               | Returns the inverse tangent (arc tangent) of y / x, where y and x are the first and second values passed to the expression respectively.                                    | `` select ATAN2(3,4) as exprVal from `films`  ``                       |
| ATANH(expr)              | Returns the inverse hyperbolic tangent (hyperbolic arc tangent) of a value.                                                                                                 | `` select ATANH(`Replacement Cost`) as exprVal from `films`  ``        |
| BINARY_SIZE(expr)        | Returns the byte size of the expression.                                                                                                                                    | `` select id,BINARY_SIZE(`First Name`) as exprVal from `customers`  `` |
| CEIL(expr)               | Returns the smallest integer greater than or equal to the specified number.                                                                                                 | `` select CEIL(`Replacement Cost`, 1) as exprVal from `films`  ``      |
| DEGREES_TO_RADIANS(expr) | Converts an input value measured in degrees to radians.                                                                                                                     | `` select DEGREES_TO_RADIANS(300) as exprVal from `films`  ``          |
| DIVIDE(expr,expr)        | Divides one number by another and returns the result.                                                                                                                       | `` select DIVIDE(`Replacement Cost`,10) as exprVal from `films`  ``    |
| EXP(expr)                | Raises Euler's number (i.e. e ) to the specified exponent and returns the result.                                                                                           | `` select EXP(`Replacement Cost`, 1) as exprVal from `films`  ``       |
| FLOOR(expr)              | Returns the largest integer less than or equal to the specified number.                                                                                                     | `` select FLOOR(`Replacement Cost`) as exprVal from `films`  ``        |
| LN(expr)                 | Calculates the natural logarithm ln (i.e log e) of a number and returns the result as a double.                                                                             | `` select LN(`id`) as exprVal from `films`  ``                         |
| LOG(number,base)         | Calculates the log of a number in the specified base and returns the result as a double.                                                                                    | `` select LOG(`id`,10) as exprVal from `films`  ``                     |
| LOG10(expr)              | Calculates the log base 10 of a number and returns the result as a double.                                                                                                  | `` select LOG10(`id`) as exprVal from `films`  ``                      |
| MAX(expr,expr,...)       | Returns the max of a set of numbers.                                                                                                                                        | `` select MAX(`id`,10) as exprVal from `films`  ``                     |
| MIN(xpr,expr,...)        | Returns the min of a set of numbers.                                                                                                                                        | `` select MIN(`id`,10) as exprVal from `films`  ``                     |
| MOD(expr,expr)           | Divides one number by another and returns the remainder.                                                                                                                    | `` select MOD(`id`,10) as exprVal from `films`  ``                     |
| MULTIPLY(expr,expr,...)  | Multiplies numbers together and returns the result                                                                                                                          | `` select MULTIPLY(`id`,10) as exprVal from `films`  ``                |
| POW(expr,exponent)       | Raises a number to the specified exponent and returns the result.                                                                                                           | `` select POW(`id`,10) as exprVal from `films`  ``                     |
| RADIANS_TO_DEGREES(expr) | Converts an input value measured in radians to degrees.                                                                                                                     | `` select RADIANS_TO_DEGREES(0.5) as exprVal from `films`  ``          |
| RAND(expr)               | Returns a random float between 0 and 1 each time it is called.                                                                                                              | `` select RAND() as exprVal from `films`  ``                           |
| ROUND(expr,[places])     | rounds a number to a whole integer or to a specified decimal place.                                                                                                         | `` select ROUND(`Replacement Cost`,1) as exprVal from `films`  ``      |
| SIN(expr)                | Returns the sine of a value that is measured in radians.                                                                                                                    | `` select SIN(90) as exprVal from `films`  ``                          |
| SINH(expr)               | Returns the hyperbolic sine of a value that is measured in radians.                                                                                                         | `` select SINH(90) as exprVal from `films`  ``                         |
| SQRT(expr)               | Calculates the square root of a positive number and returns the result as a double.                                                                                         | `` select SQRT(`id`) as exprVal from `films`  ``                       |
| SUBTRACT(expr,expr)      | Subtracts two numbers to return the difference, or two dates to return the difference in milliseconds, or a date and a number in milliseconds to return the resulting date. | `` select SUBTRACT(10,`id`) as exprVal from `films`  ``                |
| SUM(expr,expr,...)       | Sums the values provided in the expression                                                                                                                                  | `` select SUM(`Replacement Cost`,2,id) as s from `films`  ``           |
| TAN(expr)                | Returns the tangent of a value that is measured in radians.                                                                                                                 | `` select TAN(90) as exprVal from `films`  ``                          |
| TANH(expr)               | Returns the hyperbolic tangent of a value that is measured in radians.                                                                                                      | `` select TANH(90) as exprVal from `films`  ``                         |
| TRUNC(expr,[places])     | Truncates a number to a whole integer or to a specified decimal place                                                                                                       | `` select TRUNC(`Replacement Cost`, 1) as exprVal from `films`  ``     |

### Mathematical Operators

| M-SQL Operator | Description                                                                     | Example                                                                |
| -------------- | ------------------------------------------------------------------------------- | ---------------------------------------------------------------------- |
| +              | Addition operator adds 2 numbers or dates. Does not work on strings             | `` select `Replacement Cost` + id + Length as exprVal from `films`  `` |
| -              | Subtraction operator subtracts 2 numbers or dates. Does not work on strings     | `` select `Replacement Cost` - id - Length as exprVal from `films`  `` |
| /              | Division operator divides 2 numbers or dates. Does not work on strings          | `` select `Replacement Cost` / id as exprVal from `films`  ``          |
| \*             | Multiplication operator multiplies 2 numbers or dates. Does not work on strings | `` select `Replacement Cost` * id * Length as exprVal from `films`  `` |
| %              | Modulus operator                                                                | `` select `id` % Length as exprVal from `films`  ``                    |

### Conversion Functions

| M-SQL Function    | Description                                                                                                       | Example                                                                                                           |
| ----------------- | ----------------------------------------------------------------------------------------------------------------- | ----------------------------------------------------------------------------------------------------------------- |
| CONVERT(expr,to)  | Converts the expression to a mongo type: 'double', 'string', 'bool', 'date', 'int', 'objectId', 'long', 'decimal' | `` select SUBTRACT(CONVERT('1','int'),ABS(`Replacement Cost`)) as d,Title from `films`  ``                        |
| TYPEOF(expr)      | returns the mongo type of the expression                                                                          | `` select TYPEOF(id) as `conv` from `customers`  ``                                                               |
| TO_DATE(expr)     | Convert the expression to a date                                                                                  | `` select TO_DATE('2021-12-15T00:00:00Z') as `conv` from `customers`  ``                                          |
| TO_STRING(expr)   | Convert the expression to a date                                                                                  | `` select TO_STRING(`id`) as `conv` from `customers`  ``                                                          |
| TO_INT(expr)      | Convert the expression to a date                                                                                  | `` select TO_INT('12345') as `conv` from `customers`  ``                                                          |
| TO_LONG(expr)     | Convert the expression to a date                                                                                  | `` select TO_LONG('1234567891') as `conv` from `customers`  ``                                                    |
| TO_BOOL(expr)     | Convert the expression to a date                                                                                  | `` select TO_BOOLE('true') as `conv` from `customers`  ``                                                         |
| TO_DECIMAL(expr)  | Convert the expression to a date                                                                                  | `` select TO_DECIMAL('123.35') as `conv` from `customers`  ``                                                     |
| TO_DOUBLE(expr)   | Convert the expression to a date                                                                                  | `` select TO_DOUBLE('123.35') as `conv` from `customers`  ``                                                      |
| IFNULL(expr,expr) | Return the value if the expression is null                                                                        | `` select IFNULL(id,1) as `conv` from `customers`  ``                                                             |
|                   | Example using select without from for object generation                                                           | <code>select IFNULL(NULL,(select 'a' as val,1 as num)) as `conv` from `customers` <br> {"val":"a","num":1}</code> |

Literal types in where statements using conversion functions are converted automatically e.g.:

```sql
select to_date(date) as d from customers where to_date(date) > to_date('2012-01-01')
```

Will automatically convert the date value and result in the following:

```
{"$expr":{"$gt":[{"$toDate":"$date"},new Date("2012-01-01T00:00:00.000Z")]}}
```

#### Cast

Supports cast operations with the following type mappings:

| MySQL Type | Mongo Type |
| ---------- | ---------- |
| VARCHAR    | string     |
| DECIMAL    | decimal    |
| INT        | int        |
| DATETIME   | date       |
| TIME       | date       |
| FLOAT      | number     |
| CHAR       | string     |
| NCHAR      | string     |

```sql
select cast(1+`id` as varchar) as `id` from `customers`
select cast(abs(-1) as varchar) as `id` from `customers`
select cast('2021-01-01T00:00:00Z' as date) as `id` from `customers`
```

### String Functions

| M-SQL Function                  | Description                                                                                   | Example                                                                        |
| ------------------------------- | --------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------ |
| CONCAT(expr,expr,expr...)       | Concatenates strings.                                                                         | `` select CONCAT(`First Name`,':',`Last Name`) as exprVal from `customers`  `` |
| TRIM(expr,[chars])              | Trims string.                                                                                 | `` select TRIM(`First Name`,'_ -') as exprVal from `customers`  ``             |
| LTRIM(expr,[chars])             | Left trim string.                                                                             | `` select LTRIM(`First Name`,'_ -') as exprVal from `customers`  ``            |
| RTRIM(expr,[chars])             | Right trim string.                                                                            | `` select RTRIM(`First Name`,'_ -') as exprVal from `customers`  ``            |
| SUBSTR(expr,start,length)       | Returns the substring of a string.                                                            | `` select SUBSTR(`First Name`,1,10) as exprVal from `customers`  ``            |
| SUBSTR_BYTES(expr,start,length) | Returns the substring of a string by bytes.                                                   | `` select SUBSTR(`First Name`,1,10) as exprVal from `customers`  ``            |
| REPLACE(expr,find,replace)      | Replaces the first instance of a search string in an input string with a replacement string.. | `` select REPLACE(`First Name`,'a','b') as exprVal from `customers`  ``        |
| REPLACE_ALL(expr,find,replace)  | Replaces all instances of a search string in an input string with a replacement string.       | `` select REPLACE_ALL(`First Name`,'a','b') as exprVal from `customers`  ``    |
| STRLEN(expr)                    | Returns the number of UTF-8 encoded bytes in the specified string.                            | `` select STRLEN(`First Name`) as exprVal from `customers`  ``                 |
| STRLEN_CP(expr)                 | Returns the number of UTF-8 code points in the specified string.                              | `` select STRLEN_CP(`First Name`) as exprVal from `customers`  ``              |
| SPLIT(expr,delimiter)           | Splits the string to an array                                                                 | `` select SPLIT(`First Name`,',') as exprVal from `customers`  ``              |

**Note**: _+ (str + str) does not work for string concatenation._

### Date Functions

| M-SQL Function                                                                                                            | Description                                                                                                                                                              | Example                                                                                                          |
| ------------------------------------------------------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------ | ---------------------------------------------------------------------------------------------------------------- |
| DATE_FROM_STRING(expr,format,timezone,onError,onNull)                                                                     | Converts a date/time string to a date object.                                                                                                                            | `` select DATE_FROM_STRING('2021-11-15T14:43:29.000Z',null,null) as exprVal from `customers`  ``                 |
| DATE_TO_STRING(expr,format,timezone,onNull)                                                                               | Converts a date object to a string according to a user-specified format.                                                                                                 | `` select DATE_TO_STRING(DATE_FROM_STRING('2021-11-15T14:43:29.000Z'),null,null) as exprVal from `customers`  `` |
| DATE_TO_PARTS(expr,timezone,iso8601)                                                                                      | Returns a document that contains the constituent parts of a given Date value.                                                                                            | `` select DATE_TO_PARTS(DATE_FROM_STRING('2021-11-15T14:43:29.000Z'),null,true) as exprVal from `customers`  ``  |
| DATE_FROM_PARTS(year,month,day,hour,second,minute,millisecond,timezone)                                                   | Constructs and returns a Date object given the date's constituent properties.                                                                                            | `` select DATE_FROM_PARTS(2021,11,15) as exprVal from `customers`  ``                                            |
| DATE_FROM_ISO_PARTS(isoWeekYear,isoWeek,isoDayOfWeek<br>&nbsp;&nbsp;&nbsp;&nbsp;,hour,second,minute,millisecond,timezone) | Constructs and returns a Date object given the date's constituent ISO properties.                                                                                        | `` select DATE_FROM_ISO_PARTS(2017,6,3) as exprVal from `customers`  ``                                          |
| DAY_OF_WEEK(expr)                                                                                                         | Returns the day of the week for a date as a number between 1 (Sunday) and 7 (Saturday).                                                                                  | `` select DAY_OF_WEEK(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                            |
| DAY_OF_YEAR(expr)                                                                                                         | Returns the day of the year for a date as a number between 1 and 366.                                                                                                    | `` select DAY_OF_YEAR(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                            |
| DAY_OF_MONTH(expr)                                                                                                        | Returns the day of the month for a date as a number between 1 and 31.                                                                                                    | `` select DAY_OF_MONTH(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                           |
| ISO_DAY_OF_WEEK(expr)                                                                                                     | Returns the weekday number in ISO 8601 format, ranging from 1 (for Monday) to 7 (for Sunday).                                                                            | `` select ISO_DAY_OF_WEEK(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                        |
| ISO_WEEK(expr)                                                                                                            | Returns the week number in ISO 8601 format, ranging from 1 to 53. Week numbers start at 1 with the week (Monday through Sunday) that contains the year's first Thursday. | `` select ISO_WEEK(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                               |
| ISO_WEEK_YEAR(expr)                                                                                                       | Returns the year number in ISO 8601 format. The year starts with the Monday of week 1 and ends with the Sunday of the last week.                                         | `` select ISO_WEEK_YEAR(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                          |
| HOUR(expr)                                                                                                                | Returns the hour portion of a date as a number between 0 and 23.                                                                                                         | `` select HOUR(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                                   |
| MILLISECOND(expr)                                                                                                         | Returns the millisecond portion of a date as an integer between 0 and 999.                                                                                               | `` select MILLISECOND(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                            |
| MINUTE(expr)                                                                                                              | Returns the minute portion of a date as a number between 0 and 59.                                                                                                       | `` select MINUTE(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                                 |
| MONTH(expr)                                                                                                               | Returns the month of a date as a number between 1 and 12.                                                                                                                | `` select MONTH(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                                  |
| SECOND(expr)                                                                                                              | Returns the second portion of a date as a number between 0 and 59, but can be 60 to account for leap seconds.                                                            | `` select SECOND(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                                 |
| WEEK(expr)                                                                                                                | Returns the week of the year for a date as a number between 0 and 53.                                                                                                    | `` select WEEK(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                                   |
| YEAR(expr)                                                                                                                | Returns the year portion of a date.                                                                                                                                      | `` select YEAR(DATE_FROM_STRING('2021-11-15')) as exprVal from `customers`  ``                                   |

### Selecting on a calculated column by name

Calculated columns in where statements can only be used with aggregates

```sql
--have to repeat select statemnt as with sql rules
select id,Title,Rating,abs(id) as absId from `films` where abs(id)=1
```
