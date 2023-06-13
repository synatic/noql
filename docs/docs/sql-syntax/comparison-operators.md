# Comparison Operators

| M-SQL Operator | Description                                       | Example                                                                                               |
|----------------|---------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| &gt;           | Greater than                                      | <code>select \* from \`films\` where id > 10<br>select (id>10) as exprVal from \`films\` </code>      |
| &lt;           | Less than                                         | <code>select \* from \`films\` where id < 10<br>select (id<10) as exprVal from \`films\` </code>      |
| =              | Equal to                                          | <code>select \* from \`films\` where id = 10 <br>select (id=10) as exprVal from \`films\`</code>      |
| >=             | Greater than or equal to                          | <code>select \* from \`films\` where id >= 10<br>select (id>=10) as exprVal from \`films\` </code>    |
| <=             | Less than or equal to                             | <code>select \* from \`films\` where id <= 10<br>select (id<=10) as exprVal from \`films\` </code>    |
| !=             | Not equal to                                      | <code>select \* from \`films\` where id != 10 <br>select (id!=10) as exprVal from \`films\`</code>    |
| IS NOT NULL    | Is not null                                       | <code>select \* from \`films\` where id IS NOT NULL </code>                                           |
| IS NULL        | Is null                                           | <code>select \* from \`films\` where id IS NULL </code>                                               |
| LIKE           | String like, support standard %, case insensitive | <code>select \`First Name\` as FName from \`customers\` where \`First Name\` Like 'M%' </code>        |
| GT             | Greater than                                      | <code>select GT(id,10) as exprVal from \`films\` </code>                                              |
| LT             | Less than                                         | <code>select LT(id,10) as exprVal from \`films\` </code>                                              |
| EQ             | Equal to                                          | <code>select EQ(id,10) as exprVal from \`films\`</code>                                               |
| GTE            | Greater than or equal to                          | <code>select GTE(id,10) as exprVal from \`films\` </code>                                             |
| LTE            | Less than or equal to                             | <code>select LTE(id,10) as exprVal from \`films\` </code>                                             |
| NE             | Not equal to                                      | <code>select NE(id,10) as exprVal from \`films\`</code>                                               |
| IN             | In a list of values                               | <code>select \* from \`customers\` where \`Address.City\` in ('Japan','Pakistan')</code>              |
| NOT IN         | Not in a list of values                           | <code>select \* from \`customers\` where \`Address.City\` NOT IN ('Japan','Pakistan')</code>          |
| CASE           | Case statement                                    | <code>select id,(case when id=3 then '1' when id=2 then '1' else 0 end) as test from customers</code> |
