const SQLParser=require('./lib/SQLParser');

let parsedVal=SQLParser.parseSQLtoAST("select a,b,unwind(c) as c,c.a,c.b from `table` as t1  inner join `table2` as t2  on a=b ")
let x;

