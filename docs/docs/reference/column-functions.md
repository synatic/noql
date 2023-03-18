# Column Functions

Supported methods that perform operations on columns/fields

| M-SQL Function                | Description                                                                         | Example                                                   |
| ----------------------------- | ----------------------------------------------------------------------------------- | --------------------------------------------------------- |
| FIELD_EXISTS(expr,true/false) | Check if a field exists. Can only be used in where clauses and not as an expression | `` select * from `films` where FIELD_EXISTS(`id`,true) `` |
