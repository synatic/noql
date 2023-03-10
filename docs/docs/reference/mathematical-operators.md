# Mathematical Operators

| M-SQL Operator | Description                                                                     | 
| -------------- | ------------------------------------------------------------------------------- | 
| +              | Addition operator adds 2 numbers or dates. Does not work on strings <br/><br/> ```#!sql select `Replacement Cost` + id + Length as exprVal from `films` ``` |
| -              | Subtraction operator subtracts 2 numbers or dates. Does not work on strings <br/><br/> ```#!sql select `Replacement Cost` - id - Length as exprVal from `films` ``` |
| /              | Division operator divides 2 numbers or dates. Does not work on strings <br/><br/> ```#!sql select `Replacement Cost` / id as exprVal from `films` ```          |
| \*             | Multiplication operator multiplies 2 numbers or dates. Does not work on strings <br/><br/> ```#!sql select `Replacement Cost` *id* Length as exprVal from `films` ``` |
| %              | Modulus operator <br/><br/> ```#!sql select `id` % Length as exprVal from `films` ```                    |
