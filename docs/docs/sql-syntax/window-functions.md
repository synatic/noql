# `Window Functions`

Window functions applies aggregate and ranking functions over a particular window (set of rows).

## RANK

The `RANK()` function assigns a rank to every row within a partition of a result set.

For each partition, the rank of the first row is 1. The `RANK()` function adds the number of tied rows to the tied rank to calculate the rank of the next row, so the ranks may not be sequential. In addition, rows with the same values will get the same rank.

???+ success "Correct `RANK` usage"

    ```sql
    --Correct
    SELECT  value,
            RANK () OVER (
                ORDER BY value
            ) rank_number,
            unset(_id)
    FROM function-test-data
    WHERE testId='bugfix.rank.case1'
    ```

## ROW_NUMBER

The `ROW_NUMBER()` function is a window function that assigns a sequential integer to each row in a result set.

???+ success "Correct `ROW_NUMBER` usage"

    ```sql
    --Correct
    SELECT  value,
            ROW_NUMBER() OVER (
                ORDER BY value
            ) row_number,
            unset(_id)
    FROM function-test-data
    WHERE testId='bugfix.rank.case1'
    ```

## DENSE_RANK

The `DENSE_RANK()` assigns a rank to every row in each partition of a result set. Different from the `RANK()` function, the `DENSE_RANK()` function always returns consecutive rank values.

For each partition, the `DENSE_RANK()` function returns the same rank for the rows which have the same values

???+ success "Correct `DENSE_RANK` usage"

    ```sql
    --Correct
    SELECT  value,
            DENSE_RANK () OVER (
                ORDER BY value
            ) rank_number,
            unset(_id)
    FROM function-test-data
    WHERE testId='bugfix.dense-rank.case1'
    ```

## NTILE

Currently not supported, [Blocking Issue](https://github.com/taozhi8833998/node-sql-parser/issues/1473)
