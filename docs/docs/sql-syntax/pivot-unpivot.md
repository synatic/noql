# Pivot and Unpivot Operations

## PIVOT

The `PIVOT` operation allows you to transform rows into columns, creating a cross-tabular format. In NoQL, the PIVOT operation is specified as a hint on the subquery.

???+ example "Example `PIVOT` usage"

    ```sql
    SELECT 'AverageCost' as CostSortedByProductionDays,
            "0",
            "1",
            "2",
            "3",
            "4"
    FROM (
        SELECT DaysToManufacture,
               StandardCost
        FROM Production_Product
        GROUP BY DaysToManufacture, StandardCost
        ORDER BY DaysToManufacture, StandardCost
    ) 'pvt|pivot([avg(StandardCost) as AverageCost],DaysToManufacture,[0,1,2,3,4])'
    ```

The PIVOT hint has the following format:
`'pvt|pivot([aggregate_function(column) as alias], pivot_column, [pivot_values])'`

-   `aggregate_function(column) as alias`: Specifies the aggregation to be performed and the alias for the result.
-   `pivot_column`: The column whose values will become new columns.
-   `pivot_values`: An array of values from the pivot column that will become new columns.

## UNPIVOT

The `UNPIVOT` operation is the reverse of `PIVOT`. It transforms columns into rows, converting a cross-tabular format back into a normalized form. In NoQL, the UNPIVOT operation is also specified as a hint on the subquery.

???+ example "Example `UNPIVOT` usage"

    ```sql
    SELECT VendorID, Employee, Orders
    FROM (
        SELECT VendorID, Emp1, Emp2, Emp3, Emp4, Emp5, unset(_id)
        FROM pvt
    ) 'unpvt|unpivot(Orders,Employee,[Emp1, Emp2, Emp3, Emp4, Emp5])'
    ORDER BY VendorID, Employee
    ```

The UNPIVOT hint has the following format:
`'unpvt|unpivot(value_column, name_column, [column_list])'`

-   `value_column`: The name of the new column that will contain the unpivoted values.
-   `name_column`: The name of the new column that will contain the names of the original columns.
-   `column_list`: An array of column names to be unpivoted.

Multiple UNPIVOT operations can be chained using the pipe (`|`) character:

???+ example "Example of multiple `UNPIVOT` operations"

    ```sql
    SELECT SalesID,
    ROW_NUMBER() OVER (
            ORDER BY OrderName
        ) OrderNum,
    OrderName,
    OrderDate,
    OrderAmt
    FROM (
        SELECT SalesID, Order1Name, Order2Name, Order1Date, Order2Date, Order1Amt, Order2Amt, unset(_id)
        FROM multiple-unpivot
    ) 'unpvt|unpivot(OrderName,OrderNames,[Order1Name, Order2Name])|unpivot(OrderDate,OrderDates,[Order1Date, Order2Date])|unpivot(OrderAmt,OrderAmts,[Order1Amt, Order2Amt])'
    ```

Note: The exact capabilities and performance of `PIVOT` and `UNPIVOT` operations may depend on the specific implementation in NoQL and the underlying MongoDB version.
