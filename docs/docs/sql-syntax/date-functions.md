# Date Functions

## Supported Date Functions

### CURRENT_DATE

`#!sql CURRENT_DATE()`

Returns the current date

!!! example

    ```sql
    SELECT 
        CURRENT_DATE() AS exprVal 
    FROM `customers`
    ```

### DATE_FROM_ISO_PARTS

`#!sql
DATE_FROM_ISO_PARTS(isoWeekYear, isoWeek, isoDayOfWeek, hour,
                     second, minute, millisecond, timezone)
`

Constructs and returns a Date object given the date’s constituent ISO properties.

!!! example

    ```sql
    SELECT 
        DATE_FROM_ISO_PARTS(2017, 6, 3, null, null, null, null, null) AS exprVal 
    FROM `customers`
    ```

### DATE_FROM_PARTS

`#!sql DATE_FROM_PARTS(year, month, day, hour, second, minute, millisecond, timezone)`

Constructs and returns a Date object given the date’s constituent properties.

!!! example

    ```sql
    SELECT 
        DATE_FROM_PARTS(2021, 11, 15, null, null, null, null, null) AS exprVal 
    FROM `customers`
    ```

### DATE_FROM_STRING

`#!sql DATE_FROM_STRING(expr, format, timezone, onError, onNull)`

Converts a date/time string to a date object.

!!! example

    ```sql
    SELECT 
        DATE_FROM_STRING('2021-11-15T14:43:29.000Z', null, null) AS exprVal 
    FROM `customers`
    ```

### DATE_TO_PARTS

`#!sql DATE_TO_PARTS(expr, timezone, iso8601)`

Returns a document that contains the constituent parts of a given Date value.

!!! example

    ```sql
    SELECT 
        DATE_TO_PARTS(DATE_FROM_STRING('2021-11-15T14:43:29.000Z'), null, true) AS exprVal 
    FROM `customers`
    ```

### DATE_TO_STRING

`#!sql DATE_TO_STRING(expr, format, timezone, onNull)`

Converts a date object to a string according to a user-specified format.

!!! example

    ```sql
    SELECT 
        DATE_TO_STRING(DATE_FROM_STRING('2021-11-15T14:43:29.000Z'), null, null) AS exprVal
    FROM `customers`
    ```

### DAY_OF_MONTH

`#!sql DAY_OF_MONTH(expr)`

Returns the day of the month for a date as a number between 1 and 31.

!!! example

    ```sql
    SELECT 
        DAY_OF_MONTH(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### DAY_OF_WEEK

`#!sql DAY_OF_WEEK(expr)`

Returns the day of the week for a date as a number between 1 (Sunday) and 7 (Saturday).

!!! example

    ```sql
    SELECT 
        DAY_OF_WEEK(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```


### DAY_OF_YEAR

`#!sql DAY_OF_YEAR(expr)`

Returns the day of the year for a date as a number between 1 and 366.

!!! example

    ```sql
    SELECT 
        DAY_OF_YEAR(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### HOUR

`#!sql HOUR(expr)`

Returns the hour portion of a date as a number between 0 and 23.

!!! example

    ```sql
    SELECT 
        HOUR(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```
### ISO_DAY_OF_WEEK

`#!sql ISO_DAY_OF_WEEK(expr)`

Returns the weekday number in ISO 8601 format, ranging from 1 (for Monday) to 7 (for Sunday).

!!! example

    ```sql
    SELECT 
        ISO_DAY_OF_WEEK(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```
### ISO_WEEK

`#!sql ISO_WEEK(expr)`

Returns the week number in ISO 8601 format, ranging from 1 to 53. Week numbers start at 1 with the week (Monday through Sunday) that contains the year’s first Thursday.


!!! example

    ```sql
    SELECT 
        ISO_WEEK(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### ISO_WEEK_YEAR

`#!sql ISO_WEEK_YEAR(expr)`

Returns the year number in ISO 8601 format. The year starts with the Monday of week 1 and ends with the Sunday of the last week.

!!! example
    ```sql
    SELECT 
        ISO_WEEK_YEAR(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```


### MILLISECOND

`#!sql MILLISECOND(expr)`

Returns the millisecond portion of a date as an integer between 0 and 999.

!!! example

    ```sql
    SELECT 
        MILLISECOND(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### MINUTE

`#!sql MINUTE(expr)`

Returns the minute portion of a date as a number between 0 and 59.

!!! example

    ```sql
    SELECT 
        MINUTE(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```
### MONTH

`#!sql MONTH(expr)`

Returns the month of a date as a number between 1 and 12.

!!! example

    ```sql
    SELECT 
        MONTH(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### SECOND

`#!sql SECOND(expr)`

Returns the second portion of a date as a number between 0 and 59, but can be 60 to account for leap seconds.

!!! example

    ```sql
    SELECT 
        SECOND(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### WEEK

`#!sql WEEK(expr)`

Returns the week of the year for a date as a number between 0 and 53.

!!! example

    ```sql
    SELECT 
        WEEK(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### YEAR

`#!sql YEAR(expr)`

Returns the year portion of a date.

!!! example

    ```sql
    SELECT 
        YEAR(DATE_FROM_STRING('2021-11-15')) AS exprVal 
    FROM `customers`
    ```

### EXTRACT

`#!sql EXTRACT(period from expr)`

Extracts a portion of the date as per Postgres standard. Supported time periods: year, month, day, hour, minute, second, milliseconds, week, dow

!!! example

    ```sql
    SELECT 
        EXTRACT(year from orderDate) AS year
        ,EXTRACT(month from orderDate) AS month
        ,EXTRACT(day from TO_DATE('2021-10-23')) AS day 
    FROM 
      orders"
    ```
