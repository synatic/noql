NoQL supports UNION and UNION ALL.

???+ example "Example `UNION` usage"

    ```sql
    SELECT * FROM top_rated_films
    UNION
    SELECT * FROM most_popular_films;
    ```

???+ example "Example `UNION ALL` usage"

    ```sql
    SELECT * FROM top_rated_films
    UNION ALL
    SELECT * FROM most_popular_films;
    ```

Unions can be used in sub queries.

