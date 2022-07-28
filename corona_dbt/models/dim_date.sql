{{
    config(
        materialized = "table",
        post_hook = [
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (Date)"
            ]
    )
}}
WITH cte_date AS (
    SELECT
        whocovidst2020."Date"                                                          AS date,
        DAY(whocovidst2020."Date")                                                     AS day,
        WEEK(whocovidst2020."Date") :: integer                                         AS week,
        DAYOFWEEK(whocovidst2020."Date" ):: integer                                    AS day_of_week,
        MONTH(whocovidst2020."Date"):: integer                                         AS month_number,
        DECODE(EXTRACT('Month',whocovidst2020."Date"),1, 'January', 2, 'February', 3, 'March', 4, 'April', 5, 'May', 6, 'June', 7, 'July', 8, 'August', 9, 'September', 10, 'October', 11, 'November', 12, 'December') AS  month_name,
        QUARTER( whocovidst2020."Date") :: integer                                     AS quater,
        EXTRACT(YEAR FROM whocovidst2020."Date" )::integer                             AS year
from covid19_data_atlas.covid19.whocovidst2020
LEFT JOIN analytics.date_generator ON dim_date.date = date_generator.date_day
WHERE date_generator.date_day BETWEEN '2020-01-03' and CURRENT_DATE
group by 1
    --Group by 1,2,3
    --QUALIFY row_number() OVER (PARTITION BY date ORDER BY date) = 1
)
SELECT
    cte_date.date,
    cte_date.day,
    cte_date.week,
    cte_date.day_of_week,
    cte_date.month_number,
    cte_date.month_name,
    cte_date.quater,
    cte_date.year
FROM
    cte_date