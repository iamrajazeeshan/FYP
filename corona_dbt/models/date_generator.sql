{{
   config(
          materialized = 'incremental'
   )
}}

WITH cte_date_generator AS (
    SELECT
        DATEADD(day, '-' || SEQ4(), CURRENT_DATE()) AS date
        FROM
        TABLE
            (GENERATOR(ROWCOUNT => (8500))) --dim_dates will have 8500 rows, with max(date_day) always being the current date
)
SELECT  
        Date                                                                               AS Date,
        DAY(cte_date_generator.Date)                                                     AS day,
        WEEK(cte_date_generator.Date) :: integer                                         AS week,
        DAYOFWEEK(cte_date_generator.Date ):: integer                                    AS day_of_week,
        MONTH(cte_date_generator.Date):: integer                                         AS month_number,
        DECODE(EXTRACT('Month',cte_date_generator.Date),1, 'January', 2, 'February', 3, 'March', 4, 'April', 5, 'May', 6, 'June', 7, 'July', 8, 'August', 9, 'September', 10, 'October', 11, 'November', 12, 'December') AS  month_name,
        QUARTER( cte_date_generator.Date) :: integer                                     AS quater,
        EXTRACT(YEAR FROM cte_date_generator.Date )::integer                             AS year
FROM cte_date_generator
{% if is_incremental() %}
where date > (select max(date) from {{ this }})
{% endif %}
ORDER BY 1