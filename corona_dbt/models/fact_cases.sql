{{
    config(
        materialized = "table",
        post_hook = [
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (region_id,date_key)"
            ]
    )
}}
WITH cte_fact_cases AS (
    SELECT
        date_generator.date                                                               AS date_key,
        dim_geography.region_id                                                     AS region_id,
        SUM(
            CASE
                WHEN whocovidst2020."Indicator Name" = 'Cumulative Deaths' THEN "Value"
            END
        )                                                                               AS cumulative_deaths,
        SUM(
            CASE
                WHEN whocovidst2020."Indicator Name" = 'Cumulative Confirmed' THEN "Value"
            END
        )                                                                               AS cumulative_confirmed,
        SUM(
            CASE
                WHEN whocovidst2020."Indicator Name" = 'Deaths' THEN "Value"
            END
            )                                                                           AS deaths, 
        SUM(
            CASE
                WHEN whocovidst2020."Indicator Name" = 'Confirmed' THEN "Value"
            END
            )                                                                           AS cases                                            
    FROM
        covid19_data_atlas.covid19.whocovidst2020

    LEFT JOIN analytics.date_generator ON date_generator.date = whocovidst2020."Date"
    INNER JOIN analytics.dim_geography ON dim_geography.region_id = whocovidst2020."RegionId"
   GROUP BY 1,2
)
SELECT 
    cte_fact_cases.date_key,
    cte_fact_cases.region_id,
    cte_fact_cases.cumulative_deaths,
    cte_fact_cases.cumulative_confirmed,
    cte_fact_cases.deaths,
    cte_fact_cases.cases
FROM 
    cte_fact_cases