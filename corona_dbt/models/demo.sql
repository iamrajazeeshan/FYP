{{
    config(
        materialized = "table",
        post_hook = [
            "ALTER TABLE {{ this }} ADD PRIMARY KEY (region_id)"
            ]
    )
}}
WITH cte_geography AS (
    SELECT
        whocovidst2020."RegionId"                      AS region_id,
        whocovidst2020."Region"                        AS region,
        whocovidst2020."Region Name"                   AS region_name,
        whocovidst2020."Region Notes"                  AS region_notes
    From covid19_data_atlas.covid19.whocovidst2020
   group by 1,2,3,4
)
SELECT
    cte_geography.region_id,
    cte_geography.region,
    cte_geography.region_name,
    cte_geography.region_notes
FROM
    cte_geography