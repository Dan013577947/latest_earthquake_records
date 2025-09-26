{{config(
    materialized='table'
)}}

WITH source AS(SELECT
    CAST(datetime as DATE) AS date,
    COUNT(*) AS quake_count,
    ROUND(SUM(magnitude::NUMERIC),2) AS sum_magnitude
FROM {{ref('stg_earthquake_data')}}
GROUP BY 1
)
SELECT 
    *,
    SUM(quake_count) 
        OVER(
            ORDER BY date DESC 
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        ) AS quake_count_7days,
    ROUND(SUM(sum_magnitude)
        OVER(
            ORDER BY date DESC
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )/ NULLIF(
            SUM(quake_count) 
            OVER(
                ORDER BY date DESC 
                ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
            ),0 
        ),2) AS avg_mag_7days
FROM source
ORDER BY date DESC
