{{config(
    materialized='table'
)}}


SELECT 
    CAST(datetime AS DATE) AS date,
    COUNT(*) AS dailyFrequency
FROM {{ref('stg_earthquake_data')}}
GROUP BY 1
ORDER BY 1 DESC