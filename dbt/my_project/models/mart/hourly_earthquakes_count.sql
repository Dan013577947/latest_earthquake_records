{{config(
    materialized='table'
)}}


SELECT 
    CAST(datetime AS DATE) AS date,
    EXTRACT(HOUR FROM datetime) AS hour,
    COUNT(*) AS hourly_frequency
FROM {{ref('stg_earthquake_data')}}
GROUP BY 1,2
ORDER BY 1 DESC,2 DESC