{{config(
    materialized='table'
)}}

SELECT 
    EXTRACT(YEAR FROM datetime) AS year,
    EXTRACT(MONTH FROM datetime) AS month,
    COUNT(*)
FROM {{ref('stg_earthquake_data')}}
GROUP BY 1,2
ORDER BY 1 DESC,2 DESC
