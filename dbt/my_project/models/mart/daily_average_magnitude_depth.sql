{{config(
    materialized='table'
)}}

SELECT 
     CAST(datetime AS DATE) AS date,
     ROUND(AVG(magnitude::NUMERIC),2) AS avg_mag,
     ROUND(AVG(depth_km::NUMERIC),2) AS av_depth
FROM{{ref('stg_earthquake_data')}}
GROUP BY 1
ORDER BY 1 DESC

