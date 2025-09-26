{{config(
    materialized='table'
)}}
WITH source AS(SELECT 
    REGEXP_REPLACE(location, '.*\((.*)\).*', '\1') AS province,
    COUNT(*) AS count_per_loc
FROM {{ref('stg_earthquake_data')}}
GROUP BY 1
ORDER BY 2,1 DESC)
SELECT 
    *,
    RANK() OVER(ORDER BY count_per_loc DESC) AS rank 
FROM source


