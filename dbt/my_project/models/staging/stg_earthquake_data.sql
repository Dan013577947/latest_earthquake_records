{{config(
    materialized="table",
    unique_key="id"
)}}

SELECT 
    *,
    CASE 
        WHEN depth_km <= 70 THEN 'shallow'
        WHEN depth_km BETWEEN 71 AND 300 THEN 'intermediate'
        ELSE 'deep'
    END AS depth_type,
    CASE 
        WHEN magnitude <= 4.9 THEN 'minor'
        WHEN magnitude BETWEEN 5 AND 5.9 THEN 'moderate'
        WHEN magnitude BETWEEN 6 AND 6.9 THEN 'strong'
        ELSE 'major'
    END AS magnitude_type
FROM {{source('dev','raw_earthquake_records')}}
