-- =========================================================
-- Context: Consumption Layer - Aggregates
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Hourly City-Level Aggregation
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE agg_city_fact_hour_level
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE  = transform_wh
AS
WITH step01_city_level_data AS (
    SELECT 
        d.measurement_time,
        l.country,
        l.state,
        l.city,
        AVG(pm10_avg) AS pm10_avg,
        AVG(pm25_avg) AS pm25_avg,
        AVG(so2_avg)  AS so2_avg,
        AVG(no2_avg)  AS no2_avg,
        AVG(nh3_avg)  AS nh3_avg,
        AVG(co_avg)   AS co_avg,
        AVG(o3_avg)   AS o3_avg
    FROM air_quality_fact f
    JOIN date_dim d     ON f.date_fk     = d.date_pk
    JOIN location_dim l ON f.location_fk = l.location_pk
    GROUP BY 1,2,3,4
)
SELECT 
    *,
    prominent_index(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant,
    CASE
        WHEN three_sub_index_criteria(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
        THEN GREATEST(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
        ELSE 0
    END AS aqi
FROM step01_city_level_data;

-- Validation
SELECT * 
FROM agg_city_fact_hour_level 
ORDER BY country, state, city, measurement_time
LIMIT 100;

SELECT * 
FROM agg_city_fact_hour_level 
WHERE city = 'Bengaluru' 
  AND measurement_time = '2025-09-21 16:00:00.000'
ORDER BY measurement_time;

-- =========================================================
-- Daily City-Level Aggregation
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE agg_city_fact_day_level
    TARGET_LAG = '30 MIN'
    WAREHOUSE  = transform_wh
AS
WITH step01_city_day_level_data AS (
    SELECT 
        DATE(measurement_time) AS measurement_date,
        country,
        state,
        city,
        ROUND(AVG(pm10_avg)) AS pm10_avg,
        ROUND(AVG(pm25_avg)) AS pm25_avg,
        ROUND(AVG(so2_avg))  AS so2_avg,
        ROUND(AVG(no2_avg))  AS no2_avg,
        ROUND(AVG(nh3_avg))  AS nh3_avg,
        ROUND(AVG(co_avg))   AS co_avg,
        ROUND(AVG(o3_avg))   AS o3_avg
    FROM agg_city_fact_hour_level
    GROUP BY 1,2,3,4
)
SELECT 
    *,
    prominent_index(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant,
    CASE
        WHEN three_sub_index_criteria(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
        THEN GREATEST(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
        ELSE 0
    END AS aqi
FROM step01_city_day_level_data;

-- Validation
SELECT * 
FROM agg_city_fact_day_level 
ORDER BY country, state, city, measurement_date
LIMIT 100;