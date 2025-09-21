-- =========================================================
-- Context: Clean Layer Queries & Transformations
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.clean_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Step 1: Explore data for specific stations
-- =========================================================
-- Example: Bangalore / Silk Board
SELECT 
    HOUR(index_record_ts) AS measurement_hours,
    *
FROM clean_aqi_dt 
WHERE country = 'India'
  AND state = 'Karnataka'
  AND station = 'Silk Board, Bengaluru - KSPCB'
ORDER BY measurement_hours;

-- Example: Delhi / Mundka
SELECT 
    HOUR(index_record_ts) AS measurement_hours,
    *
FROM clean_aqi_dt 
WHERE country = 'India'
  AND state = 'Delhi'
  AND station = 'Mundka, Delhi - DPCC'
ORDER BY index_record_ts;

-- Observation: No duplicates in clean layer, 
-- but some measurements may be missing.

-- =========================================================
-- Step 2: Transpose pollutants from rows → columns
-- =========================================================
-- Pivot pollutants into a wide format for one timestamp
SELECT 
    index_record_ts,
    country,
    state,
    city,
    station,
    latitude,
    longitude,
    MAX(CASE WHEN pollutant_id = 'PM2.5' THEN pollutant_avg END) AS pm25_avg,
    MAX(CASE WHEN pollutant_id = 'PM10'  THEN pollutant_avg END) AS pm10_avg,
    MAX(CASE WHEN pollutant_id = 'SO2'   THEN pollutant_avg END) AS so2_avg,
    MAX(CASE WHEN pollutant_id = 'NO2'   THEN pollutant_avg END) AS no2_avg,
    MAX(CASE WHEN pollutant_id = 'NH3'   THEN pollutant_avg END) AS nh3_avg,
    MAX(CASE WHEN pollutant_id = 'CO'    THEN pollutant_avg END) AS co_avg,
    MAX(CASE WHEN pollutant_id = 'OZONE' THEN pollutant_avg END) AS o3_avg
FROM clean_aqi_dt
WHERE country = 'India'
  AND state = 'Karnataka'
  AND station = 'Silk Board, Bengaluru - KSPCB'
  AND index_record_ts = '2025-09-21 16:00:00.000'
GROUP BY index_record_ts, country, state, city, station, latitude, longitude
ORDER BY country, state, city, station;

-- Example usage of the transposed data
SELECT 
    HOUR(index_record_ts) AS measurement_hours,
    *
FROM air_quality_tmp
WHERE country = 'India'
  AND state = 'Delhi'
  AND station = 'IGI Airport (T3), Delhi - IMD';

-- =========================================================
-- Step 3: Handle missing or 'NA' values
-- =========================================================
SELECT 
    index_record_ts,
    country,
    state,
    city,
    station,
    latitude,
    longitude,
    CASE WHEN pm10_avg = 'NA' OR pm10_avg IS NULL THEN 0 ELSE ROUND(pm10_avg) END AS pm10_avg,
    CASE WHEN pm25_avg = 'NA' OR pm25_avg IS NULL THEN 0 ELSE ROUND(pm25_avg) END AS pm25_avg,
    CASE WHEN so2_avg  = 'NA' OR so2_avg  IS NULL THEN 0 ELSE ROUND(so2_avg)  END AS so2_avg,
    CASE WHEN nh3_avg  = 'NA' OR nh3_avg  IS NULL THEN 0 ELSE ROUND(nh3_avg)  END AS nh3_avg,
    CASE WHEN no2_avg  = 'NA' OR no2_avg  IS NULL THEN 0 ELSE ROUND(no2_avg)  END AS no2_avg,
    CASE WHEN co_avg   = 'NA' OR co_avg   IS NULL THEN 0 ELSE ROUND(co_avg)   END AS co_avg,
    CASE WHEN o3_avg   = 'NA' OR o3_avg   IS NULL THEN 0 ELSE ROUND(o3_avg)   END AS o3_avg
FROM air_quality_tmp;

-- =========================================================
-- Step 4: Create Flattened Dynamic Table
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE clean_flatten_aqi_dt
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE  = transform_wh
AS
WITH step01_combine_pollutant_cte AS (
    SELECT 
        index_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        MAX(CASE WHEN pollutant_id = 'PM10'  THEN pollutant_avg END) AS pm10_avg,
        MAX(CASE WHEN pollutant_id = 'PM2.5' THEN pollutant_avg END) AS pm25_avg,
        MAX(CASE WHEN pollutant_id = 'SO2'   THEN pollutant_avg END) AS so2_avg,
        MAX(CASE WHEN pollutant_id = 'NO2'   THEN pollutant_avg END) AS no2_avg,
        MAX(CASE WHEN pollutant_id = 'NH3'   THEN pollutant_avg END) AS nh3_avg,
        MAX(CASE WHEN pollutant_id = 'CO'    THEN pollutant_avg END) AS co_avg,
        MAX(CASE WHEN pollutant_id = 'OZONE' THEN pollutant_avg END) AS o3_avg
    FROM clean_aqi_dt
    GROUP BY index_record_ts, country, state, city, station, latitude, longitude
),
step02_replace_na_cte AS (
    SELECT 
        index_record_ts,
        country,
        REPLACE(state, '_', ' ') AS state,
        city,
        station,
        latitude,
        longitude,
        CASE WHEN pm25_avg = 'NA' OR pm25_avg IS NULL THEN 0 ELSE ROUND(pm25_avg) END AS pm25_avg,
        CASE WHEN pm10_avg = 'NA' OR pm10_avg IS NULL THEN 0 ELSE ROUND(pm10_avg) END AS pm10_avg,
        CASE WHEN so2_avg = 'NA' OR so2_avg IS NULL THEN 0 ELSE ROUND(so2_avg) END AS so2_avg,
        CASE WHEN no2_avg = 'NA' OR no2_avg IS NULL THEN 0 ELSE ROUND(no2_avg) END AS no2_avg,
        CASE WHEN nh3_avg = 'NA' OR nh3_avg IS NULL THEN 0 ELSE ROUND(nh3_avg) END AS nh3_avg,
        CASE WHEN co_avg = 'NA' OR co_avg IS NULL THEN 0 ELSE ROUND(co_avg) END AS co_avg,
        CASE WHEN o3_avg = 'NA' OR o3_avg IS NULL THEN 0 ELSE ROUND(o3_avg) END AS o3_avg
    FROM step01_combine_pollutant_cte
)
SELECT *
FROM step02_replace_na_cte;

-- =========================================================
-- Step 5: Validate Flattened Output
-- =========================================================
SELECT * FROM clean_flatten_aqi_dt LIMIT 10;