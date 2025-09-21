-- =========================================================
-- Context: Stage Schema - Marketplace Integration
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Explore Marketplace Weather Data
-- =========================================================
SELECT 
    'India' AS country,
    'New Delhi' AS state,
    'New Delhi' AS city,
    DATE_VALID_STD      AS measurement_dt,
    AVG_TEMPERATURE_AIR_2M_F AS temperature_in_f
FROM global_weather__climate_data_for_bi.standard_tile.history_day
WHERE country = 'IN'
  AND DATE_VALID_STD > '2024-01-01';

-- =========================================================
-- Create Dynamic Table (Live Weather Feed)
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE weather_data
    TARGET_LAG = '60 MIN'
    WAREHOUSE  = transform_wh
AS
SELECT 
    'India' AS country,
    'New Delhi' AS state,
    'New Delhi' AS city,
    DATE_VALID_STD      AS measurement_dt,
    AVG_TEMPERATURE_AIR_2M_F AS temperature_in_f
FROM global_weather__climate_data_for_bi.standard_tile.history_day
WHERE country = 'IN'
  AND DATE_VALID_STD > '2024-01-01';

-- =========================================================
-- Context: Consumption Schema
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Historical Weather Data Table
-- =========================================================
CREATE OR REPLACE TABLE weather_data AS 
SELECT 
    'India' AS country,
    'Delhi' AS state,
    'Delhi' AS city,
    DATE_VALID_STD      AS measurement_dt,
    AVG_TEMPERATURE_AIR_2M_F AS temperature_in_f
FROM global_weather__climate_data_for_bi.standard_tile.history_day
WHERE country = 'IN'
  AND DATE_VALID_STD BETWEEN '2024-01-01' AND CURRENT_DATE();

-- Validation
SELECT * 
FROM weather_data 
ORDER BY measurement_dt;

-- =========================================================
-- Task: Refresh Daily Weather Data
-- =========================================================
CREATE OR REPLACE TASK refresh_weather_data_task
    WAREHOUSE = load_wh
    SCHEDULE  = 'USING CRON 55 23 * * * Asia/Kolkata' -- Runs daily at 11:55PM IST
AS
INSERT INTO weather_data
SELECT  
    'India' AS country,
    'Delhi' AS state,
    'Delhi' AS city,
    DATE_VALID_STD      AS measurement_dt,
    AVG_TEMPERATURE_AIR_2M_F AS temperature_in_f
FROM global_weather__climate_data_for_bi.standard_tile.history_day
WHERE country = 'IN'
  AND DATE_VALID_STD = CURRENT_DATE();

-- =========================================================
-- Aggregated AQI + Weather Data
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE agg_delhi_fact_day_level
    TARGET_LAG = '60 MIN'
    WAREHOUSE  = transform_wh
AS
SELECT 
    a.measurement_date,
    a.country,
    a.state,
    a.city,
    a.pm10_avg,
    a.pm25_avg,
    a.so2_avg,
    a.no2_avg,
    a.nh3_avg,
    a.co_avg,
    a.o3_avg,
    t.temperature_in_f,
    a.prominent_pollutant,
    a.aqi
FROM agg_city_fact_day_level a
JOIN weather_data t
  ON a.measurement_date = t.measurement_dt
 AND a.country          = t.country
 AND a.state            = t.state
 AND a.city             = t.city;

-- =========================================================
-- Validation Queries
-- =========================================================
SELECT * FROM agg_city_fact_day_level LIMIT 10;
SELECT * FROM agg_delhi_fact_day_level LIMIT 10;