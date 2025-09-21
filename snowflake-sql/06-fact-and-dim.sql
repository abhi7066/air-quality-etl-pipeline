-- =========================================================
-- Context: Consumption Layer - Star Schema
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Date Dimension
-- =========================================================
-- Extract unique timestamps with hierarchy breakdown
CREATE OR REPLACE DYNAMIC TABLE date_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE  = transform_wh
AS
WITH step01_hr_data AS (
    SELECT 
        index_record_ts AS measurement_time,
        YEAR(index_record_ts)    AS aqi_year,
        MONTH(index_record_ts)   AS aqi_month,
        QUARTER(index_record_ts) AS aqi_quarter,
        DAY(index_record_ts)     AS aqi_day,
        HOUR(index_record_ts) + 1 AS aqi_hour
    FROM dev_db.clean_sch.clean_flatten_aqi_dt
    GROUP BY 1,2,3,4,5,6
)
SELECT 
    HASH(measurement_time) AS date_pk,
    *
FROM step01_hr_data
ORDER BY aqi_year, aqi_month, aqi_day, aqi_hour;

-- Validate
SELECT * FROM date_dim LIMIT 10;

-- =========================================================
-- Location Dimension
-- =========================================================
-- Extract unique station metadata
CREATE OR REPLACE DYNAMIC TABLE location_dim
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE  = transform_wh
AS
WITH step01_unique_data AS (
    SELECT 
        latitude,
        longitude,
        country,
        state,
        city,
        station
    FROM dev_db.clean_sch.clean_flatten_aqi_dt
    GROUP BY 1,2,3,4,5,6
)
SELECT 
    HASH(latitude, longitude) AS location_pk,
    *
FROM step01_unique_data
ORDER BY country, state, city, station;

-- Validate
SELECT * FROM location_dim LIMIT 10;

-- =========================================================
-- Fact Table: Air Quality Metrics
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE air_quality_fact
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE  = transform_wh
AS
SELECT 
    -- Primary Key: timestamp + location
    HASH(index_record_ts, latitude, longitude) AS aqi_pk,

    -- Foreign Keys
    HASH(index_record_ts)       AS date_fk,
    HASH(latitude, longitude)   AS location_fk,

    -- Pollutant Metrics
    pm10_avg,
    pm25_avg,
    so2_avg,
    no2_avg,
    nh3_avg,
    co_avg,
    o3_avg,

    -- Derived Metrics
    prominent_index(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant,
    CASE
        WHEN three_sub_index_criteria(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
        THEN GREATEST(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
        ELSE 0
    END AS aqi

FROM dev_db.clean_sch.clean_flatten_aqi_dt;

-- Validate
SELECT * FROM air_quality_fact LIMIT 10;

-- Example: Lookup with dimensions
SELECT 
    f.index_record_ts,
    d.aqi_year, d.aqi_month, d.aqi_day, d.aqi_hour,
    l.country, l.state, l.city, l.station,
    f.pm25_avg, f.pm10_avg, f.aqi, f.prominent_pollutant
FROM air_quality_fact f
JOIN date_dim d      ON f.date_fk = d.date_pk
JOIN location_dim l  ON f.location_fk = l.location_pk
WHERE l.city = 'Chittoor'
  AND l.station = 'Gangineni Cheruvu, Chittoor - APPCB'
  AND f.index_record_ts = '2024-03-01 18:00:00.000';