-- =========================================================
-- Context: Consumption Layer
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.consumption_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Function 1: Prominent Pollutant
-- Returns the pollutant name with the highest average
-- =========================================================
CREATE OR REPLACE FUNCTION prominent_index(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, nh3 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS VARCHAR
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'prominent_index'
AS $$
def prominent_index(pm25, pm10, so2, no2, nh3, co, o3):
    # Replace None with 0
    pm25 = pm25 or 0
    pm10 = pm10 or 0
    so2  = so2  or 0
    no2  = no2  or 0
    nh3  = nh3  or 0
    co   = co   or 0
    o3   = o3   or 0

    pollutants = {
        "PM25": pm25,
        "PM10": pm10,
        "SO2": so2,
        "NO2": no2,
        "NH3": nh3,
        "CO": co,
        "O3": o3
    }

    # Return pollutant with max value
    return max(pollutants, key=pollutants.get)
$$;

-- Test example
SELECT prominent_index(56,70,12,4,17,47,3);

-- =========================================================
-- Function 2: Three Sub-Index Criteria
-- Returns count based on PM presence + up to 2 non-PM pollutants
-- =========================================================
CREATE OR REPLACE FUNCTION three_sub_index_criteria(
    pm25 NUMBER, pm10 NUMBER, so2 NUMBER, no2 NUMBER, nh3 NUMBER, co NUMBER, o3 NUMBER
)
RETURNS NUMBER(38,0)
LANGUAGE PYTHON
RUNTIME_VERSION = '3.9'
HANDLER = 'three_sub_index_criteria'
AS $$
def three_sub_index_criteria(pm25, pm10, so2, no2, nh3, co, o3):
    pm_count = 0
    non_pm_count = 0

    # PM criteria
    if (pm25 is not None and pm25 > 0) or (pm10 is not None and pm10 > 0):
        pm_count = 1

    # Non-PM criteria (max 2 pollutants considered)
    non_pm_count = min(2, sum(p is not None and p > 0 for p in [so2, no2, nh3, co, o3]))

    return pm_count + non_pm_count
$$;

-- =========================================================
-- Function 3: Safe Int Conversion (for misc usage)
-- =========================================================
CREATE OR REPLACE FUNCTION get_int(input_value VARCHAR)
RETURNS NUMBER(38,0)
LANGUAGE SQL
AS
$$
    SELECT CASE 
        WHEN input_value IS NULL THEN 0
        WHEN input_value = 'NA' THEN 0
        ELSE TO_NUMBER(input_value)
    END
$$;

-- =========================================================
-- Dynamic Table: Final Wide AQI Table
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE aqi_final_wide_dt
    TARGET_LAG = '30 MIN'
    WAREHOUSE  = transform_wh
AS
SELECT 
    index_record_ts,
    YEAR(index_record_ts)    AS aqi_year,
    MONTH(index_record_ts)   AS aqi_month,
    QUARTER(index_record_ts) AS aqi_quarter,
    DAY(index_record_ts)     AS aqi_day,
    HOUR(index_record_ts)    AS aqi_hour,
    country,
    state,
    city,
    station,
    latitude,
    longitude,
    pm10_avg,
    pm25_avg,
    so2_avg,
    no2_avg,
    nh3_avg,
    co_avg,
    o3_avg,

    -- Prominent pollutant by value
    prominent_index(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) AS prominent_pollutant,

    -- AQI (simple rule: if 3-sub-index criteria > 2, take max value, else 0)
    CASE 
        WHEN three_sub_index_criteria(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg) > 2 
        THEN GREATEST(pm25_avg, pm10_avg, so2_avg, no2_avg, nh3_avg, co_avg, o3_avg)
        ELSE 0
    END AS aqi
FROM dev_db.clean_sch.clean_flatten_aqi_dt;

-- =========================================================
-- Handling Missing Data (Optional)
-- =========================================================
-- Example: Fill missing PM2.5 with previous/next values
WITH step01_time_hierarchy_cte AS (
    SELECT 
        index_record_ts AS aqi_dt,
        YEAR(index_record_ts)    AS aqi_year,
        MONTH(index_record_ts)   AS aqi_month,
        QUARTER(index_record_ts) AS aqi_quarter,
        DAY(index_record_ts)     AS aqi_day,
        HOUR(index_record_ts)    AS aqi_hour,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        pm10_avg,
        pm25_avg,
        so2_avg,
        no2_avg,
        nh3_avg,
        co_avg,
        o3_avg
    FROM dev_db.clean_sch.clean_flatten_aqi_dt
),
step02_with_lag_cte AS (
    SELECT 
        *,
        LAG(pm25_avg, 1)  OVER (PARTITION BY country,state,city,station ORDER BY aqi_dt) AS lag_pm25,
        LEAD(pm25_avg, 1) OVER (PARTITION BY country,state,city,station ORDER BY aqi_dt) AS lead_pm25
    FROM step01_time_hierarchy_cte
)
SELECT *
FROM step02_with_lag_cte;

-- =========================================================
-- Example Pattern: Filling Missing Values
-- =========================================================
-- This can be adapted for pollutant averages
WITH PreviousData AS (
    SELECT
        hour,
        temperature,
        LAG(temperature, 1) OVER (ORDER BY hour) AS lag1_temperature,
        LAG(temperature, 2) OVER (ORDER BY hour) AS lag2_temperature,
        LAG(temperature, 3) OVER (ORDER BY hour) AS lag3_temperature
    FROM temperature_data
)
SELECT
    hour,
    CASE
        WHEN temperature IS NULL 
        THEN COALESCE(lag1_temperature, lag2_temperature, lag3_temperature)
        ELSE temperature
    END AS final_temperature
FROM PreviousData;