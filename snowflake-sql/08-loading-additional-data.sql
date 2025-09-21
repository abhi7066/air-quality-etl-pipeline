-- =========================================================
-- Context: Orchestration & Refresh Control
-- =========================================================
USE ROLE sysadmin;

-- =========================================================
-- Dynamic Table Refresh Strategy
-- =========================================================
-- Set all downstream dependencies to refresh when sources update
ALTER DYNAMIC TABLE dev_db.consumption_sch.agg_city_fact_hour_level 
    SET TARGET_LAG = 'DOWNSTREAM';

ALTER DYNAMIC TABLE dev_db.consumption_sch.air_quality_fact 
    SET TARGET_LAG = 'DOWNSTREAM';

ALTER DYNAMIC TABLE dev_db.consumption_sch.date_dim 
    SET TARGET_LAG = 'DOWNSTREAM';

ALTER DYNAMIC TABLE dev_db.consumption_sch.location_dim 
    SET TARGET_LAG = 'DOWNSTREAM';

ALTER DYNAMIC TABLE dev_db.clean_sch.clean_flatten_aqi_dt
    SET TARGET_LAG = 'DOWNSTREAM';

ALTER DYNAMIC TABLE dev_db.clean_sch.clean_aqi_dt
    SET TARGET_LAG = 'DOWNSTREAM';

-- =========================================================
-- Task Scheduling
-- =========================================================
-- Run copy task every 5 minutes to load fresh JSON
ALTER TASK dev_db.stage_sch.copy_air_quality_data 
    SET SCHEDULE = '5 MINUTES';

-- Resume the copy task
ALTER TASK dev_db.stage_sch.copy_air_quality_data RESUME;

-- =========================================================
-- Resume Dynamic Tables
-- =========================================================
ALTER DYNAMIC TABLE dev_db.consumption_sch.agg_city_fact_day_level RESUME;
ALTER DYNAMIC TABLE dev_db.consumption_sch.agg_city_fact_hour_level RESUME;
ALTER DYNAMIC TABLE dev_db.consumption_sch.air_quality_fact RESUME;
ALTER DYNAMIC TABLE dev_db.consumption_sch.date_dim RESUME;
ALTER DYNAMIC TABLE dev_db.consumption_sch.location_dim RESUME;
ALTER DYNAMIC TABLE dev_db.clean_sch.clean_flatten_aqi_dt RESUME;
ALTER DYNAMIC TABLE dev_db.clean_sch.clean_aqi_dt RESUME;

-- =========================================================
-- Optional Pause (if needed for cost control)
-- =========================================================
-- Suspend task when not actively loading
ALTER TASK dev_db.stage_sch.copy_air_quality_data SUSPEND;