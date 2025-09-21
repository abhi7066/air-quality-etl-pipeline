-- =========================================================
-- Context: Clean Layer
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.clean_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- De-duplication + Flatten JSON
-- =========================================================
WITH air_quality_with_rank AS (
    SELECT 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts,
        ROW_NUMBER() OVER (
            PARTITION BY index_record_ts 
            ORDER BY _stg_file_load_ts DESC
        ) AS latest_file_rank
    FROM dev_db.stage_sch.raw_aqi
    WHERE index_record_ts IS NOT NULL
),
unique_air_quality_data AS (
    SELECT * 
    FROM air_quality_with_rank 
    WHERE latest_file_rank = 1
)
SELECT 
    index_record_ts,
    hourly_rec.value:country::STRING AS country,
    hourly_rec.value:state::STRING AS state,
    hourly_rec.value:city::STRING AS city,
    hourly_rec.value:station::STRING AS station,
    hourly_rec.value:latitude::NUMBER(12,7) AS latitude,
    hourly_rec.value:longitude::NUMBER(12,7) AS longitude,
    hourly_rec.value:pollutant_id::STRING AS pollutant_id,
    hourly_rec.value:max_value::STRING AS pollutant_max,
    hourly_rec.value:min_value::STRING AS pollutant_min,
    hourly_rec.value:avg_value::STRING AS pollutant_avg,

    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts

FROM unique_air_quality_data,
     LATERAL FLATTEN (input => json_data:records) hourly_rec;

-- =========================================================
-- Dynamic Table for Clean Layer
-- =========================================================
CREATE OR REPLACE DYNAMIC TABLE clean_aqi_dt
    TARGET_LAG = 'DOWNSTREAM'
    WAREHOUSE = transform_wh
AS
WITH air_quality_with_rank AS (
    SELECT 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts,
        ROW_NUMBER() OVER (
            PARTITION BY index_record_ts 
            ORDER BY _stg_file_load_ts DESC
        ) AS latest_file_rank
    FROM dev_db.stage_sch.raw_aqi
    WHERE index_record_ts IS NOT NULL
),
unique_air_quality_data AS (
    SELECT * 
    FROM air_quality_with_rank 
    WHERE latest_file_rank = 1
)
SELECT 
    index_record_ts,
    hourly_rec.value:country::STRING AS country,
    hourly_rec.value:state::STRING AS state,
    hourly_rec.value:city::STRING AS city,
    hourly_rec.value:station::STRING AS station,
    hourly_rec.value:latitude::NUMBER(12,7) AS latitude,
    hourly_rec.value:longitude::NUMBER(12,7) AS longitude,
    hourly_rec.value:pollutant_id::STRING AS pollutant_id,
    hourly_rec.value:max_value::STRING AS pollutant_max,
    hourly_rec.value:min_value::STRING AS pollutant_min,
    hourly_rec.value:avg_value::STRING AS pollutant_avg,

    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts

FROM unique_air_quality_data,
     LATERAL FLATTEN (input => json_data:records) hourly_rec;

-- =========================================================
-- Validate Clean Layer
-- =========================================================
SELECT * FROM clean_aqi_dt LIMIT 10;
