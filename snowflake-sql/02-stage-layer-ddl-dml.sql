-- =========================================================
-- Context Setup
-- =========================================================
USE ROLE sysadmin;
USE SCHEMA dev_db.stage_sch;
USE WAREHOUSE adhoc_wh;

-- =========================================================
-- Stage & File Format
-- =========================================================
-- Internal stage for storing raw JSON files
CREATE STAGE IF NOT EXISTS raw_stg
    DIRECTORY = (ENABLE = TRUE)
    COMMENT = 'Internal stage for raw air quality JSON data';

-- File format for JSON parsing
CREATE FILE FORMAT IF NOT EXISTS json_file_format 
    TYPE = 'JSON'
    COMPRESSION = 'AUTO'
    COMMENT = 'JSON file format for air quality data';

SHOW STAGES;
LIST @raw_stg;

-- =========================================================
-- Quick Exploration of Raw JSON
-- =========================================================

-- Level 1: Inspect raw files
SELECT * 
FROM @dev_db.stage_sch.raw_stg (FILE_FORMAT => json_file_format) t;

-- Level 2: Extract high-level metadata
SELECT 
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
    t.$1 AS full_json,
    t.$1:total::INT AS record_count,
    t.$1:version::STRING AS json_version
FROM @dev_db.stage_sch.raw_stg (FILE_FORMAT => json_file_format) t;

-- Level 3: Add staging file metadata
SELECT 
    TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
    t.$1 AS full_json,
    t.$1:total::INT AS record_count,
    t.$1:version::STRING AS json_version,
    METADATA$FILENAME AS _stg_file_name,
    METADATA$FILE_LAST_MODIFIED AS _stg_file_load_ts,
    METADATA$FILE_CONTENT_KEY AS _stg_file_md5,
    CURRENT_TIMESTAMP() AS _copy_data_ts
FROM @dev_db.stage_sch.raw_stg (FILE_FORMAT => json_file_format) t;

-- =========================================================
-- Raw Table for Ingestion
-- =========================================================
-- Transient is chosen over Permanent to avoid Fail-Safe costs.
-- Data is always reproducible from raw stage, so Fail-Safe not needed.

CREATE OR REPLACE TRANSIENT TABLE raw_aqi (
    id INT PRIMARY KEY AUTOINCREMENT,
    index_record_ts TIMESTAMP NOT NULL,
    json_data VARIANT NOT NULL,
    record_count NUMBER DEFAULT 0,
    json_version STRING NOT NULL,
    -- audit columns
    _stg_file_name STRING,
    _stg_file_load_ts TIMESTAMP,
    _stg_file_md5 STRING,
    _copy_data_ts TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- =========================================================
-- Task: Automated Ingestion from Stage
-- =========================================================
CREATE OR REPLACE TASK copy_air_quality_data
    WAREHOUSE = load_wh
    SCHEDULE = 'USING CRON 0 * * * * Asia/Kolkata'  -- every hour
AS
COPY INTO raw_aqi (index_record_ts, json_data, record_count, json_version, _stg_file_name, _stg_file_load_ts, _stg_file_md5, _copy_data_ts)
FROM (
    SELECT 
        TRY_TO_TIMESTAMP(t.$1:records[0].last_update::text, 'dd-mm-yyyy hh24:mi:ss') AS index_record_ts,
        t.$1,
        t.$1:total::INT AS record_count,
        t.$1:version::STRING AS json_version,
        METADATA$FILENAME,
        METADATA$FILE_LAST_MODIFIED,
        METADATA$FILE_CONTENT_KEY,
        CURRENT_TIMESTAMP()
    FROM @dev_db.stage_sch.raw_stg t
)
FILE_FORMAT = (FORMAT_NAME = 'dev_db.stage_sch.json_file_format')
ON_ERROR = ABORT_STATEMENT;

-- =========================================================
-- Task Permissions
-- =========================================================
USE ROLE accountadmin;
GRANT EXECUTE TASK, EXECUTE MANAGED TASK ON ACCOUNT TO ROLE sysadmin;
USE ROLE sysadmin;

ALTER TASK dev_db.stage_sch.copy_air_quality_data RESUME;

-- =========================================================
-- Validation Queries
-- =========================================================

-- Check ingested data
SELECT * FROM raw_aqi LIMIT 10;

-- Deduplicate: keep only latest file for each record timestamp
SELECT 
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5,
    _copy_data_ts,
    ROW_NUMBER() OVER (PARTITION BY index_record_ts ORDER BY _stg_file_load_ts DESC) AS latest_file_rank
FROM raw_aqi
ORDER BY index_record_ts DESC
LIMIT 10;
