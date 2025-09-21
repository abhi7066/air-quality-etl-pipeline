-- =========================================================
-- STEP 1: Use correct role
-- =========================================================
USE ROLE sysadmin;

-- =========================================================
-- STEP 2: Create DEV_DB and schemas for ETL lifecycle
-- =========================================================
CREATE DATABASE IF NOT EXISTS dev_db;

-- Layered schemas
CREATE SCHEMA IF NOT EXISTS dev_db.stage_sch;       -- raw ingestion from stage
CREATE SCHEMA IF NOT EXISTS dev_db.clean_sch;       -- cleaning/transformation
CREATE SCHEMA IF NOT EXISTS dev_db.consumption_sch; -- downstream consumption
CREATE SCHEMA IF NOT EXISTS dev_db.publish_sch;     -- published for BI/ML

-- Check schemas
SHOW SCHEMAS IN DATABASE dev_db;

-- =========================================================
-- STEP 3: Create warehouses for different workloads
-- =========================================================

-- Warehouse for bulk JSON ingestion
CREATE WAREHOUSE IF NOT EXISTS load_wh
     COMMENT = 'Warehouse for loading JSON files from stage'
     WAREHOUSE_SIZE = 'MEDIUM' 
     AUTO_RESUME = TRUE 
     AUTO_SUSPEND = 60 
     ENABLE_QUERY_ACCELERATION = FALSE 
     WAREHOUSE_TYPE = 'STANDARD' 
     MIN_CLUSTER_COUNT = 1 
     MAX_CLUSTER_COUNT = 1 
     SCALING_POLICY = 'STANDARD'
     INITIALLY_SUSPENDED = TRUE;

-- Warehouse for ETL/transform jobs
CREATE WAREHOUSE IF NOT EXISTS transform_wh
     COMMENT = 'Warehouse for ETL/transform workloads' 
     WAREHOUSE_SIZE = 'X-SMALL' 
     AUTO_RESUME = TRUE 
     AUTO_SUSPEND = 60 
     ENABLE_QUERY_ACCELERATION = FALSE 
     WAREHOUSE_TYPE = 'STANDARD' 
     MIN_CLUSTER_COUNT = 1 
     MAX_CLUSTER_COUNT = 1 
     SCALING_POLICY = 'STANDARD'
     INITIALLY_SUSPENDED = TRUE;

-- Longer-running Streamlit warehouse
CREATE WAREHOUSE IF NOT EXISTS streamlit_wh
     COMMENT = 'Warehouse for Streamlit applications' 
     WAREHOUSE_SIZE = 'X-SMALL' 
     AUTO_RESUME = TRUE
     AUTO_SUSPEND = 600     -- longer idle timeout for apps
     ENABLE_QUERY_ACCELERATION = FALSE 
     WAREHOUSE_TYPE = 'STANDARD' 
     MIN_CLUSTER_COUNT = 1 
     MAX_CLUSTER_COUNT = 1 
     SCALING_POLICY = 'STANDARD'
     INITIALLY_SUSPENDED = TRUE;

-- Ad-hoc dev/testing warehouse
CREATE WAREHOUSE IF NOT EXISTS adhoc_wh
     COMMENT = 'Warehouse for adhoc & dev activities' 
     WAREHOUSE_SIZE = 'X-SMALL' 
     AUTO_RESUME = TRUE 
     AUTO_SUSPEND = 60 
     ENABLE_QUERY_ACCELERATION = FALSE 
     WAREHOUSE_TYPE = 'STANDARD' 
     MIN_CLUSTER_COUNT = 1 
     MAX_CLUSTER_COUNT = 1 
     SCALING_POLICY = 'STANDARD'
     INITIALLY_SUSPENDED = TRUE;

-- Check warehouses
SHOW WAREHOUSES;