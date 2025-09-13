-- use sysadmin role.
use role sysadmin;

-- create a warehoue before you try this..

-- create development database/schema  if does not exist
create database if not exists dev_db;
create schema if not exists dev_db.stage_sch;
create schema if not exists dev_db.clean_sch;
create schema if not exists dev_db.consumption_sch;
create schema if not exists dev_db.publish_sch;

show schemas in database dev_db;
-- visit the object explorer home page from webui.

-- having load_wh warehouse
create warehouse if not exists load_wh
     comment = 'this is load warehosue for loading all the JSON files'
     warehouse_size = 'medium' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- all the ETL workload will be manage by it.
create warehouse if not exists transform_wh
     comment = 'this is ETL warehosue for all loading activity' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- specific virtual warehouse with differt resume time (for streamlit, it should be longer)
 create warehouse if not exists streamlit_wh
     comment = 'this is streamlit virtua warehouse' 
     warehouse_size = 'x-small' 
     auto_resume = true
     auto_suspend = 600 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

-- having adhoc warehouse
create warehouse if not exists adhoc_wh
     comment = 'this is adhoc warehosue for all adhoc & development activities' 
     warehouse_size = 'x-small' 
     auto_resume = true 
     auto_suspend = 60 
     enable_query_acceleration = false 
     warehouse_type = 'standard' 
     min_cluster_count = 1 
     max_cluster_count = 1 
     scaling_policy = 'standard'
     initially_suspended = true;

show warehouses;
-- visit the warehouse home page from webui.
