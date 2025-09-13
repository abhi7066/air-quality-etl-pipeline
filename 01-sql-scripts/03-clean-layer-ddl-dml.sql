-- change the context
use role sysadmin;
use schema dev_db.clean_sch;
use warehouse adhoc_wh;

-- Ste-1
select * from dev_db.stage_sch.raw_aqi order by id;
-- Step-2
select id, index_record_ts, from dev_db.stage_sch.raw_aqi order by id;

-- Step-3
select 
    id, index_record_ts
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null;
order by id;

-- Step-4
select 
    id,
    index_record_ts,
    json_data,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null
    limit 5;

-- Step 5 query without JSON data column
select 
    id,
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null; -- this will give all 24 records

-- now lets loads some duplicate data that is a common issue in some of the 
-- data project and validate the scenario 

select 
    id,
    index_record_ts,
    record_count,
    json_version,
    _stg_file_name,
    _stg_file_load_ts,
    _stg_file_md5 ,
    _copy_data_ts,
    row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
from 
    dev_db.stage_sch.raw_aqi 
where 
    index_record_ts is not null; -- this will give all 24 records



-- de-duplication of the records + flattening it
with air_quality_with_rank as (
    select 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5 ,
        _copy_data_ts,
        row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
    from dev_db.stage_sch.raw_aqi
    where index_record_ts is not null
),
unique_air_quality_data as (
    select 
        * 
    from 
        air_quality_with_rank 
    where latest_file_rank = 1
)
    select 
        index_record_ts,
        hourly_rec.value:country::text as country,
        hourly_rec.value:state::text as state,
        hourly_rec.value:city::text as city,
        hourly_rec.value:station::text as station,
        hourly_rec.value:latitude::number(12,7) as latitude,
        hourly_rec.value:longitude::number(12,7) as longitude,
        hourly_rec.value:pollutant_id::text as pollutant_id,
        hourly_rec.value:pollutant_max::text as pollutant_max,
        hourly_rec.value:pollutant_min::text as pollutant_min,
        hourly_rec.value:pollutant_avg::text as pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
  from 
    unique_air_quality_data ,
    lateral flatten (input => json_data:records) hourly_rec;


-- creating dynamic table
create or replace dynamic table clean_aqi_dt
    target_lag='downstream'
    warehouse=transform_wh
as
  with air_quality_with_rank as (
    select 
        index_record_ts,
        json_data,
        record_count,
        json_version,
        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5 ,
        _copy_data_ts,
        row_number() over (partition by index_record_ts order by _stg_file_load_ts desc) as latest_file_rank
    from dev_db.stage_sch.raw_aqi
    where index_record_ts is not null
),
unique_air_quality_data as (
    select 
        * 
    from 
        air_quality_with_rank 
    where latest_file_rank = 1
)
    select 
        index_record_ts,
        hourly_rec.value:country::text as country,
        hourly_rec.value:state::text as state,
        hourly_rec.value:city::text as city,
        hourly_rec.value:station::text as station,
        hourly_rec.value:latitude::number(12,7) as latitude,
        hourly_rec.value:longitude::number(12,7) as longitude,
        hourly_rec.value:pollutant_id::text as pollutant_id,
        hourly_rec.value:pollutant_max::text as pollutant_max,
        hourly_rec.value:pollutant_min::text as pollutant_min,
        hourly_rec.value:pollutant_avg::text as pollutant_avg,

        _stg_file_name,
        _stg_file_load_ts,
        _stg_file_md5,
        _copy_data_ts
  from 
    unique_air_quality_data ,
    lateral flatten (input => json_data:records) hourly_rec;


  select * from clean_aqi_dt limit 10;