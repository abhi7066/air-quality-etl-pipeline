use role sysadmin;
use schema dev_db.clean_sch;
use warehouse adhoc_wh;

-- Step-1 Bangalore/Silkboard
select 
    hour(index_record_ts) as measurement_hours,
    * 
from 
    clean_aqi_dt 
where 
    country = 'India' and
    state = 'Karnataka' and 
    station = 'Silk Board, Bengaluru - KSPCB'
    -- Silk Board, Bengaluru - KSPCB
    -- IGI Airport (T3), Delhi - IMD
order by 
    measurement_hours;

-- Step-2 Missing Data (How to handle NA)
select 
    hour(index_record_ts) as measurement_hours,
    * 
from 
    clean_aqi_dt 
where 
    country = 'India' and
    state = 'Delhi' and 
    station = 'Mundka, Delhi - DPCC'
    -- Mundka, Delhi - DPCC
    -- IGI Airport (T3), Delhi - IMD
order by 
    index_record_ts;

-- No duplicate, the measurement is captured in clean way
-- missing data observed

-- step-3 how to transpose the data from rows to columns.
-- create temp table air_quality_tmp as
select 
        index_record_ts,
        country,
        state,
        city,
        station,
        latitude,
        longitude,
        max(case when pollutant_id = 'PM2.5' then pollutant_avg end) as pm25_avg,
        max(case when pollutant_id = 'PM10' then pollutant_avg end) as pm10_avg,
        max(case when pollutant_id = 'SO2' then pollutant_avg end) as so2_avg,
        max(case when pollutant_id = 'NO2' then pollutant_avg end) as no2_avg,
        max(case when pollutant_id = 'NH3' then pollutant_avg end) as nh3_avg,
        max(case when pollutant_id = 'CO' then pollutant_avg end) as co_avg,
        max(case when pollutant_id = 'OZONE' then pollutant_avg end) as o3_avg
    from 
        clean_aqi_dt
    where 
        country = 'India' and
        state = 'Karnataka' and 
        station = 'Silk Board, Bengaluru - KSPCB' and 
        index_record_ts = '2024-03-01 11:00:00.000'
     group by 
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station;

-- select * from air_quality_tmp
select 
    hour(INDEX_RECORD_TS) as measurement_hours,
    *
from 
    air_quality_tmp
where
    country = 'India' and
    state = 'Delhi' and 
    station = 'IGI Airport (T3), Delhi - IMD';

select 
        INDEX_RECORD_TS,
        COUNTRY,
        STATE, -- replace(STATE,'_',' ') as STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        CASE 
            WHEN PM10_AVG = 'NA' THEN 0 
            WHEN PM10_AVG is Null THEN 0 
            ELSE round(PM10_AVG)
        END as PM10_AVG,
        CASE 
            WHEN PM25_AVG = 'NA' THEN 0 
            WHEN PM25_AVG is Null THEN 0 
            ELSE round(PM25_AVG)
        END as PM25_AVG,
        CASE 
            WHEN SO2_AVG = 'NA' THEN 0 
            WHEN SO2_AVG is Null THEN 0 
            ELSE round(SO2_AVG)
        END as SO2_AVG,
         CASE 
            WHEN NH3_AVG = 'NA' THEN 0 
            WHEN NH3_AVG is Null THEN 0 
            ELSE round(NH3_AVG)
        END as NH3_AVG,
        CASE 
            WHEN NO2_AVG = 'NA' THEN 0 
            WHEN NO2_AVG is Null THEN 0 
            ELSE round(NO2_AVG)
        END as NO2_AVG,
         CASE 
            WHEN CO_AVG = 'NA' THEN 0 
            WHEN CO_AVG is Null THEN 0 
            ELSE round(CO_AVG)
        END as CO_AVG,
         CASE 
            WHEN O3_AVG = 'NA' THEN 0 
            WHEN O3_AVG is Null THEN 0 
            ELSE round(O3_AVG)
        END as O3_AVG,
    from air_quality_tmp;

-- step-2
-- next task is to transpose it from rows to columns.
create or replace dynamic table clean_flatten_aqi_dt
    target_lag='DOWNSTREAM'
    warehouse=transform_wh
as
with step01_combine_pollutant_cte as (
    SELECT 
        INDEX_RECORD_TS,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        MAX(CASE WHEN POLLUTANT_ID = 'PM10' THEN POLLUTANT_AVG END) AS PM10_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'PM2.5' THEN POLLUTANT_AVG END) AS PM25_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'SO2' THEN POLLUTANT_AVG END) AS SO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NO2' THEN POLLUTANT_AVG END) AS NO2_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'NH3' THEN POLLUTANT_AVG END) AS NH3_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'CO' THEN POLLUTANT_AVG END) AS CO_AVG,
        MAX(CASE WHEN POLLUTANT_ID = 'OZONE' THEN POLLUTANT_AVG END) AS O3_AVG
    FROM 
        clean_aqi_dt
    group by 
        index_record_ts, country, state, city, station, latitude, longitude
        order by country, state, city, station
),
step02_replace_na_cte as (
    select 
        INDEX_RECORD_TS,
        COUNTRY,
        replace(STATE,'_',' ') as STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        CASE 
            WHEN PM25_AVG = 'NA' THEN 0 
            WHEN PM25_AVG is Null THEN 0 
            ELSE round(PM25_AVG)
        END as PM25_AVG,
        CASE 
            WHEN PM10_AVG = 'NA' THEN 0 
            WHEN PM10_AVG is Null THEN 0 
            ELSE round(PM10_AVG)
        END as PM10_AVG,
        CASE 
            WHEN SO2_AVG = 'NA' THEN 0 
            WHEN SO2_AVG is Null THEN 0 
            ELSE round(SO2_AVG)
        END as SO2_AVG,
        CASE 
            WHEN NO2_AVG = 'NA' THEN 0 
            WHEN NO2_AVG is Null THEN 0 
            ELSE round(NO2_AVG)
        END as NO2_AVG,
         CASE 
            WHEN NH3_AVG = 'NA' THEN 0 
            WHEN NH3_AVG is Null THEN 0 
            ELSE round(NH3_AVG)
        END as NH3_AVG,
         CASE 
            WHEN CO_AVG = 'NA' THEN 0 
            WHEN CO_AVG is Null THEN 0 
            ELSE round(CO_AVG)
        END as CO_AVG,
         CASE 
            WHEN O3_AVG = 'NA' THEN 0 
            WHEN O3_AVG is Null THEN 0 
            ELSE round(O3_AVG)
        END as O3_AVG,
    from step01_combine_pollutant_cte
)
select *,
from step02_replace_na_cte;