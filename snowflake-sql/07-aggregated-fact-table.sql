use role sysadmin;
use schema dev_db.consumption_sch;
use warehouse adhoc_wh;

-- step-01
select 
    d.measurement_time,
    l.state,
    l.city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    air_quality_fact f
    join date_dim d on f.date_fk = d.date_pk
    join location_dim l on f.location_fk = l.location_pk
group by 
    1,2,3;

-- step-02
with step01_city_level_data as (
select 
    d.measurement_time,
    l.state,
    l.city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    air_quality_fact f
    join date_dim d on f.date_fk = d.date_pk
    join location_dim l on f.location_fk = l.location_pk
group by 
    1,2,3
)
select 
    *,
    prominent_index(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
from 
    step01_city_level_data;


create or replace dynamic table agg_city_fact_hour_level
    target_lag='30 min'
    warehouse=transform_wh
as 
with step01_city_level_data as (
select 
    d.measurement_time,
    l.country as country,
    l.state as state,
    l.city as city,
    avg(pm10_avg) as pm10_avg,
    avg(pm25_avg) as pm25_avg,
    avg(so2_avg) as so2_avg,
    avg(no2_avg) as no2_avg,
    avg(nh3_avg) as nh3_avg,
    avg(co_avg) as co_avg,
    avg(o3_avg) as o3_avg
from 
    air_quality_fact f
    join date_dim d on f.date_fk = d.date_pk
    join location_dim l on f.location_fk = l.location_pk
group by 
    1,2,3,4
)
select 
    *,
    prominent_index(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
from 
    step01_city_level_data;


select 
    * 
from agg_city_fact_hour_level 
order by 
    country, state, city, measurement_time
limit 100;


select 
    * 
from agg_city_fact_hour_level 
where 
    city = 'Bengaluru' and 
    MEASUREMENT_TIME ='2024-03-04 11:00:00.000'
order by 
    country, state, city, measurement_time
limit 100;




create or replace dynamic table agg_city_fact_day_level
    target_lag='30 min'
    warehouse=transform_wh
as 
with step01_city_day_level_data as (
select 
    date(measurement_time) as measurement_date,
    country as country,
    state as state,
    city as city,
    round(avg(pm10_avg)) as pm10_avg,
    round(avg(pm25_avg)) as pm25_avg,
    round(avg(so2_avg)) as so2_avg,
    round(avg(no2_avg)) as no2_avg,
    round(avg(nh3_avg)) as nh3_avg,
    round(avg(co_avg)) as co_avg,
    round(avg(o3_avg)) as o3_avg
from 
    agg_city_fact_hour_level
group by 
    1,2,3,4
)
select 
    *,
    prominent_index(PM25_AVG,PM10_AVG,SO2_AVG,NO2_AVG,NH3_AVG,CO_AVG,O3_AVG)as prominent_pollutant,
        case
        when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
from 
    step01_city_day_level_data;