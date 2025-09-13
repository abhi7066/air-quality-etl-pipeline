use role sysadmin;
use schema dev_db.consumption_sch;
use warehouse adhoc_wh;

drop dynamic table aqi_final_wide_dt;

create or replace function prominent_index(pm25 number, pm10 number, so2 number, no2 number, nh3 number, co number, o3 number)
returns varchar
language python
runtime_version = '3.8'
handler = 'prominent_index'
AS ' 
def prominent_index(pm25, pm10, so2, no2, nh3, co, o3):
    # Handle None values by replacing them with 0
    pm25 = pm25 if pm25 is not None else 0
    pm10 = pm10 if pm10 is not None else 0
    so2 = so2 if so2 is not None else 0
    no2 = no2 if no2 is not None else 0
    nh3 = nh3 if nh3 is not None else 0
    co = co if co is not None else 0
    o3 = o3 if o3 is not None else 0

    # Create a dictionary to map variable names to their values
    variables = {''PM25'': pm25, ''PM10'': pm10, ''SO2'': so2, ''NO2'': no2, ''NH3'': nh3, ''CO'': co, ''O3'': o3}
    
    # Find the variable with the highest value
    max_variable = max(variables, key=variables.get)
    
    return max_variable
';
-- testing
-- 	56,70 , 12	4	17	47	3
-- 	89,70 , 12	4	17	47	3
select prominent_index(56,70,12,4,17,47,3) ;

create or replace function three_sub_index_criteria(pm25 number, pm10 number, so2 number, no2 number, nh3 number, co number, o3 number)
returns number(38,0)
language python
runtime_version = '3.8'
HANDLER = 'three_sub_index_criteria'
AS '
def three_sub_index_criteria(pm25, pm10, so2, no2, nh3, co, o3  ):
    pm_count = 0
    non_pm_count = 0

    if pm25 is not None and pm25 > 0:
        pm_count = 1
    elif pm10 is not None and pm10 > 0:
        pm_count = 1

    non_pm_count = min(2, sum(p is not None and p != 0 for p in [so2, no2, nh3, co, o3]))

    return pm_count + non_pm_count
';


-- There is no need to run this function and even it is excluded from the
-- dynamic table creation.
create or replace function get_int(input_value varchar)
returns number(38,0)
language sql
as '
    select 
        case 
            when input_value is null then 0
            when input_value = ''NA'' then 0
            else to_number(input_value) 
        end
';



show dynamic tables;
create or replace dynamic table aqi_final_wide_dt
    target_lag='30 min'
    warehouse=transform_wh
as
select 
        index_record_ts,
        year(index_record_ts) as aqi_year,
        month(index_record_ts) as aqi_month,
        quarter(index_record_ts) as aqi_quarter,
        day(index_record_ts) aqi_day,
        hour(index_record_ts) aqi_hour,
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
        prominent_index(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)as prominent_pollutant,
        case
        when three_sub_index_criteria(pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg) > 2 then greatest (pm25_avg,pm10_avg,so2_avg,no2_avg,nh3_avg,co_avg,o3_avg)
        else 0
        end
        as aqi
    from dev_db.clean_sch.clean_flatten_aqi_dt;

--
-- for missing data ..we can use lead or lag approach to fill the missing value
-- and that generally is done if we have some data scince workload.
-- for us, it is pure reporting, so we don't need to worry about.


with step01_time_hierarchy_cte as (
select 
        INDEX_RECORD_TS aqi_dt,
        year(INDEX_RECORD_TS) as aqi_year,
        month(INDEX_RECORD_TS) as aqi_month,
        quarter(INDEX_RECORD_TS) as aqi_quarter,
        day(INDEX_RECORD_TS) aqi_day,
        hour(INDEX_RECORD_TS) aqi_hour,
        COUNTRY,
        STATE,
        CITY,
        STATION,
        LATITUDE,
        LONGITUDE,
        PM10_AVG,
        PM25_AVG,
        SO2_AVG,
        NO2_AVG,
        NH3_AVG,
        CO_AVG,
        O3_AVG
    from dev_db.clean_sch.clean_flatten_aqi_dt
    
),
step02_with_lag_cte as (
    select 
        *,
        LAG(PM25_AVG, 1) OVER (partition by COUNTRY,STATE,CITY,STATION,LATITUDE,LONGITUDE ORDER BY aqi_hour) AS lag_pm25,
        LEAD(PM25_AVG, 1) OVER (partition by COUNTRY,STATE,CITY,STATION,LATITUDE,LONGITUDE ORDER BY aqi_hour) AS lead_pm25
from step01_time_hierarchy_cte
)
select * from step02_with_lag_cte;
    

    

    WITH PreviousData AS (
    SELECT
        hour,
        temperature,
        LAG(temperature, 1) OVER (ORDER BY hour) AS lag1_temperature,
        LAG(temperature, 2) OVER (ORDER BY hour) AS lag2_temperature,
        LAG(temperature, 3) OVER (ORDER BY hour) AS lag3_temperature
    FROM
        temperature_data
)
SELECT
    hour,
    CASE
        WHEN temperature IS NULL THEN COALESCE(lag1_temperature, lag2_temperature, lag3_temperature)
        ELSE temperature
    END AS final_temperature
FROM
    PreviousData;
