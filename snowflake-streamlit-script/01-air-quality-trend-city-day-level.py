# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("Air Quality Trend - City+Day Level")
st.write("This streamlit app hosted on Snowflake shows")

# Get Session
session = get_active_session()

# sql statement
sql_stmt = """
select 
    state, 
    city, 
    pm25_avg,
    pm10_avg,
    so2_avg,
    no2_avg,
    nh3_avg,
    co_avg,
    o3_avg,
    prominent_pollutant,
    aqi 
from 
    dev_db.consumption_sch.agg_city_fact_day_level
where 
    measurement_date = (select max(measurement_date) from dev_db.consumption_sch.agg_city_fact_day_level)
order by aqi desc 
limit 10;
"""

# create a data frame
sf_df = session.sql(sql_stmt).collect()

pd_df =pd.DataFrame(
        sf_df,
        columns=['State','City','PM2.5','PM10','SO3','CO','NO2','NH3','O3','Primary Pollutant','AQI'])

st.dataframe(pd_df)