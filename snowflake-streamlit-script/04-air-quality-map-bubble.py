import streamlit as st
import pandas as pd
from decimal import Decimal
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("Air Quality Trend - City+Day Level")
st.write("This streamlit app hosted on Snowflake shows")

# Get Session
session = get_active_session()

# sql statement
sql_stmt = """
select l.latitude, l.longitude,f.aqi
        from 
        dev_db.consumption_sch.air_quality_fact f
        join dev_db.consumption_sch.location_dim l on l.location_pk = f.location_fk
    where 
    date_fk = (select date_pk from dev_db.consumption_sch.date_dim
    order by measurement_time desc 
    limit 1 )
"""

# create a data frame
sf_df = session.sql(sql_stmt).collect()

pd_df =pd.DataFrame(
        sf_df,
        columns=['lat','lon','AQI'])


columns_to_convert = ['lat', 'lon']
pd_df[columns_to_convert] = pd_df[columns_to_convert].astype(float)
st.map(pd_df,size='AQI') # the size argument does not work in snowflake instance