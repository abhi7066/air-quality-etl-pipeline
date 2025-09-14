
# Import python packages
import streamlit as st
import pandas as pd
from snowflake.snowpark.context import get_active_session

# Page Title
st.title("New Delhi - AQI Trend")

# Get Session
session = get_active_session()


trend_sql = f"""
        select 
        measurement_date,
        aqi,
        temperature_in_f
    from 
        dev_db.consumption_sch.agg_delhi_fact_day_level
    where 
        measurement_date between '2024-03-01' and '2024-03-10'
    order by measurement_date
    """
sf_df = session.sql(trend_sql).collect()

# create panda's dataframe
pd_df =pd.DataFrame(
    sf_df,
    columns=['Day','AQI','TEMP'])

st.line_chart(pd_df,x='Day')
    