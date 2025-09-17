# Air Quality ETL Pipeline 🚀

An end-to-end **ETL (Extract, Transform, Load) pipeline** for ingesting, processing, and visualizing **Air Quality Index (AQI) data for India**.  
The pipeline automates the **hourly collection of air quality data** from the Indian government open data API, processes it in **Snowflake** using **Snowpark & SQL**, and provides **interactive dashboards** built with **Streamlit**.

---

## 📌 Features

- **Data Ingestion**  
  - Python scripts fetch hourly AQI data from the public API.  
  - Data is uploaded into Snowflake external/internal stages.  

- **Data Processing**  
  - Schemas, staging tables, and dynamic tables created in Snowflake.  
  - Deduplication & JSON flattening for structured datasets.  
  - Aggregations for AQI computation at **station, city, and national levels**.  

- **Transformation Logic**  
  - Custom **Snowflake Python UDFs** to calculate AQI and identify prominent pollutants.  
  - Data cleaning and standardization pipelines.  

- **Visualization**  
  - Interactive **Streamlit dashboards** powered by Snowflake queries.  
  - Trend analysis at multiple geographic levels.  

- **Orchestration & Scheduling**  
  - **Snowflake Tasks** to run transformations automatically.  
  - **GitHub Actions** for hourly ingestion and CI/CD integration.  

