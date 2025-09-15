import os
import requests
import json
from datetime import datetime
from snowflake.snowpark import Session
import sys
import pytz
import logging

# initiate logging at info level
logging.basicConfig(stream=sys.stdout, level=logging.INFO, format='%(levelname)s - %(message)s')

# Set the IST time zone
ist_timezone = pytz.timezone('Asia/Kolkata')

# Get the current time in IST
current_time_ist = datetime.now(ist_timezone)

# Format the timestamp
timestamp = current_time_ist.strftime('%Y_%m_%d_%H_%M_%S')
file_name = f'air_quality_data_{timestamp}.json'
today_string = current_time_ist.strftime('%Y_%m_%d')

def snowpark_basic_auth() -> Session:
    connection_parameters = {
       "ACCOUNT": os.getenv("SNOWFLAKE_ACCOUNT"),
       "region": os.getenv("SNOWFLAKE_REGION"),
       "USER": os.getenv("SNOWFLAKE_USER"),
       "PASSWORD": os.getenv("SNOWFLAKE_PASSWORD"),
       "ROLE": os.getenv("SNOWFLAKE_ROLE", "SYSADMIN"),
       "DATABASE": os.getenv("SNOWFLAKE_DATABASE", "dev_db"),
       "SCHEMA": os.getenv("SNOWFLAKE_SCHEMA", "stage_sch"),
       "WAREHOUSE": os.getenv("SNOWFLAKE_WAREHOUSE", "load_wh")
    }
    return Session.builder.configs(connection_parameters).create()

def get_air_quality_data(limit):
    api_url = 'https://api.data.gov.in/resource/3b01bcb8-0b14-4abf-b6f2-c1bfd384ba69'
    api_key = os.getenv("DATA_GOV_API_KEY")

    params = {
        'api-key': api_key,
        'format': 'json',
        'limit': limit
    }

    headers = {'accept': 'application/json'}

    try:
        response = requests.get(api_url, params=params, headers=headers)
        logging.info('Got the response, check if 200 or not')

        if response.status_code == 200:
            logging.info('Got the JSON Data')
            json_data = response.json()

            logging.info('Writing the JSON file into local location before it moved to snowflake stage')
            with open(file_name, 'w') as json_file:
                json.dump(json_data, json_file, indent=2)

            logging.info(f'File Written to local disk with name: {file_name}')

            stg_location = '@dev_db.stage_sch.raw_stg/india/' + today_string + '/'
            sf_session = snowpark_basic_auth()

            logging.info(f'Placing the file, the file name is {file_name} and stage location is {stg_location}')
            sf_session.file.put(file_name, stg_location)

            logging.info('JSON File placed successfully in stage location in snowflake')
            lst_query = f'list {stg_location}{file_name}.gz'
            result_lst = sf_session.sql(lst_query).collect()

            logging.info(f'File is placed in snowflake stage location= {result_lst}')
            logging.info('The job over successfully...')
            return json_data
        else:
            logging.error(f"Error: {response.status_code} - {response.text}")
            sys.exit(1)

    except Exception as e:
        logging.error(f"An error occurred: {e}")
        sys.exit(1)

    return None

if __name__ == "__main__":
    limit_value = 4000
    air_quality_data = get_air_quality_data(limit_value)
