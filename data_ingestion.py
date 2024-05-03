#!/usr/bin/env python
# coding: utf-8


import os
import pandas as pd
from dotenv import load_dotenv
import dlt
import requests
from time import time
import logging
import time
# from sqlalchemy import create_engine



# Configure logging
logging.basicConfig(
filename='LoadData.log',  # Specify the file name for the log
level=logging.INFO,  # Set the logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
format='%(asctime)s - %(levelname)s - %(message)s',  # Specify the log message format
datefmt='%Y-%m-%d %H:%M:%S'  # Specify the date/time format
)

logger = logging.getLogger(__name__)



# Load environment variables
load_dotenv()
EMAIL = os.getenv("email")
KEY = os.getenv("key")




# API URLs and parameters
STATES_URL = f'https://aqs.epa.gov/data/api/list/states?email={EMAIL}&key={KEY}'
DAILY_DATA_URL_TEMPLATE = 'https://aqs.epa.gov/data/api/dailyData/byState?email={email}&key={key}&param={param}&bdate={bdate}&edate={edate}&state={state}'
PARAM = 44201     #Ozone
BDATE = 20230101
EDATE = 20231231



# Extract data
def extract_data_from_api(url):
    """Extracts data from the API endpoint."""
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json().get('Data', [])
        if data:
            return pd.DataFrame(data)
    else:
        logger.error(f"API request failed with status code: {response.status_code}")



# Load data 
def load_to_postgres(df, pipeline_name, table_name):
    """Loads DataFrame to PostgreSQL using Dlt."""
    rows = df.shape[0]
    pipeline = dlt.pipeline(
        pipeline_name=pipeline_name,
        destination='postgres',
        dataset_name='public'
    )
    pipeline.run(df, table_name=table_name, write_disposition="append")




def main():
    states_data = extract_data_from_api(STATES_URL)
    if states_data is not None:
        states_codes = states_data.iloc[:,0].tolist()
        states = states_data.iloc[:,1].tolist()
    else:
        logger.warning('No data available Can not extract data using states')
        
    total_rows_loaded = 0
    for index, state in enumerate(states_codes):
        daily_data_url = DAILY_DATA_URL_TEMPLATE.format(email=EMAIL, key=KEY, param=PARAM, bdate=BDATE, edate=EDATE, state=state)
        data = extract_data_from_api(daily_data_url)
        
        if data is not None:   
            data['date_local'] = pd.to_datetime(data['date_local'])
            data['date_of_last_change'] = pd.to_datetime(data['date_of_last_change'])
            rows_loaded = data.shape[0]
            load_to_postgres(data, pipeline_name='AQS_AirData',table_name='AQS_daily_air_data')
            total_rows_loaded += rows_loaded            
            logging.info(f'Loaded {rows_loaded} rows for state {states[index]}. Total rows loaded: {total_rows_loaded}')
        else:
            logger.warning(f'No data available for state {states[index]}.')
            
        time.sleep(1)   
    




if __name__ == "__main__":    
    main()


